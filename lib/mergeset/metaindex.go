package mergeset

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

var metaindexChunksCache = blockcache.NewCache(getMaxMetaindexChunksCacheSize)

func getMaxMetaindexChunksCacheSize() int {
	return int(0.05 * float64(memory.Allowed()))
}

// metaindexChunkRows is the number of metaindex rows per in-memory chunk. 0 disables chunking.
//
// It affects only the in-memory representation of metaindex. Nothing is written to disk.
var metaindexChunkRows atomic.Int64

// SetMetaindexChunkRows enables chunked in-memory metaindex for parts with big metaindex keys.
//
// n is the number of metaindex rows per chunk. n=0 disables chunking.
func SetMetaindexChunkRows(n int) {
	if n < 0 {
		n = 0
	}
	metaindexChunkRows.Store(int64(n))
}

// metaindexChunkedMinKeyBytes is the total size of metaindex keys starting from which the part switches to chunked metaindex.
var metaindexChunkedMinKeyBytes = 16 * 1024 * 1024

// metaindexChunkCompressLevel is a fast level, since the chunk keys share long prefixes.
const metaindexChunkCompressLevel = 1

// metaindexRowRef is metaindexRow without firstItem.
type metaindexRowRef struct {
	blockHeadersCount uint32
	indexBlockSize    uint32
	indexBlockOffset  uint64
}

// metaindex holds metaindex rows of a part.
//
// Small parts keep rows in mrs. Parts with big keys keep only the first key of every chunk
// of n rows (sparse) and compress the remaining keys of the chunk (chunks).
type metaindex struct {
	// owner is used as a part key in metaindexChunksCache.
	owner any

	mrs []metaindexRow

	n      int
	rows   []metaindexRowRef
	sparse [][]byte

	// chunks[i] contains zstd-compressed keys of rows i*n+1 ... in chunk i.
	// Every key is encoded as varuint(prefixLen) varuint(suffixLen) suffix,
	// where prefixLen is the common prefix length with the previous key in the chunk.
	// The first key of the chunk is sparse[i].
	chunks [][]byte
}

func (mi *metaindex) isChunked() bool {
	return mi.n > 0
}

func (mi *metaindex) rowsCount() int {
	if mi.isChunked() {
		return len(mi.rows)
	}
	return len(mi.mrs)
}

// row returns the metaindexRow with the given idx. firstItem is empty in chunked mode.
func (mi *metaindex) row(idx int) metaindexRow {
	if !mi.isChunked() {
		return mi.mrs[idx]
	}
	r := mi.rows[idx]
	return metaindexRow{
		blockHeadersCount: r.blockHeadersCount,
		indexBlockOffset:  r.indexBlockOffset,
		indexBlockSize:    r.indexBlockSize,
	}
}

func (mi *metaindex) sizeBytes() uint64 {
	if !mi.isChunked() {
		return metaindexSizeBytes(mi.mrs)
	}
	n := uint64(cap(mi.rows))*uint64(unsafe.Sizeof(metaindexRowRef{})) + uint64(cap(mi.sparse)+cap(mi.chunks))*uint64(unsafe.Sizeof([]byte(nil)))
	for _, b := range mi.sparse {
		n += uint64(cap(b))
	}
	for _, b := range mi.chunks {
		n += uint64(cap(b))
	}
	return n
}

// seekRow returns the index of the metaindex row to start the search for k from.
//
// The result is the same as sort.Search(k <= firstItem[i]) followed by n-- for n > 0.
func (mi *metaindex) seekRow(k []byte) int {
	if !mi.isChunked() {
		n := sort.Search(len(mi.mrs), func(i int) bool {
			return string(k) <= string(mi.mrs[i].firstItem)
		})
		if n > 0 {
			n--
		}
		return n
	}

	c := sort.Search(len(mi.sparse), func(i int) bool {
		return string(k) <= string(mi.sparse[i])
	})
	if c == 0 {
		return 0
	}
	// All the keys in chunks before c-1 are smaller than k, and the first key of the chunk c is not.
	// So the first row with the key >= k is in the chunk c-1 or is the first row of the chunk c.
	c--
	return c*mi.n + mi.seekInChunk(c, k) - 1
}

// seekInChunk returns the index of the first row in the chunk c with the key >= k or the rows count in the chunk
// if there is no such row. The first row of the chunk is smaller than k.
func (mi *metaindex) seekInChunk(c int, k []byte) int {
	rowsInChunk := min(mi.n, len(mi.rows)-c*mi.n)
	data := mi.getChunk(c).data

	bb := metaindexKeyBufPool.Get()
	defer metaindexKeyBufPool.Put(bb)
	key := append(bb.B[:0], mi.sparse[c]...)
	for i := 1; i < rowsInChunk; i++ {
		key, data = nextChunkKey(key, data)
		if string(k) <= string(key) {
			bb.B = key
			return i
		}
	}
	bb.B = key
	return rowsInChunk
}

// nextChunkKey decodes the next front-coded key from data on top of the previous key prev.
//
// It returns the decoded key, which reuses prev, and the tail of data.
func nextChunkKey(prev, data []byte) ([]byte, []byte) {
	prefixLen, n := encoding.UnmarshalVarUint64(data)
	data = data[n:]
	suffixLen, n := encoding.UnmarshalVarUint64(data)
	data = data[n:]
	key := append(prev[:prefixLen], data[:suffixLen]...)
	return key, data[suffixLen:]
}

var metaindexKeyBufPool bytesutil.ByteBufferPool

type metaindexChunk struct {
	data []byte
}

func (c *metaindexChunk) SizeBytes() int {
	return int(unsafe.Sizeof(*c)) + cap(c.data)
}

func (mi *metaindex) getChunk(c int) *metaindexChunk {
	if len(mi.chunks[c]) == 0 {
		// The chunk consists of a single row, which is stored in sparse.
		return &metaindexChunk{}
	}
	key := blockcache.Key{
		Part:   mi.owner,
		Offset: uint64(c),
	}
	if b := metaindexChunksCache.GetBlock(key); b != nil {
		return b.(*metaindexChunk)
	}
	data, err := encoding.DecompressZSTD(nil, mi.chunks[c])
	if err != nil {
		panic(fmt.Errorf("BUG: cannot decompress metaindex chunk #%d: %w", c, err))
	}
	mc := &metaindexChunk{data: data}
	metaindexChunksCache.TryPutBlock(key, mc)
	return mc
}

func (mi *metaindex) mustClose() {
	if !mi.isChunked() {
		return
	}
	metaindexChunksCache.RemoveBlocksForPart(mi.owner)
}

// metaindexBuilder builds metaindex from sorted rows.
type metaindexBuilder struct {
	mi metaindex

	// chunkRows is the number of rows per chunk. 0 keeps all the rows in plain mode.
	chunkRows int

	// minKeyBytes is the total keys size, which switches the builder to chunked mode.
	minKeyBytes int

	keyBytes int
	prevKey  []byte
	raw      []byte
	packed   []byte
}

func newMetaindexBuilder(owner any) *metaindexBuilder {
	b := &metaindexBuilder{
		chunkRows:   int(metaindexChunkRows.Load()),
		minKeyBytes: metaindexChunkedMinKeyBytes,
	}
	b.mi.owner = owner
	return b
}

func (b *metaindexBuilder) add(mr *metaindexRow) error {
	if b.mi.rowsCount() > 0 && string(b.prevKey) > string(mr.firstItem) {
		return fmt.Errorf("metaindex rows aren't sorted by firstItem; row #%d is smaller than the previous row", b.mi.rowsCount())
	}
	if b.mi.isChunked() {
		b.addChunked(mr, b.prevKey)
	} else {
		cp := *mr
		cp.firstItem = append([]byte(nil), mr.firstItem...)
		b.mi.mrs = append(b.mi.mrs, cp)
		b.keyBytes += len(mr.firstItem)
		if b.chunkRows > 0 && b.keyBytes > b.minKeyBytes {
			b.switchToChunked()
		}
	}
	b.prevKey = append(b.prevKey[:0], mr.firstItem...)
	return nil
}

func (b *metaindexBuilder) switchToChunked() {
	mrs := b.mi.mrs
	b.mi.mrs = nil
	b.mi.n = b.chunkRows
	var prev []byte
	for i := range mrs {
		b.addChunked(&mrs[i], prev)
		prev = mrs[i].firstItem
	}
}

// addChunked adds mr to the chunked metaindex. prev is the first item of the previous row.
func (b *metaindexBuilder) addChunked(mr *metaindexRow, prev []byte) {
	mi := &b.mi
	idx := len(mi.rows)
	mi.rows = append(mi.rows, metaindexRowRef{
		blockHeadersCount: mr.blockHeadersCount,
		indexBlockSize:    mr.indexBlockSize,
		indexBlockOffset:  mr.indexBlockOffset,
	})
	if idx%mi.n == 0 {
		mi.sparse = append(mi.sparse, append([]byte(nil), mr.firstItem...))
	} else {
		prefixLen := commonPrefixLen(prev, mr.firstItem)
		b.raw = encoding.MarshalVarUint64(b.raw, uint64(prefixLen))
		b.raw = encoding.MarshalVarUint64(b.raw, uint64(len(mr.firstItem)-prefixLen))
		b.raw = append(b.raw, mr.firstItem[prefixLen:]...)
	}
	if (idx+1)%mi.n == 0 {
		b.flushChunk()
	}
}

func (b *metaindexBuilder) flushChunk() {
	var chunk []byte
	if len(b.raw) > 0 {
		// Compress into a reused buffer and copy the result, since the compressor may over-allocate dst
		// and the chunk lives for the part lifetime.
		b.packed = encoding.CompressZSTDLevel(b.packed[:0], b.raw, metaindexChunkCompressLevel)
		chunk = append([]byte(nil), b.packed...)
	}
	b.mi.chunks = append(b.mi.chunks, chunk)
	b.raw = b.raw[:0]
}

// finish returns the built metaindex.
func (b *metaindexBuilder) finish() (*metaindex, error) {
	mi := &b.mi
	if mi.rowsCount() == 0 {
		return nil, fmt.Errorf("expecting non-zero metaindex rows; got zero")
	}
	if mi.isChunked() {
		if len(mi.rows)%mi.n != 0 {
			b.flushChunk()
		}
		// Drop the spare capacity of the biggest slice.
		mi.rows = append(make([]metaindexRowRef, 0, len(mi.rows)), mi.rows...)
	}
	return mi, nil
}

// metaindexReader streams metaindex rows from r without keeping the whole metaindex in memory.
type metaindexReader struct {
	f  filestream.ReadCloser
	zr *zstd.Reader
	br *bufio.Reader

	// mr contains the last row read by next. It is valid until the next call to next.
	mr  metaindexRow
	buf [16]byte
	n   int
}

func newMetaindexReader(f filestream.ReadCloser) *metaindexReader {
	zr := zstd.GetReader(f)
	br, _ := metaindexBufioPool.Get().(*bufio.Reader)
	if br == nil {
		br = bufio.NewReaderSize(zr, 32*1024)
	} else {
		br.Reset(zr)
	}
	return &metaindexReader{
		f:  f,
		zr: zr,
		br: br,
	}
}

var metaindexBufioPool sync.Pool

// next reads the next row into r.mr. It returns io.EOF after the last row.
func (r *metaindexReader) next() error {
	keyLen, err := binary.ReadUvarint(r.br)
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("cannot read firstItem length: %w", unexpectedEOF(err))
	}
	if keyLen > maxInmemoryBlockSize {
		return fmt.Errorf("too big firstItem length: %d; cannot exceed %d", keyLen, maxInmemoryBlockSize)
	}
	r.mr.firstItem = bytesutil.ResizeNoCopyMayOverallocate(r.mr.firstItem, int(keyLen))
	if _, err := io.ReadFull(r.br, r.mr.firstItem); err != nil {
		return fmt.Errorf("cannot read firstItem: %w", unexpectedEOF(err))
	}
	if _, err := io.ReadFull(r.br, r.buf[:]); err != nil {
		return fmt.Errorf("cannot read metaindexRow fields: %w", unexpectedEOF(err))
	}
	if _, err := r.mr.unmarshalFields(r.buf[:]); err != nil {
		return err
	}
	r.n++
	return nil
}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *metaindexReader) mustClose() {
	r.br.Reset(nil)
	metaindexBufioPool.Put(r.br)
	zstd.PutReader(r.zr)
	r.f.MustClose()
	*r = metaindexReader{}
}

// readMetaindex reads metaindex from r and closes r.
func readMetaindex(r filestream.ReadCloser, owner any) (*metaindex, error) {
	mir := newMetaindexReader(r)
	defer mir.mustClose()

	b := newMetaindexBuilder(owner)
	for {
		if err := mir.next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("cannot read metaindexRow #%d: %w", mir.n, err)
		}
		if err := b.add(&mir.mr); err != nil {
			return nil, err
		}
	}
	return b.finish()
}
