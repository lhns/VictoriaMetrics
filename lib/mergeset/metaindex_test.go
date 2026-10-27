package mergeset

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// setMetaindexChunks sets chunk rows and the chunked mode threshold for new parts until the end of the test.
func setMetaindexChunks(t testing.TB, n, minKeyBytes int) {
	t.Helper()
	oldRows := metaindexChunkRows.Load()
	oldMin := metaindexChunkedMinKeyBytes
	SetMetaindexChunkRows(n)
	metaindexChunkedMinKeyBytes = minKeyBytes
	t.Cleanup(func() {
		metaindexChunkRows.Store(oldRows)
		metaindexChunkedMinKeyBytes = oldMin
	})
}

// forceMetaindexChunks makes every new part use chunked metaindex with n rows per chunk.
func forceMetaindexChunks(t testing.TB, n int) {
	t.Helper()
	setMetaindexChunks(t, n, 0)
}

// newLongKeyItems returns sorted unique items: long keys sharing long prefixes mixed with short keys.
func newLongKeyItems(r *rand.Rand, count int) []string {
	var prefixes []string
	for i := range 3 {
		prefixes = append(prefixes, fmt.Sprintf("\x07%d/", i)+strings.Repeat(fmt.Sprintf("cp%d:/opt/lib/x.jar,", i), 800+i*100))
	}
	m := make(map[string]struct{}, count)
	for len(m) < count {
		var item string
		if r.Intn(5) == 0 {
			item = fmt.Sprintf("short-%d", r.Intn(1e6))
		} else {
			p := prefixes[r.Intn(len(prefixes))]
			// Some keys share only a part of the prefix.
			p = p[:len(p)-r.Intn(len(p)/2)]
			item = fmt.Sprintf("%s|%06d|%s", p, r.Intn(1e5), strings.Repeat("t", r.Intn(300)))
		}
		m[item] = struct{}{}
	}
	items := make([]string, 0, count)
	for s := range m {
		items = append(items, s)
	}
	sort.Strings(items)
	return items
}

func writeTestBlocks(bsw *blockStreamWriter, ph *partHeader, items []string) {
	var ib inmemoryBlock
	flush := func() {
		if len(ib.items) == 0 {
			return
		}
		data := ib.data
		ph.itemsCount += uint64(len(ib.items))
		if ph.blocksCount == 0 {
			ph.firstItem = append(ph.firstItem[:0], ib.items[0].Bytes(data)...)
		}
		ph.lastItem = append(ph.lastItem[:0], ib.items[len(ib.items)-1].Bytes(data)...)
		ph.blocksCount++
		bsw.WriteBlock(&ib)
		ib.Reset()
	}
	for _, item := range items {
		if !ib.Add([]byte(item)) {
			flush()
			if !ib.Add([]byte(item)) {
				panic("item doesn't fit an empty block")
			}
		}
	}
	flush()
}

func newTestInmemoryPart(items []string) *inmemoryPart {
	var ip inmemoryPart
	var bsw blockStreamWriter
	bsw.MustInitFromInmemoryPart(&ip, -3)
	writeTestBlocks(&bsw, &ip.ph, items)
	bsw.MustClose()
	return &ip
}

// testMetaindexKeys returns all the keys of mi.
func testMetaindexKeys(t testing.TB, mi *metaindex) [][]byte {
	t.Helper()
	var keys [][]byte
	if !mi.isChunked() {
		for i := range mi.mrs {
			keys = append(keys, mi.mrs[i].firstItem)
		}
		return keys
	}
	for c := range mi.sparse {
		keys = append(keys, mi.sparse[c])
		rowsInChunk := min(mi.n, len(mi.rows)-c*mi.n)
		data := mi.getChunk(c).data
		key := append([]byte(nil), mi.sparse[c]...)
		for i := 1; i < rowsInChunk; i++ {
			key, data = nextChunkKey(key, data)
			keys = append(keys, append([]byte(nil), key...))
		}
		if len(data) != 0 {
			t.Fatalf("unexpected tail of %d bytes in the chunk %d", len(data), c)
		}
	}
	return keys
}

// testProbeKeys returns keys before, between and after the items, including chunk boundaries.
func testProbeKeys(r *rand.Rand, items []string) [][]byte {
	var keys [][]byte
	add := func(s string) { keys = append(keys, []byte(s)) }
	add("")
	add("\x00")
	add("\xff\xff")
	for _, item := range items {
		add(item)
		add(item[:len(item)-1])
		add(item + "\x00")
		add(item[:r.Intn(len(item)+1)])
	}
	for range 200 {
		add(items[r.Intn(len(items))] + "|" + fmt.Sprint(r.Intn(1e5)))
	}
	return keys
}

func TestMetaindexChunkedSeekRow(t *testing.T) {
	for _, n := range []int{1, 2, 4, 5, 64} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(n)))
			items := newLongKeyItems(r, 1500)
			ip := newTestInmemoryPart(items)

			SetMetaindexChunkRows(0)
			plain := ip.NewPart()
			defer plain.MustClose()
			forceMetaindexChunks(t, n)
			chunked := ip.NewPart()
			defer chunked.MustClose()

			if plain.mi.isChunked() || !chunked.mi.isChunked() {
				t.Fatalf("unexpected modes: plain chunked=%v; chunked chunked=%v", plain.mi.isChunked(), chunked.mi.isChunked())
			}
			rows := plain.mi.rowsCount()
			if rows < 20 {
				t.Fatalf("too few metaindex rows: %d", rows)
			}
			if chunked.mi.rowsCount() != rows {
				t.Fatalf("unexpected rows count; got %d; want %d", chunked.mi.rowsCount(), rows)
			}
			pk := testMetaindexKeys(t, plain.mi)
			ck := testMetaindexKeys(t, chunked.mi)
			for i := range pk {
				if !bytes.Equal(pk[i], ck[i]) {
					t.Fatalf("key #%d mismatch", i)
				}
				pr, cr := plain.mi.row(i), chunked.mi.row(i)
				if pr.blockHeadersCount != cr.blockHeadersCount || pr.indexBlockOffset != cr.indexBlockOffset || pr.indexBlockSize != cr.indexBlockSize {
					t.Fatalf("row #%d mismatch: %+v vs %+v", i, pr, cr)
				}
			}
			for _, k := range testProbeKeys(r, items) {
				want, got := plain.mi.seekRow(k), chunked.mi.seekRow(k)
				if want != got {
					t.Fatalf("seekRow(%q) mismatch; got %d; want %d", k, got, want)
				}
			}
		})
	}
}

func TestMetaindexChunkedPartSearch(t *testing.T) {
	for _, n := range []int{2, 4, 64} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(n)))
			items := newLongKeyItems(r, 1500)
			ip := newTestInmemoryPart(items)

			SetMetaindexChunkRows(0)
			plain := ip.NewPart()
			defer plain.MustClose()
			forceMetaindexChunks(t, n)
			chunked := ip.NewPart()
			defer chunked.MustClose()

			if err := testPartSearchSerial(r, chunked, items); err != nil {
				t.Fatalf("chunked part search: %s", err)
			}

			var psPlain, psChunked partSearch
			psPlain.Init(plain, false)
			psChunked.Init(chunked, false)
			for _, k := range testProbeKeys(r, items) {
				psPlain.Seek(k)
				psChunked.Seek(k)
				for range 7 {
					a, b := psPlain.NextItem(), psChunked.NextItem()
					if a != b {
						t.Fatalf("NextItem mismatch for %q: %v vs %v", k, a, b)
					}
					if !a {
						break
					}
					if string(psPlain.Item) != string(psChunked.Item) {
						t.Fatalf("item mismatch for %q", k)
					}
				}
				if psPlain.Error() != nil || psChunked.Error() != nil {
					t.Fatalf("unexpected errors: %v; %v", psPlain.Error(), psChunked.Error())
				}
			}
		})
	}
}

func TestMetaindexSwitchesToChunkedByThreshold(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	items := newLongKeyItems(r, 600)
	ip := newTestInmemoryPart(items)

	// The switch happens in the middle of the metaindex.
	setMetaindexChunks(t, 4, 100*1024)
	p := ip.NewPart()
	defer p.MustClose()
	if !p.mi.isChunked() {
		t.Fatalf("expecting chunked metaindex")
	}
	metaindexChunkedMinKeyBytes = 1 << 30
	p2 := ip.NewPart()
	defer p2.MustClose()
	if p2.mi.isChunked() {
		t.Fatalf("expecting plain metaindex")
	}
	if p.metaindexSizeBytes >= p2.metaindexSizeBytes {
		t.Fatalf("chunked metaindex must be smaller; got %d vs %d", p.metaindexSizeBytes, p2.metaindexSizeBytes)
	}
	if err := testPartSearchSerial(r, p, items); err != nil {
		t.Fatal(err)
	}
}

// TestMetaindexReferenceParse verifies that the streaming loader returns the same rows as the original unmarshalMetaindexRows.
func TestMetaindexReferenceParse(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	items := newLongKeyItems(r, 800)
	ip := newTestInmemoryPart(items)

	mrs, err := unmarshalMetaindexRows(nil, ip.metaindexData.NewReader())
	if err != nil {
		t.Fatalf("reference parse: %s", err)
	}
	for _, n := range []int{0, 3} {
		forceMetaindexChunks(t, n)
		mi, err := readMetaindex(ip.metaindexData.NewReader(), nil)
		if err != nil {
			t.Fatalf("n=%d: %s", n, err)
		}
		keys := testMetaindexKeys(t, mi)
		if len(keys) != len(mrs) {
			t.Fatalf("n=%d: rows count mismatch; got %d; want %d", n, len(keys), len(mrs))
		}
		for i := range mrs {
			got := mi.row(i)
			got.firstItem = keys[i]
			if string(got.firstItem) != string(mrs[i].firstItem) || got.blockHeadersCount != mrs[i].blockHeadersCount ||
				got.indexBlockOffset != mrs[i].indexBlockOffset || got.indexBlockSize != mrs[i].indexBlockSize {
				t.Fatalf("n=%d: row #%d mismatch", n, i)
			}
		}
	}
}

func TestMetaindexRejectsUnsortedRows(t *testing.T) {
	var data []byte
	for _, k := range []string{"b", "a"} {
		mr := metaindexRow{firstItem: []byte(k), blockHeadersCount: 1, indexBlockSize: 1}
		data = mr.Marshal(data)
	}
	var ip inmemoryPart
	ip.metaindexData.MustWrite(encoding.CompressZSTDLevel(nil, data, 1))
	if _, err := readMetaindex(ip.metaindexData.NewReader(), nil); err == nil {
		t.Fatalf("expecting error for unsorted rows")
	}
	if _, err := unmarshalMetaindexRows(nil, ip.metaindexData.NewReader()); err == nil {
		t.Fatalf("expecting error from the reference parser for unsorted rows")
	}
}

func writeTestFilePart(t testing.TB, path string, items []string) {
	t.Helper()
	var ph partHeader
	var bsw blockStreamWriter
	bsw.MustInitFromFilePart(path, true, 3)
	writeTestBlocks(&bsw, &ph, items)
	bsw.MustClose()
	ph.MustWriteMetadata(path)
}

func readDirFiles(t testing.TB, path string) map[string][]byte {
	t.Helper()
	m := map[string][]byte{}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(path, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		m[e.Name()] = data
	}
	return m
}

var stockPartFiles = []string{indexFilename, itemsFilename, lensFilename, metaindexFilename, metadataFilename}

// checkStockPartFiles verifies that the part dir contains only the stock files and that metaindex.bin parses with the reference parser.
func checkStockPartFiles(t testing.TB, partPath string) {
	t.Helper()
	files := readDirFiles(t, partPath)
	if len(files) != len(stockPartFiles) {
		t.Fatalf("unexpected files in %q: %d; want %d", partPath, len(files), len(stockPartFiles))
	}
	for _, name := range stockPartFiles {
		if _, ok := files[name]; !ok {
			t.Fatalf("missing %q in %q", name, partPath)
		}
	}
	if _, err := unmarshalMetaindexRows(nil, bytes.NewReader(files[metaindexFilename])); err != nil {
		t.Fatalf("cannot parse %s/metaindex.bin with the reference parser: %s", partPath, err)
	}
}

// TestWriterIndependentOfChunkRows verifies that part files do not depend on -storage.metaindexChunkRows.
func TestWriterIndependentOfChunkRows(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	items := newLongKeyItems(r, 1000)
	dir := t.TempDir()
	defaultMin := metaindexChunkedMinKeyBytes

	var ref map[string][]byte
	for i, n := range []int{0, 4, 64} {
		for _, force := range []bool{false, true} {
			minKeyBytes := defaultMin
			if force {
				minKeyBytes = 0
			}
			setMetaindexChunks(t, n, minKeyBytes)
			path := filepath.Join(dir, fmt.Sprintf("part-%d-%v", i, force))
			writeTestFilePart(t, path, items)

			// Open and search the part with the given settings. This must not change anything on disk.
			p := mustOpenFilePart(path)
			if err := testPartSearchSerial(r, p, items); err != nil {
				t.Fatalf("n=%d force=%v: %s", n, force, err)
			}
			p.MustClose()

			files := readDirFiles(t, path)
			if ref == nil {
				ref = files
				checkStockPartFiles(t, path)
				continue
			}
			if len(files) != len(ref) {
				t.Fatalf("n=%d force=%v: different file lists", n, force)
			}
			for name, data := range files {
				if !bytes.Equal(data, ref[name]) {
					t.Fatalf("n=%d force=%v: file %q differs", n, force, name)
				}
			}
		}
	}
}

func mergeAllFileParts(t testing.TB, tb *Table) {
	t.Helper()
	tb.partsLock.Lock()
	var pws []*partWrapper
	for _, pw := range tb.fileParts {
		if !pw.isInMerge {
			pw.isInMerge = true
			pws = append(pws, pw)
		}
	}
	tb.partsLock.Unlock()
	if len(pws) == 0 {
		return
	}
	if err := tb.mergeParts(pws, nil, true); err != nil {
		t.Fatalf("cannot merge parts: %s", err)
	}
}

func checkTableItems(t testing.TB, tb *Table, items []string, r *rand.Rand, label string) {
	t.Helper()
	var ts TableSearch
	ts.Init(tb, false)
	defer ts.MustClose()
	for _, k := range testProbeKeys(r, items) {
		ts.Seek(k)
		n := sort.SearchStrings(items, string(k))
		for i := n; i < min(n+5, len(items)); i++ {
			if !ts.NextItem() {
				t.Fatalf("%s: missing item #%d for key %q: %v", label, i, k, ts.Error())
			}
			if string(ts.Item) != items[i] {
				t.Fatalf("%s: unexpected item #%d for key %q", label, i, k)
			}
		}
	}
}

// TestChunkRowsReopenMatrix reopens the same table with different chunk rows settings, adds items and merges parts.
func TestChunkRowsReopenMatrix(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	all := newLongKeyItems(r, 2100)
	r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	path := filepath.Join(t.TempDir(), "table")

	var added []string
	var isReadOnly atomic.Bool
	steps := []int{0, 64, 4, 0, 4, 64, 0}
	batchSize := len(all) / len(steps)
	for step, n := range steps {
		forceMetaindexChunks(t, n)
		label := fmt.Sprintf("step %d n=%d", step, n)
		tb := MustOpenTable(path, 0, nil, 0, nil, &isReadOnly)

		sorted := append([]string(nil), added...)
		sort.Strings(sorted)
		if len(sorted) > 0 {
			checkTableItems(t, tb, sorted, r, label+" after open")
		}

		batch := all[step*batchSize : (step+1)*batchSize]
		if step == len(steps)-1 {
			batch = all[step*batchSize:]
		}
		for _, item := range batch {
			tb.AddItems([][]byte{[]byte(item)})
		}
		added = append(added, batch...)
		tb.DebugFlush()
		if step%2 == 0 {
			// Merge under this setting.
			mergeAllFileParts(t, tb)
		}
		sorted = append(sorted[:0], added...)
		sort.Strings(sorted)
		checkTableItems(t, tb, sorted, r, label+" after add")
		tb.MustClose()

		// Every part dir must contain only the stock files.
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		partDirs := 0
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			partDirs++
			checkStockPartFiles(t, filepath.Join(path, e.Name()))
		}
		if partDirs == 0 {
			t.Fatalf("%s: no parts in %q", label, path)
		}
	}
}
