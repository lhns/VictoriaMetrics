package mergeset

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkMetaindexSeek(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	items := newLongKeyItems(r, 20000)
	ip := newTestInmemoryPart(items)
	keys := testProbeKeys(r, items[:2000])

	for _, n := range []int{0, 64, 128, 256} {
		name := "plain"
		if n > 0 {
			name = fmt.Sprintf("chunked/n=%d", n)
		}
		b.Run(name, func(b *testing.B) {
			if n > 0 {
				forceMetaindexChunks(b, n)
			} else {
				SetMetaindexChunkRows(0)
			}
			p := ip.NewPart()
			defer p.MustClose()
			var ps partSearch
			ps.Init(p, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ps.Seek(keys[i%len(keys)])
				ps.NextItem()
			}
			b.ReportMetric(float64(p.metaindexSizeBytes), "metaindexBytes")
		})
	}
}
