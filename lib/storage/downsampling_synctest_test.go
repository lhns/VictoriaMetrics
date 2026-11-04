//go:build synctest

package storage

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// The zstd decoder for limited decompression is created lazily and shared by all tests. Create it outside
// the synctest bubbles, since a decoder created in one bubble cannot be used from another.
func init() {
	src := encoding.CompressZSTDLevel(nil, []byte("prime the decoder"), 1)
	if _, err := encoding.DecompressZSTDLimited(nil, src, 0x800000); err != nil {
		panic(fmt.Errorf("cannot prime the zstd decoder: %w", err))
	}
}

type testPartitionStats struct {
	rows        uint64
	minDedup    int64
	partPaths   []string
	finalNeeded bool
}

func getTestPartitionStats(s *Storage) map[string]testPartitionStats {
	m := make(map[string]testPartitionStats)
	ptws := s.tb.GetAllPartitions(nil)
	defer s.tb.PutPartitions(ptws)
	for _, ptw := range ptws {
		pws := ptw.pt.GetParts(nil, false)
		st := testPartitionStats{
			minDedup:    getMinDedupInterval(pws),
			finalNeeded: ptw.pt.isFinalDedupNeeded(),
		}
		for _, pw := range pws {
			st.rows += pw.p.ph.RowsCount
			st.partPaths = append(st.partPaths, pw.p.path)
		}
		slices.Sort(st.partPaths)
		ptw.pt.PutParts(pws)
		m[ptw.pt.name] = st
	}
	return m
}

// The samples are at start+15s, start+30s, ... so that whole buckets are filled: (start, start+1m] is the first one.
func addTestSamples(s *Storage, series int, start time.Time, samples int) {
	var mrs []MetricRow
	for i := range series {
		mn := MetricName{
			AccountID:   12,
			ProjectID:   34,
			MetricGroup: fmt.Appendf(nil, "ds_metric_%d", i),
		}
		raw := mn.marshalRaw(nil)
		for j := range samples {
			mrs = append(mrs, MetricRow{
				MetricNameRaw: raw,
				Timestamp:     start.Add(time.Duration(j+1) * 15 * time.Second).UnixMilli(),
				Value:         float64(j),
			})
		}
	}
	s.AddRows(mrs, 64)
	s.DebugFlush()
}

// The test walks one dataset through the fork and stock behavior: thin, switch to stock, backfill, switch back.
func TestDownsamplingSwitchStockAndFork(t *testing.T) {
	defer testRemoveAll(t)

	const (
		series = 3
		// 2 hours of 15s samples, which become 120 samples per series at 1m.
		samples       = 480
		thinned       = 120
		backfill      = 240 // 1 hour of 15s samples, 60 samples per series at 1m
		backfillThin  = 60
		dedup15s      = int64(15000)
		dedup1m       = int64(60000)
		retentionDays = 5 * 365
	)
	opts := OpenOptions{Retention: retentionDays * 24 * time.Hour}

	synctest.Test(t, func(t *testing.T) {
		// synctest starts at 2000-01-01T00:00:00Z. With 30d:1m, 1999_12 is too young and 1999_08..1999_11 are eligible.
		withDownsamplingRules(t, dedup15s, "30d:1m", false)

		s := MustOpenStorage(t.Name(), opts)
		defer func() { s.MustClose() }()

		for _, month := range []time.Month{8, 9, 10, 11, 12} {
			addTestSamples(s, series, time.Date(1999, month, 10, 0, 0, 0, 0, time.UTC), samples)
		}
		addTestSamples(s, series, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 10)

		// Not-yet-merged inmemory parts claim no thinning, so every old month needs the final merge.
		st := getTestPartitionStats(s)
		for _, name := range []string{"1999_08", "1999_09", "1999_10", "1999_11"} {
			if !st[name].finalNeeded {
				t.Fatalf("partition %s must need thinning before the merge; stats=%+v", name, st[name])
			}
		}

		// a. Force merge thins the eligible months only.
		if err := s.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		st = getTestPartitionStats(s)
		for _, name := range []string{"1999_08", "1999_09", "1999_10", "1999_11"} {
			if got, want := st[name].rows, uint64(series*thinned); got != want {
				t.Fatalf("partition %s: unexpected rows after thinning; got %d; want %d", name, got, want)
			}
			if st[name].minDedup != dedup1m || st[name].finalNeeded {
				t.Fatalf("partition %s: unexpected stats after thinning: %+v", name, st[name])
			}
		}
		if got, want := st["1999_12"].rows, uint64(series*samples); got != want {
			t.Fatalf("partition 1999_12 must stay untouched; got %d rows; want %d", got, want)
		}
		if st["1999_12"].minDedup != dedup15s || st["1999_12"].finalNeeded {
			t.Fatalf("partition 1999_12: unexpected stats: %+v", st["1999_12"])
		}
		if st["2000_01"].minDedup != dedup15s || st["2000_01"].finalNeeded {
			t.Fatalf("partition 2000_01: unexpected stats: %+v", st["2000_01"])
		}

		// Simulate a crash in the middle of a force merge: a part directory which parts.json doesn't list.
		stray := filepath.Join(filepath.Dir(st["1999_10"].partPaths[0]), "00000000000000FF")
		if err := os.CopyFS(stray, os.DirFS(st["1999_10"].partPaths[0])); err != nil {
			t.Fatalf("cannot create stray part: %s", err)
		}
		s.MustClose()

		// b. Stock: no rules. The thinned months are not scheduled and the stray part is gone after the restart.
		withDownsamplingRules(t, dedup15s, "", false)
		// merge indexes derive from the clock, which is frozen in the bubble
		time.Sleep(time.Second)
		s = MustOpenStorage(t.Name(), opts)
		if _, err := os.Stat(stray); !os.IsNotExist(err) {
			t.Fatalf("stray part %s must be removed on startup; err=%v", stray, err)
		}
		stStock := getTestPartitionStats(s)
		for name, v := range st {
			got := stStock[name]
			if got.rows != v.rows || got.minDedup != v.minDedup || got.finalNeeded || !slices.Equal(got.partPaths, v.partPaths) {
				t.Fatalf("partition %s changed after the switch to stock; got %+v; want %+v", name, got, v)
			}
		}

		// Stock backfill into a thinned month: the merged part claims only the global interval.
		addTestSamples(s, series, time.Date(1999, 10, 20, 0, 0, 0, 0, time.UTC), backfill)
		if err := s.ForceMergePartitions("1999_10"); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		stStock = getTestPartitionStats(s)
		if got, want := stStock["1999_10"].rows, uint64(series*(thinned+backfill)); got != want {
			t.Fatalf("stock must keep the backfill as is; got %d rows; want %d", got, want)
		}
		if stStock["1999_10"].minDedup != dedup15s {
			t.Fatalf("stock merge must claim the global interval; got %d", stStock["1999_10"].minDedup)
		}
		s.MustClose()

		// c. Back to the fork: only the month with the backfill is scheduled and rewritten.
		withDownsamplingRules(t, dedup15s, "30d:1m", false)
		// merge indexes derive from the clock, which is frozen in the bubble
		time.Sleep(time.Second)
		s = MustOpenStorage(t.Name(), opts)
		st = getTestPartitionStats(s)
		for name, v := range st {
			if want := name == "1999_10"; v.finalNeeded != want {
				t.Fatalf("partition %s: unexpected finalNeeded=%v", name, v.finalNeeded)
			}
		}
		before := st
		ptws := s.tb.GetAllPartitions(nil)
		for _, ptw := range ptws {
			if ptw.pt.isFinalDedupNeeded() {
				if err := ptw.pt.mergeUnderStampedParts(nil); err != nil {
					t.Fatalf("cannot force merge: %s", err)
				}
			}
		}
		s.tb.PutPartitions(ptws)
		st = getTestPartitionStats(s)
		for name, v := range st {
			if v.finalNeeded {
				t.Fatalf("partition %s is still scheduled", name)
			}
			if name == "1999_10" {
				if want := uint64(series * (thinned + backfillThin)); v.rows != want || v.minDedup != dedup1m {
					t.Fatalf("partition %s: unexpected stats after re-thinning; got %+v; want rows=%d", name, v, want)
				}
				continue
			}
			if !slices.Equal(v.partPaths, before[name].partPaths) || v.rows != before[name].rows {
				t.Fatalf("partition %s must not be rewritten; got %+v; want %+v", name, v, before[name])
			}
		}
	})
}

func TestDownsamplingSteps(t *testing.T) {
	defer testRemoveAll(t)

	synctest.Test(t, func(t *testing.T) {
		// 30d:1m,60d:5m at 2000-01-01: 1999_12 untouched, 1999_11 at 1m (ends 31 days before),
		// 1999_10 (ends 62 days before) and 1999_09 at 5m.
		withDownsamplingRules(t, 15000, "30d:1m,60d:5m", false)
		s := MustOpenStorage(t.Name(), OpenOptions{Retention: 5 * 365 * 24 * time.Hour})
		defer s.MustClose()

		for _, month := range []time.Month{9, 10, 11, 12} {
			addTestSamples(s, 2, time.Date(1999, month, 10, 0, 0, 0, 0, time.UTC), 480)
		}
		if err := s.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		st := getTestPartitionStats(s)
		// 480 samples of 15s are 2 hours: 120 at 1m or 24 at 5m, per series.
		want := map[string]struct {
			rows     uint64
			minDedup int64
		}{
			"1999_09": {2 * 24, 300000},
			"1999_10": {2 * 24, 300000},
			"1999_11": {2 * 120, 60000},
			"1999_12": {2 * 480, 15000},
		}
		for name, w := range want {
			if st[name].rows != w.rows || st[name].minDedup != w.minDedup || st[name].finalNeeded {
				t.Fatalf("partition %s: unexpected stats %+v; want %+v", name, st[name], w)
			}
		}
	})
}

type testSample struct {
	ts int64
	v  float64
}

// testSampleBlock is the samples of one stored block, so that block boundaries can be told apart.
type testSampleBlock []testSample

func sameTestValue(a, b float64) bool {
	if decimal.IsStaleNaN(a) || decimal.IsStaleNaN(b) {
		return decimal.IsStaleNaN(a) && decimal.IsStaleNaN(b)
	}
	return a == b
}

func addTestSeries(s *Storage, input map[string][]testSample) {
	var mrs []MetricRow
	for name, samples := range input {
		mn := MetricName{AccountID: 12, ProjectID: 34, MetricGroup: []byte(name)}
		raw := mn.marshalRaw(nil)
		for _, sm := range samples {
			mrs = append(mrs, MetricRow{MetricNameRaw: raw, Timestamp: sm.ts, Value: sm.v})
		}
	}
	s.AddRows(mrs, 64)
	s.DebugFlush()
}

// readTestBlocks returns the raw stored blocks of the ds_ref_* series in tr, in storage order.
func readTestBlocks(t *testing.T, s *Storage, tr TimeRange) map[string][]testSampleBlock {
	t.Helper()
	tfs := NewTagFilters(12, 34)
	if err := tfs.Add(nil, []byte("ds_ref_.*"), false, true); err != nil {
		t.Fatalf("cannot add tag filter: %s", err)
	}
	var search Search
	defer search.MustClose()
	search.Init(noDeadlineContext, nil, s, []*TagFilters{tfs}, tr, 1e5)
	res := make(map[string][]testSampleBlock)
	for search.NextMetricBlock(noDeadlineContext) {
		var mn MetricName
		if err := mn.Unmarshal(search.MetricBlockRef.MetricName); err != nil {
			t.Fatalf("cannot unmarshal metric name: %s", err)
		}
		var b Block
		search.MetricBlockRef.BlockRef.MustReadBlock(&b)
		if err := b.UnmarshalData(); err != nil {
			t.Fatalf("cannot unmarshal block: %s", err)
		}
		tss, vs := b.AppendRowsWithTimeRangeFilter(nil, nil, tr)
		blk := make(testSampleBlock, len(tss))
		for i := range tss {
			blk[i] = testSample{tss[i], vs[i]}
		}
		name := string(mn.MetricGroup)
		res[name] = append(res[name], blk)
	}
	if err := search.Error(); err != nil {
		t.Fatalf("error in Search: %s", err)
	}
	return res
}

func flattenTestBlocks(blocks map[string][]testSampleBlock) map[string][]testSample {
	res := make(map[string][]testSample, len(blocks))
	for name, blks := range blocks {
		for _, blk := range blks {
			res[name] = append(res[name], blk...)
		}
	}
	return res
}

// testSamplesSig makes samples comparable, since stale NaNs are not equal to each other.
func testSamplesSig(m map[string][]testSample) string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	slices.Sort(names)
	var b []byte
	for _, name := range names {
		b = fmt.Appendf(b, "%s:%d\n", name, len(m[name]))
		for _, sm := range m[name] {
			b = fmt.Appendf(b, "%d=%x ", sm.ts, math.Float64bits(sm.v))
		}
		b = append(b, '\n')
	}
	return string(b)
}

func testRange(year int, month time.Month) TimeRange {
	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	return TimeRange{
		MinTimestamp: start.UnixMilli(),
		MaxTimestamp: start.AddDate(0, 1, 0).UnixMilli() - 1,
	}
}

// genTestSeries returns n samples roughly 15s apart after base, either a jittered gauge with stale markers
// and duplicate timestamps or a monotonic counter.
func genTestSeries(rng *rand.Rand, base int64, n int, counter bool) []testSample {
	var res []testSample
	v := 0.0
	for i := range n {
		ts := base + int64(i+1)*15000 + rng.Int63n(5000) - 2500
		if counter {
			v += float64(rng.Intn(5))
			res = append(res, testSample{ts, v})
			continue
		}
		v = float64(rng.Intn(1000))
		if i%97 == 0 {
			v = decimal.StaleNaN
		}
		res = append(res, testSample{ts, v})
		if i%211 == 0 {
			res = append(res, testSample{ts, v + 1})
		}
	}
	return res
}

// checkThinned verifies the thinning invariants of one series without recomputing the thinning itself.
func checkThinned(t *testing.T, name string, input []testSample, blocks []testSampleBlock, interval int64, counter bool) {
	t.Helper()
	values := make(map[int64][]float64)
	resolved := make(map[int64]float64)
	lastTs := make(map[int64]int64)
	for _, sm := range input {
		values[sm.ts] = append(values[sm.ts], sm.v)
		// stock dedup: max value on equal timestamps, numeric preferred over stale NaN
		old, ok := resolved[sm.ts]
		if !ok || decimal.IsStaleNaN(old) || (!decimal.IsStaleNaN(sm.v) && sm.v > old) {
			resolved[sm.ts] = sm.v
		}
		b := bucketOf(sm.ts, interval)
		lastTs[b] = max(lastTs[b], sm.ts)
	}

	type placed struct {
		testSample
		block int
	}
	kept := make(map[int64][]placed)
	var all []testSample
	for bi, blk := range blocks {
		for _, sm := range blk {
			// 1. Every kept sample is an input sample.
			if !slices.ContainsFunc(values[sm.ts], func(v float64) bool { return sameTestValue(v, sm.v) }) {
				t.Fatalf("%s: kept sample %+v is not in the input", name, sm)
			}
			b := bucketOf(sm.ts, interval)
			kept[b] = append(kept[b], placed{sm, bi})
			all = append(all, sm)
		}
	}
	if len(kept) != len(lastTs) {
		t.Fatalf("%s: kept %d buckets; input has %d", name, len(kept), len(lastTs))
	}
	for b, ts := range lastTs {
		k := kept[b]
		if len(k) == 0 {
			t.Fatalf("%s: bucket %d lost", name, b)
		}
		// 2. The true last sample of the bucket is kept.
		last := k[len(k)-1]
		if last.ts != ts || !sameTestValue(last.v, resolved[ts]) {
			t.Fatalf("%s: bucket %d keeps %+v last; want ts=%d v=%v", name, b, last.testSample, ts, resolved[ts])
		}
		// 3. Dedup runs per output block as in stock, so a bucket split between blocks may keep one sample in each.
		if len(k) > 2 || (len(k) == 2 && k[0].block == k[1].block) {
			t.Fatalf("%s: bucket %d keeps unexpected samples %+v", name, b, k)
		}
	}
	for i := 1; i < len(all); i++ {
		if all[i].ts < all[i-1].ts {
			t.Fatalf("%s: kept samples are not sorted by time: %+v after %+v (index %d of %d, blocks of %d samples)", name, all[i], all[i-1], i, len(all), len(blocks[0]))
		}
	}
	if !counter {
		return
	}
	// 4. A counter never decreases, and the increase between the first and the last kept bucket equals the input one.
	for i := 1; i < len(all); i++ {
		if all[i].v < all[i-1].v {
			t.Fatalf("%s: negative delta between %+v and %+v", name, all[i-1], all[i])
		}
	}
	first, last := all[0], all[len(all)-1]
	var inFirst, inLast float64
	for _, sm := range input {
		if sm.ts == lastTs[bucketOf(first.ts, interval)] {
			inFirst = sm.v
		}
		if sm.ts == lastTs[bucketOf(last.ts, interval)] {
			inLast = sm.v
		}
	}
	if last.v-first.v != inLast-inFirst {
		t.Fatalf("%s: increase is %v; want %v", name, last.v-first.v, inLast-inFirst)
	}
}

// TestDownsamplingKeepsLastPerBucket checks the thinning invariants for stale markers, duplicate timestamps,
// a counter and series which span several blocks.
func TestDownsamplingKeepsLastPerBucket(t *testing.T) {
	defer testRemoveAll(t)

	const interval = int64(60000)
	stale := decimal.StaleNaN

	synctest.Test(t, func(t *testing.T) {
		withDownsamplingRules(t, 15000, "30d:1m", false)
		s := MustOpenStorage(t.Name(), OpenOptions{Retention: 5 * 365 * 24 * time.Hour})
		defer s.MustClose()

		// Bucket k is (base+k*1m, base+(k+1)*1m].
		base := time.Date(1999, 10, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
		at := func(bucket int, offsetMsecs int64) int64 {
			return base + int64(bucket)*interval + offsetMsecs
		}

		input := make(map[string][]testSample)
		// stale markers at the start, in the middle and at the end of a bucket, and buckets with markers only
		input["ds_ref_stale"] = []testSample{
			{at(0, 5000), 1}, {at(0, 20000), stale}, {at(0, 40000), 3}, {at(0, 60000), 4},
			{at(1, 10000), 1}, {at(1, 30000), 2}, {at(1, 50000), 3}, {at(1, 60000), stale},
			{at(2, 15000), stale}, {at(2, 30000), stale}, {at(2, 45000), stale}, {at(2, 60000), stale},
			{at(3, 15000), stale}, {at(3, 30000), 7}, {at(3, 45000), 8}, {at(3, 60000), 9},
			{at(4, 7000), 5}, {at(4, 50000), stale},
			{at(5, 60000), stale},
			{at(6, 1000), 6}, {at(6, 2000), stale},
			{at(7, 25000), 8},
		}
		// equal timestamps with different values, including at the end of a bucket and next to stale markers
		input["ds_ref_dups"] = []testSample{
			{at(0, 15000), 1}, {at(0, 30000), 2}, {at(0, 30000), 9}, {at(0, 60000), 3}, {at(0, 60000), 7},
			{at(1, 60000), stale}, {at(1, 60000), 4},
			{at(2, 60000), stale}, {at(2, 60000), stale},
			{at(3, 20000), 5}, {at(3, 20000), 2}, {at(3, 40000), 1},
			{at(4, 60000), 2}, {at(4, 60000), 2}, {at(4, 60000), 1},
			{at(5, 30000), stale}, {at(5, 30000), 6},
		}
		// more than maxRowsPerBlock raw samples in the month
		rng := rand.New(rand.NewSource(1))
		input["ds_ref_big"] = genTestSeries(rng, base, 3*maxRowsPerBlock+1000, false)
		input["ds_ref_counter"] = genTestSeries(rng, base, 2*maxRowsPerBlock+500, true)

		addTestSeries(s, input)
		if err := s.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		if st := getTestPartitionStats(s)["1999_10"]; st.minDedup != interval || st.finalNeeded {
			t.Fatalf("partition 1999_10 must be thinned to %d; got %+v", interval, st)
		}

		rows1 := getTestPartitionStats(s)["1999_10"].rows
		got := readTestBlocks(t, s, testRange(1999, 10))
		if len(got) != len(input) {
			t.Fatalf("got %d series; want %d", len(got), len(input))
		}
		for name, samples := range input {
			checkThinned(t, name, samples, got[name], interval, name == "ds_ref_counter")
			t.Logf("%s: %d input samples, %d kept in %d blocks", name, len(samples), len(flattenTestBlocks(got)[name]), len(got[name]))
		}

		// A bucket split between two output blocks keeps a sample in each of them. The next merge of the thinned
		// part collapses these, as in stock, and after that the part is stable.
		merged := func() (string, testPartitionStats) {
			if err := s.ForceMergePartitions(""); err != nil {
				t.Fatalf("cannot force merge: %s", err)
			}
			blocks := readTestBlocks(t, s, testRange(1999, 10))
			for name, samples := range input {
				checkThinned(t, name, samples, blocks[name], interval, name == "ds_ref_counter")
			}
			return testSamplesSig(flattenTestBlocks(blocks)), getTestPartitionStats(s)["1999_10"]
		}
		sig2, st2 := merged()
		sig3, st3 := merged()
		t.Logf("rows after the first merge: %d; after the second: %d; after the third: %d", rows1, st2.rows, st3.rows)
		if sig3 != sig2 || st3.rows != st2.rows || st3.minDedup != st2.minDedup {
			t.Fatalf("repeated force merge changes a stable part: rows %d -> %d", st2.rows, st3.rows)
		}
	})
}

// Thinning in steps gives the same samples as thinning directly to the final interval.
func TestDownsamplingStepsEqualDirect(t *testing.T) {
	defer testRemoveAll(t)

	synctest.Test(t, func(t *testing.T) {
		withDownsamplingRules(t, 15000, "30d:1m,60d:5m", false)
		opts := OpenOptions{Retention: 5 * 365 * 24 * time.Hour}
		rng := rand.New(rand.NewSource(2))
		base := time.Date(1999, 11, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
		// Small series, so that every series is a single block on both sides and the merge shape is the same.
		input := map[string][]testSample{
			"ds_ref_gauge":   genTestSeries(rng, base, 3000, false),
			"ds_ref_counter": genTestSeries(rng, base, 3000, true),
		}

		// 1999_11 ends 31 days before the start of the bubble, so only the 1m rule applies at first.
		stepped := MustOpenStorage(t.Name()+"/stepped", opts)
		addTestSeries(stepped, input)
		if err := stepped.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		if st := getTestPartitionStats(stepped)["1999_11"]; st.minDedup != 60000 {
			t.Fatalf("expecting 1m after the first step; got %+v", st)
		}
		// 71 days after the partition end, the 5m rule applies too. The clock only moves with the storage closed,
		// since the background goroutines of an open storage block the bubble clock.
		stepped.MustClose()
		time.Sleep(40 * 24 * time.Hour)
		stepped = MustOpenStorage(t.Name()+"/stepped", opts)
		defer stepped.MustClose()
		if err := stepped.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		if st := getTestPartitionStats(stepped)["1999_11"]; st.minDedup != 300000 || st.finalNeeded {
			t.Fatalf("expecting 5m after the second step; got %+v", st)
		}

		direct := MustOpenStorage(t.Name()+"/direct", opts)
		defer direct.MustClose()
		addTestSeries(direct, input)
		if err := direct.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		if st := getTestPartitionStats(direct)["1999_11"]; st.minDedup != 300000 {
			t.Fatalf("expecting 5m for direct thinning; got %+v", st)
		}

		got := testSamplesSig(flattenTestBlocks(readTestBlocks(t, stepped, testRange(1999, 11))))
		want := testSamplesSig(flattenTestBlocks(readTestBlocks(t, direct, testRange(1999, 11))))
		if got != want {
			t.Fatalf("stepped thinning differs from direct thinning")
		}
	})
}

// A late backfill into a thinned month gives the same samples as thinning the raw data and the backfill together.
func TestDownsamplingBackfillEqualsDirect(t *testing.T) {
	defer testRemoveAll(t)

	synctest.Test(t, func(t *testing.T) {
		withDownsamplingRules(t, 15000, "30d:1m", false)
		opts := OpenOptions{Retention: 5 * 365 * 24 * time.Hour}
		rng := rand.New(rand.NewSource(3))
		base := time.Date(1999, 10, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
		input := map[string][]testSample{
			"ds_ref_gauge":   genTestSeries(rng, base, 3000, false),
			"ds_ref_counter": genTestSeries(rng, base, 3000, true),
		}
		backfill, union := genTestBackfill(rng, input)

		late := MustOpenStorage(t.Name()+"/late", opts)
		defer late.MustClose()
		addTestSeries(late, input)
		if err := late.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		addTestSeries(late, backfill)
		if err := late.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		if st := getTestPartitionStats(late)["1999_10"]; st.minDedup != 60000 || st.finalNeeded {
			t.Fatalf("expecting a thinned month after the backfill; got %+v", st)
		}

		direct := MustOpenStorage(t.Name()+"/direct", opts)
		defer direct.MustClose()
		addTestSeries(direct, union)
		if err := direct.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}

		got := testSamplesSig(flattenTestBlocks(readTestBlocks(t, late, testRange(1999, 10))))
		want := testSamplesSig(flattenTestBlocks(readTestBlocks(t, direct, testRange(1999, 10))))
		if got != want {
			t.Fatalf("thinning with a late backfill differs from thinning everything at once")
		}
	})
}

// genTestBackfill returns a backfill which overlaps input: new timestamps, and equal timestamps with other values.
// It also returns the union of input and the backfill.
func genTestBackfill(rng *rand.Rand, input map[string][]testSample) (map[string][]testSample, map[string][]testSample) {
	backfill := make(map[string][]testSample)
	union := make(map[string][]testSample)
	for name, samples := range input {
		for i, sm := range samples {
			// stale markers stay stale markers: any other NaN is dropped on ingestion
			shift := func(d float64) float64 {
				if decimal.IsStaleNaN(sm.v) {
					return sm.v
				}
				return sm.v + d
			}
			switch {
			case i%3 == 0:
				backfill[name] = append(backfill[name], testSample{sm.ts + 1 + rng.Int63n(14000), shift(1000)})
			case i%7 == 0:
				backfill[name] = append(backfill[name], testSample{sm.ts, shift(500)})
			case i%11 == 0:
				backfill[name] = append(backfill[name], testSample{sm.ts, decimal.StaleNaN})
			}
		}
		union[name] = append(slices.Clone(samples), backfill[name]...)
	}
	return backfill, union
}

// A stock backfill into a thinned month is thinned without rewriting the parts which already claim the interval.
func TestDownsamplingMergesOnlyUnderStampedParts(t *testing.T) {
	defer testRemoveAll(t)

	const interval = int64(60000)
	opts := OpenOptions{Retention: 5 * 365 * 24 * time.Hour}

	synctest.Test(t, func(t *testing.T) {
		rng := rand.New(rand.NewSource(4))
		base := time.Date(1999, 10, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
		input := map[string][]testSample{
			"ds_ref_gauge":   genTestSeries(rng, base, 3000, false),
			"ds_ref_counter": genTestSeries(rng, base, 3000, true),
		}
		backfill, union := genTestBackfill(rng, input)

		withDownsamplingRules(t, 15000, "30d:1m", false)
		s := MustOpenStorage(t.Name()+"/s", opts)
		defer func() { s.MustClose() }()
		addTestSeries(s, input)
		if err := s.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		thinnedPaths := getTestPartitionStats(s)["1999_10"].partPaths
		s.MustClose()

		// stock writes the backfill as new parts which claim only the global interval
		withDownsamplingRules(t, 15000, "", false)
		// merge indexes derive from the clock, which is frozen in the bubble
		time.Sleep(time.Second)
		s = MustOpenStorage(t.Name()+"/s", opts)
		addTestSeries(s, backfill)
		s.MustClose()

		withDownsamplingRules(t, 15000, "30d:1m", false)
		time.Sleep(time.Second)
		s = MustOpenStorage(t.Name()+"/s", opts)
		mergeOld := func() {
			ptws := s.tb.GetAllPartitions(nil)
			defer s.tb.PutPartitions(ptws)
			for _, ptw := range ptws {
				if ptw.pt.name == "1999_10" {
					if err := ptw.pt.mergeUnderStampedParts(nil); err != nil {
						t.Fatalf("cannot merge: %s", err)
					}
				}
			}
		}
		if !getTestPartitionStats(s)["1999_10"].finalNeeded {
			t.Fatalf("the backfilled month must need thinning")
		}
		mergeOld()
		st := getTestPartitionStats(s)["1999_10"]
		if st.finalNeeded || st.minDedup != interval {
			t.Fatalf("the backfilled month must be thinned; got %+v", st)
		}
		for _, p := range thinnedPaths {
			if !slices.Contains(st.partPaths, p) {
				t.Fatalf("thinned part %s must not be rewritten; parts now: %v", p, st.partPaths)
			}
		}
		if len(st.partPaths) <= len(thinnedPaths) {
			t.Fatalf("expecting the backfill in a separate part; got %v", st.partPaths)
		}
		mergeOld()
		if again := getTestPartitionStats(s)["1999_10"]; !slices.Equal(again.partPaths, st.partPaths) {
			t.Fatalf("a second merge must be a no-op; got %v; want %v", again.partPaths, st.partPaths)
		}

		// The parts are thinned separately, so a bucket keeps at most one sample from each of them.
		got := flattenTestBlocks(readTestBlocks(t, s, testRange(1999, 10)))
		for name, samples := range union {
			checkThinnedParts(t, name, samples, got[name], interval)
		}

		// The next merge which joins the parts gives the same samples as thinning everything at once.
		if err := s.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		direct := MustOpenStorage(t.Name()+"/direct", opts)
		defer direct.MustClose()
		addTestSeries(direct, union)
		if err := direct.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot force merge: %s", err)
		}
		gotSig := testSamplesSig(flattenTestBlocks(readTestBlocks(t, s, testRange(1999, 10))))
		wantSig := testSamplesSig(flattenTestBlocks(readTestBlocks(t, direct, testRange(1999, 10))))
		if gotSig != wantSig {
			t.Fatalf("thinning the joined parts differs from thinning everything at once")
		}
	})
}

// checkThinnedParts checks kept samples from two parts thinned separately: every kept sample is an input sample,
// no bucket is lost, a bucket keeps at most 2 samples, and the last input sample of every bucket is kept.
func checkThinnedParts(t *testing.T, name string, input, kept []testSample, interval int64) {
	t.Helper()
	values := make(map[int64][]float64)
	lastTs := make(map[int64]int64)
	for _, sm := range input {
		values[sm.ts] = append(values[sm.ts], sm.v)
		b := bucketOf(sm.ts, interval)
		lastTs[b] = max(lastTs[b], sm.ts)
	}
	byBucket := make(map[int64][]testSample)
	for _, sm := range kept {
		if !slices.ContainsFunc(values[sm.ts], func(v float64) bool { return sameTestValue(v, sm.v) }) {
			t.Fatalf("%s: kept sample %+v is not in the input", name, sm)
		}
		b := bucketOf(sm.ts, interval)
		byBucket[b] = append(byBucket[b], sm)
	}
	if len(byBucket) != len(lastTs) {
		t.Fatalf("%s: kept %d buckets; input has %d", name, len(byBucket), len(lastTs))
	}
	for b, ts := range lastTs {
		k := byBucket[b]
		if len(k) > 2 || !slices.ContainsFunc(k, func(sm testSample) bool { return sm.ts == ts }) {
			t.Fatalf("%s: bucket %d keeps %+v; want at most 2 samples including ts=%d", name, b, k, ts)
		}
	}
}
