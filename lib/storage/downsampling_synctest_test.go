//go:build synctest

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

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
				if err := ptw.pt.ForceMergeAllParts(nil); err != nil {
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
