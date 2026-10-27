package storage

import (
	"math/rand"
	"slices"
	"testing"
	"time"
)

const (
	testMinute = int64(60 * 1000)
	testDay    = int64(24 * 3600 * 1000)
)

func TestParseDownsamplingRules(t *testing.T) {
	f := func(s string, dedupInterval int64, want []downsamplingRule) {
		t.Helper()
		got, err := parseDownsamplingRules(s, dedupInterval)
		if err != nil {
			t.Fatalf("unexpected error for %q: %s", s, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("unexpected rules for %q; got %v; want %v", s, got, want)
		}
	}
	f("", 0, nil)
	f(" ", 15000, nil)
	f("90d:1m", 15000, []downsamplingRule{{90 * testDay, testMinute}})
	f("90d:1m,1y:5m", 15000, []downsamplingRule{{90 * testDay, testMinute}, {365 * testDay, 5 * testMinute}})
	f("30d:1m, 60d:5m,120d:1h", 60000, []downsamplingRule{{30 * testDay, testMinute}, {60 * testDay, 5 * testMinute}, {120 * testDay, 60 * testMinute}})
	// interval equal to the dedup interval is a no-op, but valid
	f("30d:15s", 15000, []downsamplingRule{{30 * testDay, 15000}})
	// short offsets are only warned about
	f("1d:1m", 15000, []downsamplingRule{{testDay, testMinute}})

	fail := func(s string, dedupInterval int64) {
		t.Helper()
		if rules, err := parseDownsamplingRules(s, dedupInterval); err == nil {
			t.Fatalf("expecting error for %q; got rules %v", s, rules)
		}
	}
	fail("90d:1m", 0)                   // dedup is disabled
	fail("90d", 15000)                  // missing interval
	fail("90d:", 15000)                 // empty interval
	fail(":1m", 15000)                  // empty offset
	fail("foo:1m", 15000)               // bad offset
	fail("90d:foo", 15000)              // bad interval
	fail("0:1m", 15000)                 // zero offset
	fail("0d:1m", 15000)                // zero offset
	fail("-1d:1m", 15000)               // negative offset
	fail("90d:0s", 15000)               // zero interval
	fail("90d:1m,60d:5m", 15000)        // offsets decrease
	fail("90d:1m,90d:5m", 15000)        // offsets are equal
	fail("30d:5m,60d:1m", 15000)        // intervals decrease
	fail("30d:5m,60d:5m", 15000)        // intervals are equal
	fail("30d:1m,60d:90s", 15000)       // not a multiple of the previous interval
	fail("30d:1m,60d:5m,90d:7m", 15000) // not a multiple of the previous interval
	fail("30d:20s", 15000)              // not a multiple of the dedup interval
	fail("30d:1m,", 15000)              // trailing comma
}

func TestGetDownsamplingInterval(t *testing.T) {
	rules := []downsamplingRule{{30 * testDay, testMinute}, {60 * testDay, 5 * testMinute}}
	f := func(maxTimestamp, now, want int64) {
		t.Helper()
		if got := getDownsamplingInterval(rules, maxTimestamp, now); got != want {
			t.Fatalf("unexpected interval for maxTimestamp=%d, now=%d; got %d; want %d", maxTimestamp, now, got, want)
		}
	}
	end := int64(1_000_000_000_000)
	f(end, end, 0)
	f(end, end+30*testDay-1, 0)
	f(end, end+30*testDay, testMinute)
	f(end, end+60*testDay-1, testMinute)
	f(end, end+60*testDay, 5*testMinute)
	f(end, end+1000*testDay, 5*testMinute)
	// partitions from the future are never eligible
	f(end, end-testDay, 0)
}

func withDownsamplingRules(t *testing.T, dedupInterval int64, period string, dryRun bool) {
	t.Helper()
	rules, err := parseDownsamplingRules(period, dedupInterval)
	if err != nil {
		t.Fatalf("cannot parse rules: %s", err)
	}
	prevRules, prevDryRun, prevDedup := downsamplingRules, downsamplingDryRun, globalDedupInterval
	downsamplingRules, downsamplingDryRun, globalDedupInterval = rules, dryRun, dedupInterval
	t.Cleanup(func() {
		downsamplingRules, downsamplingDryRun, globalDedupInterval = prevRules, prevDryRun, prevDedup
	})
}

func TestPartitionMergeDedupInterval(t *testing.T) {
	// Rules 30d:1m,60d:5m with 15s dedup. Partitions are months, so the current month is never eligible.
	withDownsamplingRules(t, 15000, "30d:1m,60d:5m", false)

	newPT := func(year int, month time.Month) *partition {
		start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 1, 0).Add(-time.Millisecond)
		return &partition{tr: TimeRange{MinTimestamp: start.UnixMilli(), MaxTimestamp: end.UnixMilli()}}
	}
	now := time.Date(2025, time.June, 15, 12, 0, 0, 0, time.UTC)
	f := func(pt *partition, now time.Time, want int64) {
		t.Helper()
		if got := pt.getMergeDedupInterval(now.UnixMilli()); got != want {
			t.Fatalf("unexpected interval for tr=%v at %s; got %d; want %d", pt.tr, now, got, want)
		}
	}
	// current and future months
	f(newPT(2025, time.June), now, 15000)
	f(newPT(2025, time.July), now, 15000)
	// May ends on May 31 23:59:59.999. +30d is June 30 23:59:59.999.
	f(newPT(2025, time.May), time.Date(2025, time.June, 30, 23, 59, 59, 998_000_000, time.UTC), 15000)
	f(newPT(2025, time.May), time.Date(2025, time.June, 30, 23, 59, 59, 999_000_000, time.UTC), 60000)
	// +60d is July 30 23:59:59.999
	f(newPT(2025, time.May), time.Date(2025, time.July, 30, 23, 59, 59, 998_000_000, time.UTC), 60000)
	f(newPT(2025, time.May), time.Date(2025, time.July, 30, 23, 59, 59, 999_000_000, time.UTC), 300000)
	f(newPT(2024, time.January), now, 300000)

	// A tiny offset still never makes the current month eligible.
	withDownsamplingRules(t, 15000, "1ms:1m", false)
	f(newPT(2025, time.June), now, 15000)
	f(newPT(2025, time.May), now, 60000)

	// Dry-run forces the global interval.
	withDownsamplingRules(t, 15000, "1ms:1m", true)
	f(newPT(2025, time.May), now, 15000)

	// No rules: the global interval.
	withDownsamplingRules(t, 15000, "", false)
	f(newPT(2020, time.May), now, 15000)
}

// thinLastPerBucket is the reference for the thinning rule: keep the last sample in every (k*interval-interval, k*interval] bucket.
func thinLastPerBucket(timestamps []int64, interval int64) []int64 {
	var dst []int64
	for i, ts := range timestamps {
		if i+1 < len(timestamps) && bucketOf(timestamps[i+1], interval) == bucketOf(ts, interval) {
			continue
		}
		dst = append(dst, ts)
	}
	return dst
}

func bucketOf(ts, interval int64) int64 {
	return (ts + interval - 1) / interval
}

func thinForTest(timestamps []int64, interval int64) []int64 {
	ts := slices.Clone(timestamps)
	values := slices.Clone(ts) // value equals timestamp, so the kept value can be checked too
	ts, values = deduplicateSamplesDuringMerge(ts, values, interval)
	if !slices.Equal(ts, values) {
		panic("BUG: values do not follow timestamps")
	}
	return ts
}

func genTimestamps(rng *rand.Rand, start int64, n int) []int64 {
	// Irregular scrape with jitter and gaps, roughly 15s apart.
	ts := make([]int64, 0, n)
	t := start
	for range n {
		t += 15000 + rng.Int63n(2001) - 1000
		if rng.Intn(50) == 0 {
			t += rng.Int63n(20 * 60 * 1000)
		}
		ts = append(ts, t)
	}
	return ts
}

func TestThinningMatchesLastPerBucket(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, interval := range []int64{15000, testMinute, 5 * testMinute} {
		for range 20 {
			src := genTimestamps(rng, 1_700_000_000_000+rng.Int63n(1e9), 3000)
			got := thinForTest(src, interval)
			want := thinLastPerBucket(src, interval)
			if !slices.Equal(got, want) {
				t.Fatalf("interval=%d: unexpected thinning result; got %d samples; want %d", interval, len(got), len(want))
			}
		}
	}
}

func TestThinningStepEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	steps := [][]int64{
		{15000, testMinute, 5 * testMinute},
		{testMinute, 5 * testMinute, 60 * testMinute},
		{15000, 5 * testMinute},
	}
	for range 20 {
		src := genTimestamps(rng, 1_700_000_000_000+rng.Int63n(1e9), 5000)
		for _, chain := range steps {
			stepped := src
			for _, interval := range chain {
				stepped = thinForTest(stepped, interval)
			}
			direct := thinForTest(src, chain[len(chain)-1])
			if !slices.Equal(stepped, direct) {
				t.Fatalf("chain %v: stepped thinning gives %d samples; direct gives %d", chain, len(stepped), len(direct))
			}
		}
	}
	// Intervals which are not multiples of each other are not equivalent. This is why the rules are validated.
	src := genTimestamps(rng, 1_700_000_000_000, 5000)
	if slices.Equal(thinForTest(thinForTest(src, 90000), 120000), thinForTest(src, 120000)) {
		t.Fatalf("expecting different results for non-multiple intervals")
	}
}

// Merging a block of late samples into a thinned month is modeled as thinning each block separately
// and then thinning the concatenated output, which is what the merge does with the blocks it writes.
func TestThinningLateWrites(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	const interval = testMinute

	countPerBucket := func(timestamps []int64) map[int64]int {
		m := make(map[int64]int)
		for _, ts := range timestamps {
			m[bucketOf(ts, interval)]++
		}
		return m
	}
	merge := func(a, b []int64) []int64 {
		m := append(slices.Clone(a), b...)
		slices.Sort(m)
		return m
	}

	for range 50 {
		orig := genTimestamps(rng, 1_700_000_000_000, 4000)
		thinned := thinForTest(orig, interval)
		want := thinLastPerBucket(orig, interval)

		// Replay a window of the original samples: the window starts and ends at arbitrary points.
		from := rng.Intn(len(orig) / 2)
		to := from + 1 + rng.Intn(len(orig)/2)
		replay := orig[from:to]

		// 1. The replay is merged into the same output block: exactly one sample per bucket and nothing lost.
		whole := thinForTest(merge(thinned, replay), interval)
		if !slices.Equal(whole, want) {
			t.Fatalf("replay merged in one block must give the same samples as thinning the original; got %d; want %d", len(whole), len(want))
		}

		// 2. The replay lands in a different output block than the thinned data: edge buckets may keep 2 samples.
		cut := thinned[rng.Intn(len(thinned))]
		var left, right []int64
		for _, ts := range merge(thinned, replay) {
			if ts <= cut {
				left = append(left, ts)
			} else {
				right = append(right, ts)
			}
		}
		split := append(thinForTest(left, interval), thinForTest(right, interval)...)
		for b, n := range countPerBucket(split) {
			if n > 2 {
				t.Fatalf("bucket %d has %d samples; want at most 2", b, n)
			}
		}
		// Nothing is lost relative to "last per bucket".
		for _, ts := range want {
			if !slices.Contains(split, ts) {
				t.Fatalf("sample %d, the last in its bucket, is lost", ts)
			}
		}
		// A later merge restores exactly one sample per bucket.
		if again := thinForTest(split, interval); !slices.Equal(again, want) {
			t.Fatalf("re-thinning the split output must give the original thinning; got %d; want %d", len(again), len(want))
		}
	}
}
