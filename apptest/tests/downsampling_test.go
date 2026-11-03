package tests

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/apptest"
)

// The tests below switch one vmstorage data dir between the fork (with VM_FORK_DOWNSAMPLING_PERIOD)
// and a stock vmstorage. They need prebuilt binaries and are skipped otherwise:
//
//	DOWNSAMPLING_E2E_FORK_VMSTORAGE=/path/to/fork/vmstorage
//	DOWNSAMPLING_E2E_STOCK_VMSTORAGE=/path/to/stock/vmstorage
//
// vminsert and vmselect come from VMINSERT_PATH and VMSELECT_PATH as usual.
// app/vmstorage/downsampling-e2e/run.sh builds the fork binaries and runs the tests in Docker.
const (
	downsamplingForkBinaryEnv  = "DOWNSAMPLING_E2E_FORK_VMSTORAGE"
	downsamplingStockBinaryEnv = "DOWNSAMPLING_E2E_STOCK_VMSTORAGE"

	dsStepMsecs   = 15_000
	dsDedupFlag   = "-dedup.minScrapeInterval=15s"
	dsSeries      = 20
	dsWindowCount = 1440 // 6 hours of 15s samples, which fits a single block of a series
)

type dsRule struct {
	offset   time.Duration
	interval time.Duration
}

func TestClusterDownsamplingSwitch_oneRule(t *testing.T) {
	testDownsamplingSwitch(t, []dsRule{{30 * 24 * time.Hour, time.Minute}}, 18400)
}

func TestClusterDownsamplingSwitch_twoRules(t *testing.T) {
	testDownsamplingSwitch(t, []dsRule{{30 * 24 * time.Hour, time.Minute}, {60 * 24 * time.Hour, 5 * time.Minute}}, 18420)
}

type dsSample struct {
	series string
	ts     int64
	value  float64
}

type dsPoint struct {
	Ts    int64
	Value float64 // exported for the comparison with go-cmp
}

type dsEnv struct {
	t        *testing.T
	tc       *apptest.TestCase
	rules    []dsRule
	period   string
	now      time.Time
	fork     string
	stock    string
	dataPath string
	ports    [3]int
	storage  *apptest.Vmstorage
	vminsert *apptest.Vminsert
	vmselect *apptest.Vmselect
	raw      []dsSample
	// listed holds the part dirs which parts.json has listed at some point. Merged-away parts are removed
	// asynchronously, so an unlisted dir is a leftover of a crash only if it has never been listed.
	listed map[string]bool
}

// newDsEnv starts the fork vmstorage with the rules, vminsert and vmselect.
func newDsEnv(t *testing.T, fork, stock string, rules []dsRule, basePort int) *dsEnv {
	tc := apptest.NewTestCase(t)
	var parts []string
	for _, r := range rules {
		parts = append(parts, fmt.Sprintf("%dh:%ds", int(r.offset.Hours()), int(r.interval.Seconds())))
	}
	e := &dsEnv{
		t:        t,
		tc:       tc,
		rules:    rules,
		period:   strings.Join(parts, ","),
		now:      time.Now().UTC(),
		fork:     fork,
		stock:    stock,
		dataPath: filepath.Join(tc.Dir(), "vmstorage"),
		ports:    [3]int{basePort, basePort + 1, basePort + 2},
		listed:   make(map[string]bool),
	}
	return e
}

// startCluster starts vmstorage, then vminsert and vmselect on top of it.
func (e *dsEnv) startCluster(fork bool) {
	e.startStorage(fork)
	e.vminsert = e.tc.MustStartVminsert("vminsert", []string{
		fmt.Sprintf("-storageNode=127.0.0.1:%d", e.ports[1]),
	})
	e.vmselect = e.tc.MustStartVmselect("vmselect", []string{
		fmt.Sprintf("-storageNode=127.0.0.1:%d", e.ports[2]),
		"-search.maxExportDuration=100000h",
	})
}

func testDownsamplingSwitch(t *testing.T, rules []dsRule, basePort int) {
	fork, stock := os.Getenv(downsamplingForkBinaryEnv), os.Getenv(downsamplingStockBinaryEnv)
	if fork == "" || stock == "" {
		t.Skipf("set %s and %s to run", downsamplingForkBinaryEnv, downsamplingStockBinaryEnv)
	}
	e := newDsEnv(t, fork, stock, rules, basePort)
	defer e.tc.Stop()
	e.startCluster(true)

	// Data: 4 months far enough in the past for any rule, a window 20 days ago and the last minutes.
	monthStart := time.Date(e.now.Year(), e.now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for _, back := range []int{3, 4, 5, 6} {
		e.insertWindow("ds_metric", dsSeries, monthStart.AddDate(0, -back, 9).UnixMilli(), dsWindowCount)
	}
	e.insertWindow("ds_metric", dsSeries, floorStep(e.now.Add(-20*24*time.Hour).UnixMilli())-dsWindowCount*dsStepMsecs, dsWindowCount)
	e.insertWindow("ds_metric", dsSeries, floorStep(e.now.Add(-time.Minute).UnixMilli())-40*dsStepMsecs, 40)
	e.flush()

	// a. The fork thins old months only.
	e.forceMerge()
	e.assertExport("a: fork force merge", e.expected(e.raw))
	afterA := e.assertParts("a", e.raw, nil)
	for _, m := range e.pastMonths(4) {
		if got := afterA[m].minClaim; got != e.intervalFor(m).Milliseconds() {
			t.Fatalf("a: month %s claims %dms; want %dms", m, got, e.intervalFor(m).Milliseconds())
		}
	}

	// b. Stock reads the same data. Nothing is merged on the switch.
	e.restartStorage(false)
	e.assertExport("b: stock, no changes", e.expected(e.raw))
	afterB := e.assertParts("b", e.raw, nil)
	for m, st := range afterA {
		if !slices.Equal(st.parts, afterB[m].parts) {
			t.Fatalf("b: parts of %s changed on the switch to stock: %v -> %v", m, st.parts, afterB[m].parts)
		}
	}

	// Stock backfill into a thinned month. Stock merges keep the data and claim only the global interval.
	backfillMonth := monthStart.AddDate(0, -4, 0)
	backfill := e.insertWindow("ds_metric", dsSeries, backfillMonth.AddDate(0, 0, 19).UnixMilli(), dsWindowCount)
	e.flush()
	e.forceMerge()
	e.assertExport("b: stock backfill", e.expectedStock(e.raw, backfill))
	afterBackfill := e.assertParts("b backfill", e.raw, backfill)
	if got := afterBackfill[monthKey(backfillMonth)].minClaim; got != dsStepMsecs {
		t.Fatalf("b: stock merge must claim the global interval; got %dms", got)
	}

	// c. The fork thins the month again. Thinning in steps equals thinning the raw data directly.
	e.restartStorage(true)
	e.forceMerge()
	e.assertExport("c: fork re-thins", e.expected(e.raw))
	e.assertParts("c", e.raw, nil)

	// d. kill -9 during a force merge of a big backfill: no stray parts, no data loss, thinning completes.
	caught := false
	for attempt := range 5 {
		series := fmt.Sprintf("ds_big%d", attempt)
		e.insertWindow(series, 500, monthStart.AddDate(0, -5, 4).UnixMilli(), dsWindowCount)
		e.flush()
		if e.killDuringForceMerge() {
			caught = true
		}
		e.restartStorage(true)
		if strays := e.strayParts(); len(strays) > 0 {
			t.Fatalf("d: stray parts after restart: %v", strays)
		}
		e.forceMerge()
		e.assertExport(fmt.Sprintf("d: attempt %d after restart and force merge", attempt), e.expected(e.raw))
		e.assertParts("d", e.raw, nil)
		if caught {
			break
		}
	}
	t.Logf("d: kill -9 landed in the middle of a force merge: %v", caught)
	if !caught {
		t.Logf("d: WARNING the merge finished before the kill in every attempt; the crash path was not exercised")
	}
}

func floorStep(ts int64) int64 {
	return ts - ts%dsStepMsecs
}

func monthKey(t time.Time) string {
	return t.UTC().Format("2006_01")
}

// pastMonths returns the keys of the n months ending at the third month before now.
func (e *dsEnv) pastMonths(n int) []string {
	monthStart := time.Date(e.now.Year(), e.now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var a []string
	for back := 3; back < 3+n; back++ {
		a = append(a, monthKey(monthStart.AddDate(0, -back, 0)))
	}
	return a
}

func (e *dsEnv) storageFlags(dedupFlag string) []string {
	return []string{
		"-storageDataPath=" + e.dataPath,
		"-retentionPeriod=100y",
		dedupFlag,
		"-inmemoryDataFlushInterval=1s",
		fmt.Sprintf("-httpListenAddr=127.0.0.1:%d", e.ports[0]),
		fmt.Sprintf("-vminsertAddr=127.0.0.1:%d", e.ports[1]),
		fmt.Sprintf("-vmselectAddr=127.0.0.1:%d", e.ports[2]),
	}
}

func (e *dsEnv) startStorage(fork bool) {
	e.t.Helper()
	if fork {
		e.storage = e.tc.MustStartVmstorageWithBinary("vmstorage", e.fork, []string{"VM_FORK_DOWNSAMPLING_PERIOD=" + e.period}, e.storageFlags(dsDedupFlag))
		return
	}
	e.storage = e.tc.MustStartVmstorageWithBinary("vmstorage", e.stock, nil, e.storageFlags(dsDedupFlag))
}

func (e *dsEnv) restartStorage(fork bool) {
	e.t.Helper()
	e.tc.StopApp("vmstorage")
	e.startStorage(fork)
}

// waitIdle waits until background merges are done. /internal/force_merge silently does nothing for a partition
// with a merge in progress.
func (e *dsEnv) waitIdle() {
	e.t.Helper()
	idle := 0
	for range 600 {
		active := 0.0
		for _, v := range e.storage.GetMetricsByPrefix(e.t, `vm_active_merges{type="storage/`) {
			active += v
		}
		if active == 0 {
			idle++
		} else {
			idle = 0
		}
		if idle >= 5 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.t.Fatalf("background merges did not stop")
}

// waitPersisted waits until all ingested data is flushed from memory to disk.
func (e *dsEnv) waitPersisted() {
	e.t.Helper()
	for range 300 {
		e.flush()
		if e.storage.GetMetric(e.t, `vm_parts{type="storage/inmemory"}`) == 0 && e.storage.GetMetric(e.t, `vm_pending_rows{type="storage"}`) == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.t.Fatalf("data was not persisted")
}

// forceMergeRunning reports whether /internal/force_merge, which runs in the background, is still working.
func (e *dsEnv) forceMergeRunning() bool {
	e.t.Helper()
	return e.storage.GetMetric(e.t, "vm_active_force_merges") > 0
}

// waitForceMergeDone waits for the force merge started by the preceding request.
func (e *dsEnv) waitForceMergeDone() {
	e.t.Helper()
	start := time.Now()
	seen := false
	for time.Since(start) < 5*time.Minute {
		if e.forceMergeRunning() {
			seen = true
		} else if seen || time.Since(start) > 3*time.Second {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("force merge did not finish in time")
}

// forceMerge merges every partition to a single part.
func (e *dsEnv) forceMerge() {
	e.t.Helper()
	for range 10 {
		e.waitIdle()
		e.storage.ForceMerge(e.t)
		e.waitForceMergeDone()
		parts, _ := e.readParts(false)
		single := true
		for _, st := range parts {
			single = single && len(st.parts) <= 1
		}
		if single {
			return
		}
	}
	e.t.Fatalf("force merge did not merge every partition to a single part")
}

func (e *dsEnv) flush() {
	e.t.Helper()
	e.storage.ForceFlush(e.t)
}

// insertWindow ingests count samples per series at 15s steps starting right after start.
func (e *dsEnv) insertWindow(name string, series int, start int64, count int) []dsSample {
	return e.insertWindowValues(name, series, start, count, 0)
}

// insertWindowValues is insertWindow with valueAdd added to every value, so that a window overlapping another one
// ingests the same timestamps with different values.
func (e *dsEnv) insertWindowValues(name string, series int, start int64, count int, valueAdd float64) []dsSample {
	e.t.Helper()
	start = floorStep(start)
	var added []dsSample
	var batch []string
	send := func() {
		if len(batch) > 0 {
			e.vminsert.PrometheusAPIV1ImportPrometheus(e.t, batch, apptest.QueryOpts{})
			batch = batch[:0]
		}
	}
	for i := range series {
		key := fmt.Sprintf(`%s{idx="%d"}`, name, i)
		for j := 1; j <= count; j++ {
			ts := start + int64(j)*dsStepMsecs
			value := float64(ts/dsStepMsecs) + valueAdd // unique per timestamp, so a wrong pick of the kept sample is visible
			added = append(added, dsSample{key, ts, value})
			batch = append(batch, fmt.Sprintf("%s %v %d", key, value, ts))
			if len(batch) >= 20000 {
				send()
			}
		}
	}
	send()
	e.raw = append(e.raw, added...)
	return added
}

// intervalFor returns the interval a month must have at e.now.
func (e *dsEnv) intervalFor(month string) time.Duration {
	start, err := time.Parse("2006_01", month)
	if err != nil {
		e.t.Fatalf("bad month %q", month)
	}
	end := start.AddDate(0, 1, 0).Add(-time.Millisecond)
	interval := time.Duration(dsStepMsecs) * time.Millisecond
	for _, r := range e.rules {
		if end.Add(r.offset).Compare(e.now) <= 0 {
			interval = r.interval
		}
	}
	return interval
}

// expected returns the series after thinning every month to its interval: the last sample per bucket (k*I-I, k*I].
func (e *dsEnv) expected(samples []dsSample) map[string][]dsPoint {
	return e.thin(samples, nil)
}

// expectedStock is the expected state after stock merged the backfill into thinned months: the backfill stays as is.
func (e *dsEnv) expectedStock(all, backfill []dsSample) map[string][]dsPoint {
	inBackfill := make(map[dsSample]bool, len(backfill))
	for _, s := range backfill {
		inBackfill[s] = true
	}
	var old, rest []dsSample
	for _, s := range all {
		if inBackfill[s] {
			rest = append(rest, s)
		} else {
			old = append(old, s)
		}
	}
	return e.thin(old, rest)
}

func (e *dsEnv) thin(thinned, verbatim []dsSample) map[string][]dsPoint {
	type key struct{ series, month string }
	groups := make(map[key][]dsPoint)
	for _, s := range thinned {
		k := key{s.series, monthKey(time.UnixMilli(s.ts))}
		groups[k] = append(groups[k], dsPoint{s.ts, s.value})
	}
	res := make(map[string][]dsPoint)
	for k, tss := range groups {
		sortPoints(tss)
		interval := e.intervalFor(k.month).Milliseconds()
		for i, p := range tss {
			if i+1 < len(tss) && (tss[i+1].Ts+interval-1)/interval == (p.Ts+interval-1)/interval {
				continue
			}
			res[k.series] = append(res[k.series], p)
		}
	}
	for _, s := range verbatim {
		res[s.series] = append(res[s.series], dsPoint{s.ts, s.value})
	}
	for _, tss := range res {
		sortPoints(tss)
	}
	return res
}

func sortPoints(a []dsPoint) {
	slices.SortFunc(a, func(x, y dsPoint) int { return cmp.Compare(x.Ts, y.Ts) })
}

func (e *dsEnv) export() map[string][]dsPoint {
	e.t.Helper()
	u := fmt.Sprintf("http://%s/select/0/prometheus/api/v1/export", e.vmselect.HTTPAddr())
	form := url.Values{}
	form.Set("match[]", `{__name__=~"ds_.*"}`)
	form.Set("start", "0")
	form.Set("end", fmt.Sprint(e.now.Add(24*time.Hour).Unix()))
	body, status := e.tc.Client().PostForm(e.t, u, form, nil)
	if status != 200 {
		return map[string][]dsPoint{"error": {{Ts: int64(status)}}}
	}
	res := make(map[string][]dsPoint)
	for line := range strings.SplitSeq(body, "\n") {
		if line == "" {
			continue
		}
		var r struct {
			Metric     map[string]string `json:"metric"`
			Timestamps []int64           `json:"timestamps"`
			Values     []float64         `json:"values"`
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			e.t.Fatalf("cannot parse export line: %s", err)
		}
		key := fmt.Sprintf(`%s{idx="%s"}`, r.Metric["__name__"], r.Metric["idx"])
		for i, ts := range r.Timestamps {
			res[key] = append(res[key], dsPoint{ts, r.Values[i]})
		}
	}
	for _, tss := range res {
		sortPoints(tss)
	}
	return res
}

// assertExport checks the export against the expectation with retries, since vmselect reconnects after storage restarts.
//
// A merge dedups every output block on its own and never joins a full block with the next one of the series,
// so a bucket split at a block boundary keeps a sample on each side, as in stock. Where the blocks split depends
// on the ingestion timing. That is why exact equality is relaxed to invariants:
//  1. every exported sample exists in the raw input with the same value,
//  2. every expected sample is exported,
//  3. an extra sample is allowed only in a bucket which has its expected sample, one per bucket, and at most
//     1% of the expected samples of the series (but at least one).
//
// The oracle test compares exactly.
func (e *dsEnv) assertExport(msg string, want map[string][]dsPoint) {
	e.t.Helper()
	extras := 0
	e.tc.Assert(&apptest.AssertOptions{
		Msg: msg,
		Got: func() any {
			problem, n := e.checkExport(e.export(), want)
			extras = n
			return problem
		},
		Want:    "",
		Retries: 100,
		Period:  200 * time.Millisecond,
		FailNow: true,
	})
	e.t.Logf("%s: %d extra samples at block boundaries", msg, extras)
}

// checkExport returns the first violated invariant of assertExport, if any, and the number of extra samples.
func (e *dsEnv) checkExport(got, want map[string][]dsPoint) (string, int) {
	raw := make(map[string]map[int64]float64)
	for _, s := range e.raw {
		if raw[s.series] == nil {
			raw[s.series] = make(map[int64]float64)
		}
		raw[s.series][s.ts] = s.value
	}
	bucketOf := func(ts int64) int64 {
		interval := e.intervalFor(monthKey(time.UnixMilli(ts))).Milliseconds()
		return (ts + interval - 1) / interval
	}
	total := 0
	for series, g := range got {
		if _, ok := want[series]; !ok {
			return fmt.Sprintf("unexpected series %s", series), 0
		}
		wantTs := make(map[int64]float64, len(want[series]))
		wantBuckets := make(map[int64]bool, len(want[series]))
		for _, p := range want[series] {
			wantTs[p.Ts] = p.Value
			wantBuckets[bucketOf(p.Ts)] = true
		}
		extraInBucket := make(map[int64]bool)
		extras := 0
		seen := 0
		for _, p := range g {
			if v, ok := raw[series][p.Ts]; !ok || v != p.Value {
				return fmt.Sprintf("%s: exported %+v is not in the raw input", series, p), 0
			}
			if _, ok := wantTs[p.Ts]; ok {
				seen++
				continue
			}
			b := bucketOf(p.Ts)
			if !wantBuckets[b] || extraInBucket[b] {
				return fmt.Sprintf("%s: unexpected extra sample %+v", series, p), 0
			}
			extraInBucket[b] = true
			extras++
		}
		if seen != len(wantTs) {
			return fmt.Sprintf("%s: %d expected samples are missing", series, len(wantTs)-seen), 0
		}
		// A series of a 5m month has only 72 samples, so allow one extra there.
		if extras > max(1, len(wantTs)/100) {
			return fmt.Sprintf("%s: %d extra samples for %d expected", series, extras, len(wantTs)), 0
		}
		total += extras
	}
	for series := range want {
		if _, ok := got[series]; !ok {
			return fmt.Sprintf("missing series %s", series), 0
		}
	}
	return "", total
}

type dsPartitionState struct {
	rows     uint64
	minClaim int64
	parts    []string
}

// scanParts reads the on-disk state from parts.json and metadata.json of the listed parts. It also returns the
// directories which parts.json doesn't list.
func (e *dsEnv) scanParts(strict bool) (map[string]dsPartitionState, []string, error) {
	res := make(map[string]dsPartitionState)
	var strays []string
	smallRoot := filepath.Join(e.dataPath, "data", "small")
	months, err := os.ReadDir(smallRoot)
	if err != nil {
		return nil, nil, err
	}
	readPartsJSON := func(smallDir string) (names struct{ Small, Big []string }, err error) {
		data, err := os.ReadFile(filepath.Join(smallDir, "parts.json"))
		if err != nil {
			return names, err
		}
		return names, json.Unmarshal(data, &names)
	}
	for _, m := range months {
		if _, err := time.Parse("2006_01", m.Name()); err != nil {
			continue // snapshots and other non-partition dirs
		}
		smallDir := filepath.Join(smallRoot, m.Name())
		bigDir := filepath.Join(e.dataPath, "data", "big", m.Name())
		pj, err := readPartsJSON(smallDir)
		if err != nil {
			return nil, nil, err
		}
		st := dsPartitionState{minClaim: -1}
		for _, d := range []struct {
			dir   string
			names []string
		}{{smallDir, pj.Small}, {bigDir, pj.Big}} {
			entries, err := os.ReadDir(d.dir)
			if err != nil {
				continue
			}
			// parts.json is read again after listing the dir: a part created and registered in between is not a stray.
			pj2, err := readPartsJSON(smallDir)
			if err != nil {
				return nil, nil, err
			}
			names2 := pj2.Small
			if d.dir == bigDir {
				names2 = pj2.Big
			}
			for _, ent := range entries {
				path := filepath.Join(d.dir, ent.Name())
				if ent.IsDir() && !slices.Contains(names2, ent.Name()) && (strict || !e.listed[path]) {
					strays = append(strays, path)
				}
			}
			for _, name := range d.names {
				md, err := os.ReadFile(filepath.Join(d.dir, name, "metadata.json"))
				if err != nil {
					return nil, nil, err
				}
				var ph struct {
					RowsCount        uint64
					MinDedupInterval int64
				}
				if err := json.Unmarshal(md, &ph); err != nil {
					return nil, nil, err
				}
				e.listed[filepath.Join(d.dir, name)] = true
				st.rows += ph.RowsCount
				if st.minClaim < 0 || ph.MinDedupInterval < st.minClaim {
					st.minClaim = ph.MinDedupInterval
				}
				st.parts = append(st.parts, name)
			}
		}
		slices.Sort(st.parts)
		res[m.Name()] = st
	}
	return res, strays, nil
}

// readParts is scanParts, which retries until the leftovers of merged-away parts are gone (strict) or fails.
func (e *dsEnv) readParts(strict bool) (map[string]dsPartitionState, []string) {
	e.t.Helper()
	var res map[string]dsPartitionState
	var strays []string
	for range 100 {
		var err error
		res, strays, err = e.scanParts(strict)
		if err != nil {
			e.t.Fatalf("cannot scan parts: %s", err)
		}
		if len(strays) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return res, strays
}

// strayParts returns the dirs which parts.json doesn't list. It must be called on a freshly started storage.
func (e *dsEnv) strayParts() []string {
	_, strays := e.readParts(true)
	return strays
}

// assertParts checks rows and claims of every month on disk against the expectation after a full merge.
func (e *dsEnv) assertParts(msg string, all, verbatim []dsSample) map[string]dsPartitionState {
	e.t.Helper()
	var want map[string][]dsPoint
	if verbatim != nil {
		want = e.expectedStock(all, verbatim)
	} else {
		want = e.expected(all)
	}
	wantRows := make(map[string]uint64)
	for _, tss := range want {
		for _, p := range tss {
			wantRows[monthKey(time.UnixMilli(p.Ts))]++
		}
	}
	got, strays := e.readParts(false)
	if len(strays) > 0 {
		e.t.Fatalf("%s: stray parts: %v", msg, strays)
	}
	for m, rows := range wantRows {
		// Samples kept on both sides of a block boundary are the extras allowed by assertExport.
		if got[m].rows < rows || got[m].rows*100 > rows*101 {
			e.t.Fatalf("%s: month %s has %d rows on disk; want %d to %d", msg, m, got[m].rows, rows, rows*101/100)
		}
	}
	return got
}

// killDuringForceMerge starts a force merge and sends SIGKILL as soon as the merge has created a part
// directory which parts.json doesn't list. It returns false if the merge finished first.
func (e *dsEnv) killDuringForceMerge() bool {
	e.t.Helper()
	// kill -9 loses what is not on disk yet, which has nothing to do with the merge under test.
	e.waitPersisted()
	e.waitIdle()
	e.readParts(false)
	e.storage.ForceMerge(e.t)
	start := time.Now()
	for i := 0; time.Since(start) < 5*time.Minute; i++ {
		if _, strays, err := e.scanParts(false); err == nil && len(strays) > 0 {
			e.tc.KillApp("vmstorage")
			e.t.Logf("d: killed in the middle of a merge after %s; unlisted dirs at kill: %v", time.Since(start), strays)
			return true
		}
		if i%20 == 0 && !e.forceMergeRunning() && i > 0 {
			e.t.Logf("d: force merge finished before the kill (%s)", time.Since(start))
			return false
		}
		time.Sleep(time.Millisecond)
	}
	e.t.Fatalf("d: force merge did not finish in time")
	return false
}

// TestClusterDownsamplingOracle uses stock vmstorage as the oracle: for every month the fork thins to interval I,
// the result must equal what stock produces from the same data with -dedup.minScrapeInterval=I.
// Months the fork doesn't thin must stay byte-identical.
//
// Prometheus staleness markers can't be ingested through vminsert except with OpenTelemetry, so this test has none.
// They are covered by the unit tests.
func TestClusterDownsamplingOracle(t *testing.T) {
	fork, stock := os.Getenv(downsamplingForkBinaryEnv), os.Getenv(downsamplingStockBinaryEnv)
	if fork == "" || stock == "" {
		t.Skipf("set %s and %s to run", downsamplingForkBinaryEnv, downsamplingStockBinaryEnv)
	}
	rules := []dsRule{{30 * 24 * time.Hour, time.Minute}, {60 * 24 * time.Hour, 5 * time.Minute}}
	e := newDsEnv(t, fork, stock, rules, 18440)
	defer e.tc.Stop()
	period := e.period
	e.period = "" // no rules: the fork behaves as stock with 15s dedup while the data is ingested
	e.startCluster(true)

	// Every month of the last half year, so that some months are in every interval class, plus the current one.
	// ds_dup has overlapping windows, so that the same timestamps have different values.
	// ds_long has more than 8192 samples per series, so that merges split blocks.
	monthStart := time.Date(e.now.Year(), e.now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for back := 1; back <= 6; back++ {
		start := monthStart.AddDate(0, -back, 9).UnixMilli()
		e.insertWindow("ds_metric", dsSeries, start, dsWindowCount)
		e.insertWindow("ds_long", 2, start+7*24*3600*1000, 9000)
		e.insertWindow("ds_dup", 3, start, dsWindowCount)
		e.insertWindowValues("ds_dup", 3, start+720*dsStepMsecs, dsWindowCount, 7000)
		e.insertWindowValues("ds_dup", 3, start+1000*dsStepMsecs, dsWindowCount, -3000)
	}
	e.insertWindow("ds_metric", dsSeries, floorStep(e.now.Add(-time.Minute).UnixMilli())-40*dsStepMsecs, 40)
	e.flush()
	e.waitPersisted()
	// One part per month, so that all runs below merge the same parts.
	e.forceMerge()
	raw := e.exportRetry()
	e.tc.StopApp("vmstorage")

	base := e.dataPath
	copyDir := func(name string) string {
		dst := filepath.Join(e.tc.Dir(), name)
		if err := os.CopyFS(dst, os.DirFS(base)); err != nil {
			t.Fatalf("cannot copy %s: %s", base, err)
		}
		return dst
	}
	hashBefore := hashMonthDirs(t, base)

	intervals := make(map[time.Duration]bool)
	for m := range hashBefore {
		intervals[e.intervalFor(m)] = true
	}
	// run starts the storage on dir, force merges it and returns the export.
	run := func(dir, binary string, env []string, dedup time.Duration) map[string][]dsPoint {
		e.dataPath = dir
		e.storage = e.tc.MustStartVmstorageWithBinary("vmstorage", binary, env, e.storageFlags(fmt.Sprintf("-dedup.minScrapeInterval=%ds", int(dedup.Seconds()))))
		defer e.tc.StopApp("vmstorage")
		e.forceMerge()
		return e.exportRetry()
	}
	oracle := make(map[time.Duration]map[string][]dsPoint)
	for interval := range intervals {
		if interval > dsStepMsecs*time.Millisecond {
			oracle[interval] = run(copyDir(fmt.Sprintf("stock-%ds", int(interval.Seconds()))), e.stock, nil, interval)
		}
	}
	forkDir := copyDir("fork")
	got := run(forkDir, e.fork, []string{"VM_FORK_DOWNSAMPLING_PERIOD=" + period}, 15*time.Second)
	hashAfter := hashMonthDirs(t, forkDir)

	thinned := 0
	for m := range hashBefore {
		interval := e.intervalFor(m)
		want := raw
		if interval > dsStepMsecs*time.Millisecond {
			want = oracle[interval]
			thinned++
		} else if hashBefore[m] != hashAfter[m] {
			t.Fatalf("month %s is not eligible, but its part files changed", m)
		}
		if msg := diffMonth(m, got, want); msg != "" {
			t.Fatalf("month %s (interval %s) differs from the stock oracle: %s", m, interval, msg)
		}
	}
	if thinned == 0 {
		t.Fatalf("no month was thinned")
	}
	t.Logf("compared %d months, %d of them thinned, intervals %v", len(hashBefore), thinned, intervals)
}

// exportRetry exports until vmselect is connected to the freshly started vmstorage.
func (e *dsEnv) exportRetry() map[string][]dsPoint {
	e.t.Helper()
	for range 100 {
		res := e.export()
		if _, failed := res["error"]; !failed && len(res) > 0 {
			return res
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.t.Fatalf("cannot export")
	return nil
}

// hashMonthDirs returns a hash of the part files of every monthly partition of the data dir. Part dir names and
// parts.json are left out: a merge which keeps the data writes a part with a new name, but the same bytes.
func hashMonthDirs(t *testing.T, dataPath string) map[string]string {
	t.Helper()
	res := make(map[string]string)
	for _, kind := range []string{"small", "big"} {
		root := filepath.Join(dataPath, "data", kind)
		months, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("cannot read %s: %s", root, err)
		}
		for _, m := range months {
			if _, err := time.Parse("2006_01", m.Name()); err != nil {
				continue
			}
			parts, err := os.ReadDir(filepath.Join(root, m.Name()))
			if err != nil {
				t.Fatalf("cannot read %s: %s", m.Name(), err)
			}
			var hashes []string
			for _, p := range parts {
				if !p.IsDir() {
					continue
				}
				h := sha256.New()
				files, err := os.ReadDir(filepath.Join(root, m.Name(), p.Name()))
				if err != nil {
					t.Fatalf("cannot read %s: %s", p.Name(), err)
				}
				for _, f := range files {
					data, err := os.ReadFile(filepath.Join(root, m.Name(), p.Name(), f.Name()))
					if err != nil {
						t.Fatalf("cannot read %s: %s", f.Name(), err)
					}
					fmt.Fprintf(h, "%s %d\n", f.Name(), len(data))
					h.Write(data)
				}
				hashes = append(hashes, fmt.Sprintf("%x", h.Sum(nil)))
			}
			slices.Sort(hashes)
			res[m.Name()] += kind + ":" + strings.Join(hashes, ",") + ";"
		}
	}
	return res
}

// diffMonth compares the samples of the month in two exports and describes the first difference.
func diffMonth(month string, got, want map[string][]dsPoint) string {
	inMonth := func(a []dsPoint) []dsPoint {
		var res []dsPoint
		for _, p := range a {
			if monthKey(time.UnixMilli(p.Ts)) == month {
				res = append(res, p)
			}
		}
		return res
	}
	for series, w := range want {
		g, w := inMonth(got[series]), inMonth(w)
		if len(g) != len(w) {
			return fmt.Sprintf("%s has %d samples; want %d", series, len(g), len(w))
		}
		for i := range w {
			if g[i] != w[i] {
				return fmt.Sprintf("%s sample %d is %+v; want %+v", series, i, g[i], w[i])
			}
		}
	}
	for series, g := range got {
		if _, ok := want[series]; !ok && len(inMonth(g)) > 0 {
			return fmt.Sprintf("unexpected series %s", series)
		}
	}
	return ""
}
