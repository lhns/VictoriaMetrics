package tests

import (
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

func testDownsamplingSwitch(t *testing.T, rules []dsRule, basePort int) {
	fork, stock := os.Getenv(downsamplingForkBinaryEnv), os.Getenv(downsamplingStockBinaryEnv)
	if fork == "" || stock == "" {
		t.Skipf("set %s and %s to run", downsamplingForkBinaryEnv, downsamplingStockBinaryEnv)
	}
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

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
	e.startStorage(true)
	e.vminsert = tc.MustStartVminsert("vminsert", []string{
		fmt.Sprintf("-storageNode=127.0.0.1:%d", e.ports[1]),
	})
	e.vmselect = tc.MustStartVmselect("vmselect", []string{
		fmt.Sprintf("-storageNode=127.0.0.1:%d", e.ports[2]),
		"-search.maxExportDuration=100000h",
	})

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

func (e *dsEnv) storageFlags() []string {
	return []string{
		"-storageDataPath=" + e.dataPath,
		"-retentionPeriod=100y",
		dsDedupFlag,
		"-inmemoryDataFlushInterval=1s",
		fmt.Sprintf("-httpListenAddr=127.0.0.1:%d", e.ports[0]),
		fmt.Sprintf("-vminsertAddr=127.0.0.1:%d", e.ports[1]),
		fmt.Sprintf("-vmselectAddr=127.0.0.1:%d", e.ports[2]),
	}
}

func (e *dsEnv) startStorage(fork bool) {
	e.t.Helper()
	if fork {
		e.storage = e.tc.MustStartVmstorageWithBinary("vmstorage", e.fork, []string{"VM_FORK_DOWNSAMPLING_PERIOD=" + e.period}, e.storageFlags())
		return
	}
	e.storage = e.tc.MustStartVmstorageWithBinary("vmstorage", e.stock, nil, e.storageFlags())
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
			added = append(added, dsSample{key, ts})
			batch = append(batch, fmt.Sprintf("%s %d %d", key, ts/dsStepMsecs, ts))
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
func (e *dsEnv) expected(samples []dsSample) map[string][]int64 {
	return e.thin(samples, nil)
}

// expectedStock is the expected state after stock merged the backfill into thinned months: the backfill stays as is.
func (e *dsEnv) expectedStock(all, backfill []dsSample) map[string][]int64 {
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

func (e *dsEnv) thin(thinned, verbatim []dsSample) map[string][]int64 {
	type key struct{ series, month string }
	groups := make(map[key][]int64)
	for _, s := range thinned {
		k := key{s.series, monthKey(time.UnixMilli(s.ts))}
		groups[k] = append(groups[k], s.ts)
	}
	res := make(map[string][]int64)
	for k, tss := range groups {
		slices.Sort(tss)
		interval := e.intervalFor(k.month).Milliseconds()
		for i, ts := range tss {
			if i+1 < len(tss) && (tss[i+1]+interval-1)/interval == (ts+interval-1)/interval {
				continue
			}
			res[k.series] = append(res[k.series], ts)
		}
	}
	for _, s := range verbatim {
		res[s.series] = append(res[s.series], s.ts)
	}
	for _, tss := range res {
		slices.Sort(tss)
	}
	return res
}

func (e *dsEnv) export() map[string][]int64 {
	e.t.Helper()
	u := fmt.Sprintf("http://%s/select/0/prometheus/api/v1/export", e.vmselect.HTTPAddr())
	form := url.Values{}
	form.Set("match[]", `{__name__=~"ds_.*"}`)
	form.Set("start", "0")
	form.Set("end", fmt.Sprint(e.now.Add(24*time.Hour).Unix()))
	body, status := e.tc.Client().PostForm(e.t, u, form, nil)
	if status != 200 {
		return map[string][]int64{"error": {int64(status)}}
	}
	res := make(map[string][]int64)
	for line := range strings.SplitSeq(body, "\n") {
		if line == "" {
			continue
		}
		var r struct {
			Metric     map[string]string `json:"metric"`
			Timestamps []int64           `json:"timestamps"`
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			e.t.Fatalf("cannot parse export line: %s", err)
		}
		key := fmt.Sprintf(`%s{idx="%s"}`, r.Metric["__name__"], r.Metric["idx"])
		res[key] = append(res[key], r.Timestamps...)
	}
	for _, tss := range res {
		slices.Sort(tss)
	}
	return res
}

// assertExport compares the export with the expectation with retries, since vmselect reconnects after storage restarts.
func (e *dsEnv) assertExport(msg string, want map[string][]int64) {
	e.t.Helper()
	e.tc.Assert(&apptest.AssertOptions{
		Msg:     msg,
		Got:     func() any { return e.export() },
		Want:    want,
		Retries: 100,
		Period:  200 * time.Millisecond,
		FailNow: true,
	})
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
	var want map[string][]int64
	if verbatim != nil {
		want = e.expectedStock(all, verbatim)
	} else {
		want = e.expected(all)
	}
	wantRows := make(map[string]uint64)
	for _, tss := range want {
		for _, ts := range tss {
			wantRows[monthKey(time.UnixMilli(ts))]++
		}
	}
	got, strays := e.readParts(false)
	if len(strays) > 0 {
		e.t.Fatalf("%s: stray parts: %v", msg, strays)
	}
	for m, rows := range wantRows {
		if got[m].rows != rows {
			e.t.Fatalf("%s: month %s has %d rows on disk; want %d", msg, m, got[m].rows, rows)
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
