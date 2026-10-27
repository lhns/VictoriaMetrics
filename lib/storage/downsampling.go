package storage

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

// Time-based downsampling of whole monthly partitions. This is a fork-only feature.
//
// The rules are read from the VM_FORK_DOWNSAMPLING_PERIOD env var (not a flag, so stock binaries ignore it)
// in the form offset:interval[,offset:interval], for example 90d:1m,1y:5m.
// A partition is thinned to the interval of the largest rule whose offset has passed since the partition end.
// Thinning reuses the dedup machinery: the last sample in every interval-aligned bucket is kept.
// Nothing new is stored on disk: partHeader.MinDedupInterval records the applied interval,
// so stock reads the data as is and the fork can resume after a switch back.
const (
	downsamplingPeriodEnvVar = "VM_FORK_DOWNSAMPLING_PERIOD"
	downsamplingDryRunEnvVar = "VM_FORK_DOWNSAMPLING_DRYRUN"

	// Thinning younger partitions is allowed, but late writes and replays then cause avoidable rewrites of the month.
	minRecommendedDownsamplingOffset = 14 * 24 * time.Hour
)

type downsamplingRule struct {
	offsetMsecs   int64
	intervalMsecs int64
}

var (
	downsamplingRules  []downsamplingRule
	downsamplingDryRun bool
)

// InitDownsampling reads and validates VM_FORK_DOWNSAMPLING_PERIOD and VM_FORK_DOWNSAMPLING_DRYRUN.
//
// It must be called after SetDedupInterval and before initializing the storage.
func InitDownsampling() {
	downsamplingDryRun = os.Getenv(downsamplingDryRunEnvVar) == "1"
	rules, err := parseDownsamplingRules(os.Getenv(downsamplingPeriodEnvVar), GetDedupInterval())
	if err != nil {
		logger.Fatalf("invalid %s: %s", downsamplingPeriodEnvVar, err)
	}
	downsamplingRules = rules
	for _, r := range rules {
		if time.Duration(r.offsetMsecs)*time.Millisecond < minRecommendedDownsamplingOffset {
			logger.Warnf("%s: offset %s is smaller than %s; late writes into the thinned months will cause month rewrites",
				downsamplingPeriodEnvVar, msecsToDuration(r.offsetMsecs), minRecommendedDownsamplingOffset)
		}
	}
	if len(rules) > 0 {
		logger.Infof("downsampling rules from %s: %s; dryRun=%v", downsamplingPeriodEnvVar, formatDownsamplingRules(rules), downsamplingDryRun)
	}
}

// DownsamplingRules returns the configured (offset, interval) pairs in milliseconds for metrics.
func DownsamplingRules() [][2]int64 {
	var a [][2]int64
	for _, r := range downsamplingRules {
		a = append(a, [2]int64{r.offsetMsecs, r.intervalMsecs})
	}
	return a
}

// IsDownsamplingDryRun returns true if downsampling decisions are only logged.
func IsDownsamplingDryRun() bool {
	return downsamplingDryRun
}

func parseDownsamplingRules(s string, dedupInterval int64) ([]downsamplingRule, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if dedupInterval <= 0 {
		return nil, fmt.Errorf("-dedup.minScrapeInterval must be positive when downsampling rules are set")
	}
	var rules []downsamplingRule
	for _, item := range strings.Split(s, ",") {
		offsetStr, intervalStr, ok := strings.Cut(strings.TrimSpace(item), ":")
		if !ok {
			return nil, fmt.Errorf("missing ':' in %q; expecting offset:interval", item)
		}
		offset, err := timeutil.ParseDuration(offsetStr)
		if err != nil {
			return nil, fmt.Errorf("cannot parse offset in %q: %w", item, err)
		}
		interval, err := timeutil.ParseDuration(intervalStr)
		if err != nil {
			return nil, fmt.Errorf("cannot parse interval in %q: %w", item, err)
		}
		r := downsamplingRule{
			offsetMsecs:   offset.Milliseconds(),
			intervalMsecs: interval.Milliseconds(),
		}
		if r.offsetMsecs <= 0 {
			return nil, fmt.Errorf("offset must be positive in %q", item)
		}
		if r.intervalMsecs <= 0 {
			return nil, fmt.Errorf("interval must be positive in %q", item)
		}
		if r.intervalMsecs%dedupInterval != 0 {
			return nil, fmt.Errorf("interval in %q must be a multiple of -dedup.minScrapeInterval=%dms", item, dedupInterval)
		}
		if n := len(rules); n > 0 {
			prev := rules[n-1]
			if r.offsetMsecs <= prev.offsetMsecs {
				return nil, fmt.Errorf("offsets must be strictly increasing; got %q after %s", item, msecsToDuration(prev.offsetMsecs))
			}
			if r.intervalMsecs <= prev.intervalMsecs {
				return nil, fmt.Errorf("intervals must be strictly increasing; got %q after %s", item, msecsToDuration(prev.intervalMsecs))
			}
			// Otherwise thinning in steps would differ from thinning directly to the larger interval.
			if r.intervalMsecs%prev.intervalMsecs != 0 {
				return nil, fmt.Errorf("interval in %q must be a multiple of the previous interval %s", item, msecsToDuration(prev.intervalMsecs))
			}
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func formatDownsamplingRules(rules []downsamplingRule) string {
	a := make([]string, len(rules))
	for i, r := range rules {
		a[i] = fmt.Sprintf("%s:%s", msecsToDuration(r.offsetMsecs), msecsToDuration(r.intervalMsecs))
	}
	return strings.Join(a, ",")
}

func msecsToDuration(msecs int64) time.Duration {
	return time.Duration(msecs) * time.Millisecond
}

// getDownsamplingInterval returns the largest rule interval, which applies to a partition ending at maxTimestamp at the given now.
//
// It returns 0 if no rule applies. The current and future partitions never match, since their end is in the future.
func getDownsamplingInterval(rules []downsamplingRule, maxTimestamp, now int64) int64 {
	interval := int64(0)
	for _, r := range rules {
		if maxTimestamp+r.offsetMsecs <= now {
			interval = r.intervalMsecs
		}
	}
	return interval
}

// getMergeDedupInterval returns the dedup interval for merges in pt at the given now in milliseconds.
//
// The same value must be written to partHeader.MinDedupInterval of the merged part.
func (pt *partition) getMergeDedupInterval(now int64) int64 {
	if downsamplingDryRun {
		return GetDedupInterval()
	}
	return pt.getDownsamplingTargetInterval(now)
}

// getDownsamplingTargetInterval returns the interval pt must be thinned to at the given now, ignoring dry-run.
func (pt *partition) getDownsamplingTargetInterval(now int64) int64 {
	return max(GetDedupInterval(), getDownsamplingInterval(downsamplingRules, pt.tr.MaxTimestamp, now))
}

// logDownsamplingDryRun logs what would be done for pt if dry-run was disabled.
func (pt *partition) logDownsamplingDryRun(now int64) {
	if !downsamplingDryRun {
		return
	}
	interval := pt.getDownsamplingTargetInterval(now)
	pws := pt.GetParts(nil, false)
	minInterval := getMinDedupInterval(pws)
	pt.PutParts(pws)
	logger.Infof("downsampling dry-run: partition %s would be thinned to %s; parts claim %s; scheduled=%v",
		pt.name, msecsToDuration(interval), msecsToDuration(minInterval), interval > minInterval)
}
