package ffmpegrunner

import "sync"

// profileTotals is the shared summation surface embedded by both
// aggregate shapes. JSON marshaling flattens embedded structs, so
// consumers see a single flat object.
type profileTotals struct {
	TotalSpawnMS       int64 `json:"total_spawn_ms"`
	TotalFirstOutputMS int64 `json:"total_first_output_ms"`
	TotalProcessingMS  int64 `json:"total_processing_ms"`
	TotalExitWaitMS    int64 `json:"total_exit_wait_ms"`
	TotalWallMS        int64 `json:"total_wall_ms"`
	TotalUserCPUMs     int64 `json:"total_user_cpu_ms"`
	TotalSystemCPUMs   int64 `json:"total_system_cpu_ms"`
	PeakRSSBytes       int64 `json:"peak_rss_bytes"`
	TotalReadBytes     int64 `json:"total_read_bytes"`
	TotalWriteBytes    int64 `json:"total_write_bytes"`
}

// OperationAggregate is the per-operation-type slice of a ProfileAggregate
// (e.g. all compose passes of an attempt, separately from audio_mix and
// encode).
type OperationAggregate struct {
	ProcessCount int `json:"process_count"`
	profileTotals
}

// ProfileAggregate answers the batching question at a glance: across N
// ffmpeg processes, how much wall time went into spawn/setup
// (total_spawn_ms + total_first_output_ms) versus actual work
// (total_processing_ms). Per-operation slices isolate the phase that
// dominates. JSON-safe for telemetry; contains no paths or tokens.
type ProfileAggregate struct {
	ProcessCount  int                           `json:"process_count"`
	profileTotals                               // flattened
	Operations    map[string]OperationAggregate `json:"operations,omitempty"`
}

// addProfile folds one FFmpegResult into the totals. Results are counted
// even when their phase trio is zero (silent processes still contribute
// wall/CPU/RSS/I/O).
func addProfile(dst *profileTotals, p FFmpegResult) {
	dst.TotalSpawnMS += p.ProcessSpawnMS
	dst.TotalFirstOutputMS += p.FirstOutputMS
	dst.TotalProcessingMS += p.ProcessingMS
	dst.TotalExitWaitMS += p.ExitWaitMS
	dst.TotalWallMS += p.ProcessWallMS
	dst.TotalUserCPUMs += p.UserCPUMs
	dst.TotalSystemCPUMs += p.SystemCPUMs
	if p.PeakRSSBytes > dst.PeakRSSBytes {
		dst.PeakRSSBytes = p.PeakRSSBytes
	}
	dst.TotalReadBytes += p.ReadBytes
	dst.TotalWriteBytes += p.WriteBytes
}

// Aggregate sums any number of profiles into one ProfileAggregate,
// grouped by operation type. A zero/empty input yields an empty
// aggregate (ProcessCount == 0) — callers should skip stamping it.
func Aggregate(profiles []FFmpegResult) ProfileAggregate {
	var out ProfileAggregate
	byOp := make(map[string]*OperationAggregate)
	for _, p := range profiles {
		out.ProcessCount++
		addProfile(&out.profileTotals, p)
		key := string(p.Operation)
		if key == "" {
			key = "unknown"
		}
		op := byOp[key]
		if op == nil {
			op = &OperationAggregate{}
			byOp[key] = op
		}
		op.ProcessCount++
		addProfile(&op.profileTotals, p)
	}
	if len(byOp) > 0 {
		out.Operations = make(map[string]OperationAggregate, len(byOp))
		for key, op := range byOp {
			out.Operations[key] = *op
		}
	}
	return out
}

// Aggregator accumulates FFmpegResults attempt-scoped. It is safe for
// concurrent use (multiple executors in one attempt add from different
// goroutines) and exposes one Aggregate() snapshot at report time.
type Aggregator struct {
	mu       sync.Mutex
	profiles []FFmpegResult
}

// NewAggregator returns an empty attempt-scoped accumulator.
func NewAggregator() *Aggregator { return &Aggregator{} }

// Add folds one process result into the aggregate. Nil-safe.
func (a *Aggregator) Add(result FFmpegResult) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.profiles = append(a.profiles, result)
}

// Aggregate returns a snapshot of the accumulated totals. Nil-safe.
func (a *Aggregator) Aggregate() ProfileAggregate {
	if a == nil {
		return ProfileAggregate{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return Aggregate(a.profiles)
}

// ProcessCount reports how many profiles were accumulated. Nil-safe.
func (a *Aggregator) ProcessCount() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.profiles)
}

// Reset clears the accumulator (attempt boundary reuse).
func (a *Aggregator) Reset() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.profiles = nil
}
