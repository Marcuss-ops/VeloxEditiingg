package performance

// compare_runs.go owns CompareBenchmarkRuns (plan §22): the delta
// report between a BASELINE BenchmarkRun and a CANDIDATE run of the
// SAME fixture. Every big media-engine intervention ends with this
// report — wall p50/p95, external execve count, read/write
// amplification and audio encode passes, each as
// baseline → candidate → delta%.
//
// Only same-fixture, same-cache-mode runs are comparable; anything
// else is a caller error (a copy-only run can never be compared to a
// composite one, and a cold-cache run measures a different thing than a
// warm-cache run). Wall percentiles come from the run summaries (the
// aggregation already owned by the runner); execve / audio-encode are
// deterministic architecture counters, aggregated as the MAX over
// successful observations; read amplification is a ratio and uses the
// p50, mirroring the write-amplification summary.
//
// Delta direction: for every metric here LOWER IS BETTER, so
// MetricPoint.Improved is true when candidate < baseline. When the
// baseline is 0 the delta percent is undefined and reported as a label
// ("from zero") instead of an infinite number.

import (
	"fmt"
	"strings"
)

// MetricPoint is one baseline → candidate → delta comparison. All
// metrics in this report are lower-is-better (wall time, process
// counts, byte amplification, encode passes).
type MetricPoint struct {
	Baseline     float64  `json:"baseline"`
	Candidate    float64  `json:"candidate"`
	DeltaPercent *float64 `json:"delta_percent,omitempty"` // nil when baseline is 0
	DeltaLabel   string   `json:"delta_label,omitempty"`   // e.g. "n/a (baseline 0)"
	Improved     bool     `json:"improved"`                // candidate < baseline
}

// KPIComparison carries the KPIs of the §22 report.
type KPIComparison struct {
	WallP50            MetricPoint `json:"wall_p50_ms"`
	WallP95            MetricPoint `json:"wall_p95_ms"`
	ExecveCount        MetricPoint `json:"execve_count"`
	ReadAmplification  MetricPoint `json:"read_amplification"`
	WriteAmplification MetricPoint `json:"write_amplification"`
	AudioEncodePasses  MetricPoint `json:"audio_encode_passes"`
	// Output durability p95s (publishAtomic): fsync of the partial
	// output file, the atomic rename, and the parent-directory fsync.
	// A p95 fsync in the low tens of ms is fine; a seconds-range p95
	// justifies durability work.
	FileFsyncMSP95      MetricPoint `json:"file_fsync_ms_p95"`
	OutputRenameMSP95   MetricPoint `json:"output_rename_ms_p95"`
	DirectoryFsyncMSP95 MetricPoint `json:"directory_fsync_ms_p95"`
}

// RunRef identifies one side of the comparison for the report header.
type RunRef struct {
	BenchmarkRunID  string    `json:"benchmark_run_id"`
	GitCommit       string    `json:"git_commit"`
	Release         string    `json:"release,omitempty"`
	EngineDigest    string    `json:"engine_digest,omitempty"`
	WorkerID        string    `json:"worker_id"`
	HostFingerprint string    `json:"host_fingerprint,omitempty"`
	CacheMode       CacheMode `json:"cache_mode"`
}

// BenchmarkComparison is the machine-readable §22 report.
type BenchmarkComparison struct {
	FixtureID BenchmarkFixtureID `json:"fixture_id"`
	Baseline  RunRef             `json:"baseline"`
	Candidate RunRef             `json:"candidate"`
	// CandidateGateFailures > 0 means the candidate violated
	// deterministic invariants: its timing numbers must NOT be trusted
	// as a performance signal (the numbers may still be reported, but
	// the runner already failed the gate).
	CandidateGateFailures int           `json:"candidate_gate_failures"`
	ArtifactSHAChanged    bool          `json:"artifact_sha_changed"` // both pinned and different
	KPIs                  KPIComparison `json:"kpis"`
	// AnyRegression is true when at least one KPI is worse. The CLI's
	// -fail-on-regression turns this into a non-zero exit for the
	// dedicated benchmark worker.
	AnyRegression bool `json:"any_regression"`
}

// CompareBenchmarkRuns produces the baseline/candidate/delta report.
// The runs must be non-nil, carry the same fixture ID and the same
// cache mode — a different fixture or cache mode is not a regression,
// it is an apples-to-oranges comparison and fails.
func CompareBenchmarkRuns(base, candidate *BenchmarkRun) (*BenchmarkComparison, error) {
	if base == nil || candidate == nil {
		return nil, fmt.Errorf("compare benchmark runs: baseline and candidate are required")
	}
	if base.FixtureID != candidate.FixtureID {
		return nil, fmt.Errorf("compare benchmark runs: fixture mismatch (%s vs %s) — only same-fixture runs are comparable",
			base.FixtureID, candidate.FixtureID)
	}
	if base.CacheMode != candidate.CacheMode {
		return nil, fmt.Errorf("compare benchmark runs: cache mode mismatch (%s vs %s) — cold and warm runs measure different things",
			base.CacheMode, candidate.CacheMode)
	}

	c := &BenchmarkComparison{
		FixtureID: base.FixtureID,
		Baseline:  runRef(base),
		Candidate: runRef(candidate),
		KPIs: KPIComparison{
			WallP50:             lowerIsBetter(base.Summary.WallMSP50, candidate.Summary.WallMSP50),
			WallP95:             lowerIsBetter(base.Summary.WallMSP95, candidate.Summary.WallMSP95),
			ExecveCount:         lowerIsBetter(maxInvariantMetric(base.Receipts, externalExecveOf), maxInvariantMetric(candidate.Receipts, externalExecveOf)),
			ReadAmplification:   lowerIsBetter(p50RatioMetric(base.Receipts, readAmpOf), p50RatioMetric(candidate.Receipts, readAmpOf)),
			WriteAmplification:  lowerIsBetter(base.Summary.WriteAmplificationP50, candidate.Summary.WriteAmplificationP50),
			AudioEncodePasses:   lowerIsBetter(maxInvariantMetric(base.Receipts, audioEncodePassesOf), maxInvariantMetric(candidate.Receipts, audioEncodePassesOf)),
			FileFsyncMSP95:      lowerIsBetter(base.Summary.FileFsyncMSP95, candidate.Summary.FileFsyncMSP95),
			OutputRenameMSP95:   lowerIsBetter(base.Summary.OutputRenameMSP95, candidate.Summary.OutputRenameMSP95),
			DirectoryFsyncMSP95: lowerIsBetter(base.Summary.DirectoryFsyncMSP95, candidate.Summary.DirectoryFsyncMSP95),
		},
	}
	c.CandidateGateFailures = candidate.Summary.GateFailures
	c.ArtifactSHAChanged = base.ArtifactSHA256 != "" && candidate.ArtifactSHA256 != "" &&
		base.ArtifactSHA256 != candidate.ArtifactSHA256
	c.AnyRegression = c.KPIs.WallP50.Regressed() || c.KPIs.WallP95.Regressed() ||
		c.KPIs.ExecveCount.Regressed() || c.KPIs.ReadAmplification.Regressed() ||
		c.KPIs.WriteAmplification.Regressed() || c.KPIs.AudioEncodePasses.Regressed() ||
		c.KPIs.FileFsyncMSP95.Regressed() || c.KPIs.OutputRenameMSP95.Regressed() ||
		c.KPIs.DirectoryFsyncMSP95.Regressed()
	return c, nil
}

// Regressed reports whether this metric got strictly worse.
func (m MetricPoint) Regressed() bool {
	return !m.Improved && m.Candidate > m.Baseline
}

// runRef extracts the identity side of a run.
func runRef(r *BenchmarkRun) RunRef {
	return RunRef{
		BenchmarkRunID:  r.BenchmarkRunID,
		GitCommit:       r.GitCommit,
		Release:         r.Release,
		EngineDigest:    r.EngineDigest,
		WorkerID:        r.WorkerID,
		HostFingerprint: r.HostFingerprint,
		CacheMode:       r.CacheMode,
	}
}

// lowerIsBetter computes the delta for a lower-is-better metric.
func lowerIsBetter(baseline, candidate float64) MetricPoint {
	p := MetricPoint{Baseline: baseline, Candidate: candidate, Improved: candidate < baseline}
	if baseline != 0 {
		delta := (candidate - baseline) / baseline * 100
		p.DeltaPercent = &delta
	} else {
		p.DeltaLabel = "n/a (baseline 0)"
	}
	return p
}

// maxInvariantMetric returns the MAX of a deterministic invariant KPI
// (external execve, audio encode passes) over successful observations:
// an invariant must hold in EVERY run, so any run that violated it must
// dominate the aggregate.
func maxInvariantMetric(obs []BenchmarkRunObservation, extract func(*PerformanceReceiptV1) float64) float64 {
	var max float64
	for _, o := range obs {
		if o.Error != "" || o.Receipt == nil {
			continue
		}
		if v := extract(o.Receipt); v > max {
			max = v
		}
	}
	return max
}

// externalExecveOf extracts the total external process count.
func externalExecveOf(r *PerformanceReceiptV1) float64 {
	return float64(r.Process.ExternalProcessCount)
}

// audioEncodePassesOf extracts the audio encode passes count.
func audioEncodePassesOf(r *PerformanceReceiptV1) float64 { return float64(r.Media.EncodePasses) }

// readAmpOf extracts the read-amplification KPI from a receipt.
func readAmpOf(r *PerformanceReceiptV1) float64 { return r.Derived.ReadAmplification }

// p50RatioMetric returns the nearest-rank p50 of a ratio KPI over
// successful observations (mirrors the runner's summary aggregation).
func p50RatioMetric(obs []BenchmarkRunObservation, extract func(*PerformanceReceiptV1) float64) float64 {
	var sample []float64
	for _, o := range obs {
		if o.Error != "" || o.Receipt == nil {
			continue
		}
		sample = append(sample, extract(o.Receipt))
	}
	return percentile(sample, 0.50)
}

// FormatTable renders the §22 BASELINE/CANDIDATE/DELTA table.
func (c *BenchmarkComparison) FormatTable() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n\n", c.FixtureID)
	fmt.Fprintf(&b, "BASELINE\n")
	fmt.Fprintf(&b, "  commit          %s\n", c.Baseline.GitCommit)
	fmt.Fprintf(&b, "  run             %s\n", c.Baseline.BenchmarkRunID)
	fmt.Fprintf(&b, "  worker          %s\n", c.Baseline.WorkerID)
	fmt.Fprintf(&b, "\nCANDIDATE\n")
	fmt.Fprintf(&b, "  commit          %s\n", c.Candidate.GitCommit)
	fmt.Fprintf(&b, "  run             %s\n", c.Candidate.BenchmarkRunID)
	fmt.Fprintf(&b, "  worker          %s\n", c.Candidate.WorkerID)
	fmt.Fprintf(&b, "\nMETRIC                    BASELINE    CANDIDATE    DELTA\n")
	row := func(name string, m MetricPoint) {
		fmt.Fprintf(&b, "  %-22s %10s %11s %9s\n", name,
			formatValue(m.Baseline), formatValue(m.Candidate), formatDelta(m))
	}
	row("wall p50 (ms)", c.KPIs.WallP50)
	row("wall p95 (ms)", c.KPIs.WallP95)
	row("external execve", c.KPIs.ExecveCount)
	row("read amplification", c.KPIs.ReadAmplification)
	row("write amplification", c.KPIs.WriteAmplification)
	row("audio encode passes", c.KPIs.AudioEncodePasses)
	row("fsync p95 (ms)", c.KPIs.FileFsyncMSP95)
	row("rename p95 (ms)", c.KPIs.OutputRenameMSP95)
	row("dir-fsync p95 (ms)", c.KPIs.DirectoryFsyncMSP95)
	if c.CandidateGateFailures > 0 {
		fmt.Fprintf(&b, "\nWARNING: candidate has %d gate failure(s) — timing numbers are NOT trustworthy (invariants violated)\n", c.CandidateGateFailures)
	}
	if c.ArtifactSHAChanged {
		fmt.Fprintf(&b, "WARNING: artifact SHA differs between runs — nondeterministic artifact\n")
	}
	if c.AnyRegression {
		fmt.Fprintf(&b, "\nVERDICT: REGRESSION — at least one KPI is worse\n")
	} else {
		fmt.Fprintf(&b, "\nVERDICT: no regression\n")
	}
	return b.String()
}

func formatValue(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.2f", v)
}

func formatDelta(m MetricPoint) string {
	if m.DeltaPercent != nil {
		return fmt.Sprintf("%+.1f%%", *m.DeltaPercent)
	}
	return m.DeltaLabel
}
