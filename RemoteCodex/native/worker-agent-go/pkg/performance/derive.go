package performance

import "velox-shared/telemetry"

// derive.go owns the SINGLE DerivedMetricsCalculator for the receipt.
//
// Separation of RAW and DERIVED (the only rule here):
//
//	producer ──► RAW facts ──► Derive(raw) ──► DerivedMetrics
//
// A producer NEVER writes a ratio (no `metrics["read_amplification"] = …`).
// Producers only observe RAW facts (bytes read, bytes written, wall ms,
// exclusive phase durations, process counts). Every derived KPI on the
// receipt — accounted_ratio, read/write amplification, processes_per_clip,
// useful_work_ratio, cpu_wall_ratio — is computed HERE, from this one
// function, so Prometheus projections, benchmark tools and the receipt can
// never disagree about what a ratio means.
//
// Namespace note: DeriveIO (assembler.go) is NOT a derived KPI — it maps
// raw I/O telemetry into the RAW IOMetrics section of the receipt. Derive
// is the only function that produces DerivedMetrics. Keep them distinct:
// DeriveIO → RAW, Derive → DERIVED.

// RawMetrics is the raw, observed input to Derive. Every field is a
// measured fact (Fact Owners: attempt_telemetry for clocks/CPU and
// process-tree byte totals, muxer/publisher for the final output bytes,
// process_runner for process counts, render_plan for clip count) — never
// a computed ratio.
type RawMetrics struct {
	// WallMs is the authoritative wall clock of the attempt (the master
	// accounting clock; typically TimingMetrics.WallMs).
	WallMs int64

	// Phases carries the timed phase observations of the attempt (the
	// receipt's Phases section, each row stamped with its catalog timing
	// role). Derive implements the catalog accounted_ratio_rule in one
	// place:
	//
	//	"sum only duration events with timing_mode=exclusive; never sum
	//	 span_parent or span_child"
	//
	// It sums ONLY the rows whose TimingMode is TimingExclusive — span
	// parents, span children and unclassified rows are never summed, so
	// parallel work can never double-count against the wall clock.
	Phases []PhaseTiming

	// CPUWallMS is the accumulated CPU time across all cores observed
	// for the attempt (cgroup v2 / process-tree CPU, owner
	// attempt_telemetry). It is a RAW fact — cpu_wall_ratio is derived
	// from it here.
	CPUWallMS int64

	// TotalBytesRead / TotalBytesWritten are the /proc/<pid>/io rchar /
	// wchar totals over the whole engine process tree (owner
	// attempt_telemetry). They are the amplification numerators.
	TotalBytesRead    int64
	TotalBytesWritten int64

	// OutputBytes is the final artifact size (owner muxer/publisher) —
	// the amplification denominator.
	OutputBytes int64

	// ExternalProcessCount is the engine-spawned external tool process
	// count (owner process_runner); ClipCount is the render-plan-owned
	// clip inventory (owner render_plan). Their ratio is derived here.
	ExternalProcessCount int64
	ClipCount            int

	// UsefulPipelineMS is the caller-known useful pipeline work time (a
	// directional observation, not an exact split — see
	// DerivedMetrics.UsefulWorkRatio). Zero means "not measured", never a
	// measured zero, and yields a zero ratio.
	UsefulPipelineMS int64
}

// Derive is the single DerivedMetricsCalculator: it computes every ratio
// KPI on the receipt from the raw observed facts. It never invents input
// (a zero raw value yields a zero derived value) and every formula lives
// here — no other package recomputes these ratios.
func Derive(raw RawMetrics) DerivedMetrics {
	d := DerivedMetrics{}

	// accounted_ratio (catalog accounted_ratio_rule): the sum of the
	// EXCLUSIVE top-level phase durations is the "explained" budget;
	// wall − explained is the unaccounted remainder. Span parents, span
	// children and unclassified rows are never summed — the rule is
	// enforced here, not trusted to the caller.
	var accountedMS int64
	for _, phase := range raw.Phases {
		if phase.TimingMode != telemetry.TimingExclusive {
			continue
		}
		accountedMS += phase.DurationMS
	}
	d.UnaccountedMS = raw.WallMs - accountedMS
	if raw.WallMs > 0 {
		d.AccountedRatio = float64(accountedMS) / float64(raw.WallMs)
		d.UsefulWorkRatio = ratioOf(raw.UsefulPipelineMS, raw.WallMs)
		d.CPUWallRatio = ratioOf(raw.CPUWallMS, raw.WallMs)
	}

	// read/write amplification: total process-tree bytes ÷ final output
	// bytes. copy-only Phase-1 targets write_amplification close to 1.x.
	if raw.OutputBytes > 0 {
		d.ReadAmplification = float64(raw.TotalBytesRead) / float64(raw.OutputBytes)
		d.WriteAmplification = float64(raw.TotalBytesWritten) / float64(raw.OutputBytes)
	}

	// processes_per_clip: external tool processes per render-plan clip.
	// Phase-1 copy-only target: 0.
	if raw.ClipCount > 0 {
		d.ProcessesPerClip = float64(raw.ExternalProcessCount) / float64(raw.ClipCount)
	}

	return d
}

// ratioOf is the shared zero-safe division used by every ratio KPI: a
// zero denominator (or a zero numerator) yields 0, never +Inf or NaN, so
// a partially-collected receipt never emits nonsense ratios.
func ratioOf(numerator, denominator int64) float64 {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}
