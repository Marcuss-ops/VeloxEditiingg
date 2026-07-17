package costmodel

import "strings"

// ── Enums (mirror of executor package) ──────────────────────────────────────
//
// These constants shadow the canonical ones in
// velox-worker-agent/internal/executor. We re-declare them as
// distinct string types here so the DataServer-side cost model has no
// cross-module compile dependency on the worker.
//
// The cost-model compatibility matrix and Score() math in score.go
// MUST stay aligned with status-quo worker behavior; the parity test
// (worker-agent-go/internal/costmodel/cost_test.go) enforces this.

// ResourceClass identifies the dominant kind of resource a task
// type uses. Higher capability adoption gives higher priority in
// mergeExecutorsInto so a multi-executor worker advertises its
// best ResourceClass.
type ResourceClass string

const (
	ResourceCPU   ResourceClass = "cpu"
	ResourceGPU   ResourceClass = "gpu"
	ResourceMixed ResourceClass = "mixed"
	ResourceIO    ResourceClass = "io"
)

// Valid mirrors executor.ResourceClass.Valid.
func (r ResourceClass) Valid() bool {
	switch r {
	case ResourceCPU, ResourceGPU, ResourceMixed, ResourceIO:
		return true
	}
	return false
}

// TemporalMode describes how an executor moves through time.
type TemporalMode string

const (
	TemporalFrameLocal TemporalMode = "frame_local"
	TemporalWindowed   TemporalMode = "windowed"
	TemporalStateful   TemporalMode = "stateful"
	TemporalGlobal     TemporalMode = "global"
)

func (t TemporalMode) Valid() bool {
	switch t {
	case TemporalFrameLocal, TemporalWindowed, TemporalStateful, TemporalGlobal:
		return true
	}
	return false
}

// ── JobRequirements ──────────────────────────────────────────────────────────

// JobRequirements describes what a job needs from a worker. Empty
// fields (ResourceClass == "" / TemporalMode == "") mean "no
// requirement" — Score() does not gate eligibility on those fields
// when the requirement is empty. This permissive-default behavior at
// the eligibility layer preserves legacy queue routing until enqueue publishes
// per-job requirements on QueueItem/Job.
type JobRequirements struct {
	// ResourceClass: when set, Score gates by
	// resourceClassSatisfies(w.ResourceClass, j.ResourceClass).
	ResourceClass ResourceClass
	// TemporalMode: when set, Score requires exact match against w.TemporalMode.
	TemporalMode TemporalMode
	// Deterministic is currently informational at eligibility time.
	Deterministic bool
	// Cacheable is currently surfaced as score explanation metadata.
	Cacheable bool
	// MinBandwidthMbps applies a score penalty when the worker reports a
	// positive bandwidth lower than the requested minimum.
	MinBandwidthMbps float64
}

// DefaultRequirements returns the safe, permissive default used at
// the eligibility layer until enqueue carries per-job requirements.
// Empty ResourceClass + empty TemporalMode ⇒ no field-based filter;
// only drain + offline + capacity remain.
func DefaultRequirements() JobRequirements {
	return JobRequirements{}
}

// ── Cost + Explanation ───────────────────────────────────────────────────────

// Explanation is the structured breakdown for scored placement
// ("Return a structured explanation for the winning placement").
// Lower is better for every numeric component; Score is their sum
// when Eligible=true. IneligibilityReason is empty iff Eligible=true.
//
// Components are dimensionless cost weights (not seconds / bytes);
// calibration is the responsibility of the per-executor estimator
// and is intentionally out of scope here.
type Explanation struct {
	// CapabilityFit: 0 for exact match, 1 for degraded fallback
	// (e.g. job=ResourceGPU on a ResourceMixed worker). Unset
	// (<0) when no ResourceClass requirement was published.
	CapabilityFit float64
	// LoadFactor: ActiveJobs / max(MaxParallel, 1). Measured at the
	// eligibility layer pulls 0+0 (MaxParallel == 0 ⇒ unknown).
	LoadFactor float64
	// DeterminismHit: reserved for rank scoring.
	DeterminismHit float64
	// CacheableHint: reserved for rank scoring (cache-hit bonus).
	CacheableHint float64
	// BandwidthFit: 0 when the worker link is sufficient OR
	// unreported (legacy = unknown = pass-through); 1 when
	// w.LinkBandwidthMbps > 0 AND w.LinkBandwidthMbps <
	// j.MinBandwidthMbps (penalty, NOT rejection — preserves the
	// "tolerable but penalized" convention set by CapabilityFit).
	BandwidthFit float64
	// ModeFit: 0 for exact match, 1 for fallback (reserved).
	// is strict so ModeFit stays at 0 when eligible.
	ModeFit float64
	// IneligibilityReason: human-readable explanation when
	// Cost.Eligible == false. Stable across releases so callers can
	// log without churn.
	IneligibilityReason string
}

// Cost is the canonical placement decision for one
// (WorkerProfile, JobRequirements) pair. When Eligible=false,
// Score is zero and Explanation.IneligibilityReason carries the
// reason. When Eligible=true, Score is comparable across workers
// for the SAME JobRequirements (lower is better).
type Cost struct {
	Eligible bool
	Score    float64
}

// ── Compatibility helpers used by tests ──────────────────────────────────────

// CanonicalKey is the canonical ResourceClass × TemporalMode
// identifier used in assertions. Stable across releases so golden
// vectors stay reproducible.
func CanonicalKey(rc ResourceClass, tm TemporalMode) string {
	return strings.TrimSpace(string(rc)) + "|" + strings.TrimSpace(string(tm))
}
