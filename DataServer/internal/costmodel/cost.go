// Package costmodel derives a per-worker placement decision (eligible +
// lower-is-better score + structured explanation) from a
// WorkerProfile and a JobRequirements.
//
// The model consumes the four canonical fields exposed on
// executor.Descriptor (ResourceClass + TemporalMode +
// Deterministic + Cacheable) plus transient worker state (drain,
// offline, capacity). WorkerProfiles are built from heartbeat
// capabilities maps in worker_profile.go.
//
// Module boundaries: this package is duplicated in
// RemoteCodex/native/worker-agent-go/internal/costmodel. See the
// "Cost model pos: Duplicata in due" choice. Both implementations
// stay in lock-step and are verified against each other by the
// parity test in the worker-side mirror.
package costmodel

import "strings"

// ── Enums (mirror of executor package) ──────────────────────────────────────
//
// These constants shadow the canonical ones in
// velox-worker-agent/internal/executor. We re-declare them as
// distinct string types here so the DataServer-side cost model has no
// cross-module compile dependency on the worker.
//
// The cost-model compatibility matrix and Score() math below MUST
// stay aligned with status-quo worker behavior; the parity test
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
// when the requirement is empty. This Permissive-Default behavior at
// The eligibility layer preserves legacy queue routing until enqueue publishes
// per-job requirements on QueueItem/Job.
type JobRequirements struct {
	// ResourceClass: when set, Score gates by
	//   resourceClassSatisfies(w.ResourceClass, j.ResourceClass).
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

// Score applies the canonical four-field + transient-state rule
// set. The signature also returns Explanation so callers
// can surface the breakdown per the "explainable score" acceptance
// criteria without re-implementing the formula.
//
// Deterministic: identical (worker, job) inputs produce identical
// (Cost, Explanation) outputs — verified by TestScore_Reproductive.
//
// ResourceClass compatibility matrix:
//
//	job: cpu   → w ∈ {cpu, mixed, io}
//	job: mixed → w ∈ {mixed, gpu}
//	job: gpu   → w ∈ {gpu} | (mixed with degraded penalty)
//	job: io    → w ∈ {io, cpu, mixed}
//
// // TemporalMode: exact match required. Relaxation lands
// when the shard planner provides a temporal-mode
// hierarchy.
func Score(w WorkerProfile, j JobRequirements) (Cost, Explanation) {
	var exp Explanation

	// 1. Transient-state gates. These short-circuit on absence of
	// capability data so legacy workers (no `executors` field in
	// their capabilities) are still filtered through the existing
	// boolean-AND semantics.
	if w.IsDraining {
		exp.IneligibilityReason = "worker is draining"
		return Cost{Eligible: false}, exp
	}
	if w.IsOffline {
		exp.IneligibilityReason = "worker is offline"
		return Cost{Eligible: false}, exp
	}
	if w.MaxParallel > 0 && w.ActiveJobs >= w.MaxParallel {
		exp.IneligibilityReason = "worker at capacity"
		return Cost{Eligible: false}, exp
	}

	// 2. Four-field rules. Empty requirement fields pass through
	// (preserves legacy behavior at the eligibility layer until enqueue publishes
	// per-job requirements).
	if j.ResourceClass != "" {
		ok, penalty := resourceClassSatisfies(w.ResourceClass, j.ResourceClass)
		if !ok {
			exp.IneligibilityReason = "resource_class " + string(w.ResourceClass) +
				" cannot satisfy job " + string(j.ResourceClass)
			return Cost{Eligible: false}, exp
		}
		exp.CapabilityFit = penalty
	}

	if j.TemporalMode != "" && w.TemporalMode != "" && w.TemporalMode != j.TemporalMode {
		exp.IneligibilityReason = "temporal_mode " + string(w.TemporalMode) +
			" cannot satisfy job " + string(j.TemporalMode)
		return Cost{Eligible: false}, exp
	}

	// 3. LoadFactor. Only meaningful when capacity was reported
	// (MaxParallel > 0); otherwise stays at 0 to preserve legacy
	// behavior at the registry layer (active_jobs aren't surfaced
	// there yet, see worker_profile.go bridge).
	if w.MaxParallel > 0 {
		exp.LoadFactor = float64(w.ActiveJobs) / float64(w.MaxParallel)
	}

	// 4. BandwidthFit. Penalty (NOT rejection) when the
	// job declares a MinBandwidthMbps > 0 and the worker reports a
	// positive LinkBandwidthMbps strictly less than the minimum.
	// Legacy / unreported workers (LinkBandwidthMbps == 0) are
	// treated as "unknown" bandwidth and pass through so legacy
	// queue payloads keep today's routing.
	if j.MinBandwidthMbps > 0 && w.LinkBandwidthMbps > 0 &&
		w.LinkBandwidthMbps < j.MinBandwidthMbps {
		exp.BandwidthFit = 1
	}

	score := exp.CapabilityFit + exp.LoadFactor + exp.DeterminismHit + exp.CacheableHint + exp.BandwidthFit + exp.ModeFit
	return Cost{Eligible: true, Score: score}, exp
}

// resourceClassSatisfies is the canonical compatibility check; ok
// is eligibility, penalty is the per-pair degraded-fallback cost
// component (0 if exact, 1 if degraded). Mixed-as-GPU is the only
// degraded pair — other mismatches are outright
// rejections.
func resourceClassSatisfies(w, j ResourceClass) (bool, float64) {
	if !j.Valid() {
		// Defensive: callers that publish invalid ResourceClass
		// requirements are treated as "no requirement" — the
		// permissive behavior at the eligibility layer reduces blast radius.
		j = ""
	}
	if j == "" {
		return true, 0
	}
	switch j {
	case ResourceCPU:
		if w == ResourceCPU || w == ResourceMixed || w == ResourceIO {
			return true, 0
		}
	case ResourceMixed:
		if w == ResourceMixed || w == ResourceGPU {
			return true, 0
		}
	case ResourceIO:
		if w == ResourceIO || w == ResourceCPU || w == ResourceMixed {
			return true, 0
		}
	case ResourceGPU:
		if w == ResourceGPU {
			return true, 0
		}
		if w == ResourceMixed {
			return true, 1
		}
	}
	return false, 0
}

// ── Compatibility helpers used by tests ──────────────────────────────────────

// CanonicalKey is the canonical ResourceClass × TemporalMode
// identifier used in assertions. Stable across releases so golden
// vectors stay reproducible.
func CanonicalKey(rc ResourceClass, tm TemporalMode) string {
	return strings.TrimSpace(string(rc)) + "|" + strings.TrimSpace(string(tm))
}
