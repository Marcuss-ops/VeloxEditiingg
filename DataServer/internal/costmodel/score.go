package costmodel

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
// TemporalMode: exact match required. Relaxation lands
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
