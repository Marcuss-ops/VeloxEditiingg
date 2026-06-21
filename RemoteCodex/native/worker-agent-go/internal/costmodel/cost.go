// Package costmodel is the worker-side mirror of
// DataServer/internal/costmodel.
//
// This package is INTENTIONALLY a redundant duplicate of the
// master-side cost model rather than a thin alias re-export. The
// "Cost model pos: Duplicata in due" choice in PR-04 scope mandates
// deployment-independence over DRY: a change to one module's cost
// formula must NOT force a coupled release of the other module.
//
// The two implementations are verified against each other by:
//   - This package's cost_test.go (parity tests using golden vectors
//     over the canonical input set — must match DataServer/internal/
//     costmodel/cost_test.go assertions byte-for-byte).
//   - The DataServer-side cost_test.go (mirrors the same vectors).
//
// When EITHER file is changed, BOTH must be updated. The reviewer
// guard `scripts/ci/check-architecture.sh` enforces that the two
// packages stay in lock-step via a fingerprint comparison (added in
// a follow-up PR).
//
// ASPIRATIONAL: at PR-04 this package has no consumer yet. It
// exists so future worker-side scoring (self-cost reports, readiness
// publication) can land without a cross-module compile dependency.
package costmodel

import "strings"

// ── Enums (mirror of master cost model) ──────────────────────────────────────
//
// Same canonical values as DataServer/internal/costmodel. Distinct
// Go types so neither module imports the other.

type ResourceClass string

const (
	ResourceCPU   ResourceClass = "cpu"
	ResourceGPU   ResourceClass = "gpu"
	ResourceMixed ResourceClass = "mixed"
	ResourceIO    ResourceClass = "io"
)

func (r ResourceClass) Valid() bool {
	switch r {
	case ResourceCPU, ResourceGPU, ResourceMixed, ResourceIO:
		return true
	}
	return false
}

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
//
// Mirror of the master copy. Empty Requirement fields = no
// eligibility constraint at PR-04.4 (legacy queue routing). PR-04.5
// promotes Score to gate Rank.

type JobRequirements struct {
	ResourceClass ResourceClass
	TemporalMode  TemporalMode
	Deterministic bool
	Cacheable     bool
	// MinBandwidthMbps: PR-04.6-rank-side mirror. When > 0, Score()
	// assigns BandwidthFit = 1 if (w.LinkBandwidthMbps > 0 AND
	// w.LinkBandwidthMbps < j.MinBandwidthMbps). Legacy / unreported
	// workers (w == 0) are treated as "unknown" and pass through with
	// no penalty so the rank path preserves legacy routing for
	// pre-PR-04.6 queue payloads.
	MinBandwidthMbps float64
}

// DefaultRequirements returns the safe permissive default.
func DefaultRequirements() JobRequirements {
	return JobRequirements{}
}

// ── WorkerProfile ────────────────────────────────────────────────────────────
//
// Worker-side self-assessment shape. Same fields as the master-side
// copy but the field that doesn't exist on the master —
// MaxConcurrent vs MaxParallel — matches the worker-side naming
// in this module for symmetry with resource sampler (PR-3.6).

type WorkerProfile struct {
	WorkerID        string
	ResourceClass   ResourceClass
	TemporalMode    TemporalMode
	Deterministic   bool
	Cacheable       bool
	SupportsAlpha   bool
	// LinkBandwidthMbps: PR-04.6 mirror. Reported by the worker
	// through capabilities["link_bandwidth_mbps"] (per executor or
	// root). 0 means the worker has not yet published the field
	// (legacy) and Score.BandwidthFit treats it as "unknown" =
	// pass-through so pre-PR-04.6 workers are not penalized.
	LinkBandwidthMbps float64
	IsDraining        bool
	IsOffline         bool
	ActiveJobs        int
	MaxConcurrent     int
}

// ── Cost + Explanation ───────────────────────────────────────────────────────

type Explanation struct {
	CapabilityFit       float64
	LoadFactor          float64
	DeterminismHit      float64
	CacheableHint       float64
	// BandwidthFit: PR-04.6 mirror. 0 when the worker link is
	// sufficient OR unreported (legacy = unknown = pass-through);
	// 1 when w.LinkBandwidthMbps > 0 AND w.LinkBandwidthMbps <
	// j.MinBandwidthMbps (penalty, NOT rejection).
	BandwidthFit        float64
	ModeFit             float64
	IneligibilityReason string
}

type Cost struct {
	Eligible bool
	Score    float64
}

// Score is the canonical placement decision. Mirror of
// DataServer/internal/costmodel.Score; see that file for the
// authoritative doc-comment. Per "Duplicata in due", the formulas
// stay textually identical.
func Score(w WorkerProfile, j JobRequirements) (Cost, Explanation) {
	var exp Explanation

	if w.IsDraining {
		exp.IneligibilityReason = "worker is draining"
		return Cost{Eligible: false}, exp
	}
	if w.IsOffline {
		exp.IneligibilityReason = "worker is offline"
		return Cost{Eligible: false}, exp
	}
	if w.MaxConcurrent > 0 && w.ActiveJobs >= w.MaxConcurrent {
		exp.IneligibilityReason = "worker at capacity"
		return Cost{Eligible: false}, exp
	}

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

	if w.MaxConcurrent > 0 {
		exp.LoadFactor = float64(w.ActiveJobs) / float64(w.MaxConcurrent)
	}

	// 4. BandwidthFit (PR-04.6). Penalty (NOT rejection) when the
	// job declares a MinBandwidthMbps > 0 and the worker reports
	// a positive LinkBandwidthMbps strictly less than the minimum.
	// Legacy / unreported workers (LinkBandwidthMbps == 0) are
	// treated as "unknown" bandwidth and pass through so pre-04.6
	// queue payloads keep today's routing.
	if j.MinBandwidthMbps > 0 && w.LinkBandwidthMbps > 0 &&
		w.LinkBandwidthMbps < j.MinBandwidthMbps {
		exp.BandwidthFit = 1
	}

	score := exp.CapabilityFit + exp.LoadFactor + exp.DeterminismHit + exp.CacheableHint + exp.BandwidthFit + exp.ModeFit
	return Cost{Eligible: true, Score: score}, exp
}

// resourceClassSatisfies is the canonical compatibility check —
// textually identical to the master-side copy. Do NOT modify one
// without the other.
func resourceClassSatisfies(w, j ResourceClass) (bool, float64) {
	if !j.Valid() {
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

// ── Compatibility helpers ────────────────────────────────────────────────────

// CanonicalKey mirrors the master-side identifier for golden vector
// assertions. Stable across releases.
func CanonicalKey(rc ResourceClass, tm TemporalMode) string {
	return strings.TrimSpace(string(rc)) + "|" + strings.TrimSpace(string(tm))
}
