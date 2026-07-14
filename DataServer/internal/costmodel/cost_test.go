package costmodel

import "testing"

// ── Eligibility gates ────────────────────────────────────────────────────────

// TestScore_GPUJobExcludesCPUWorker: a
// ResourceGPU-required job MUST NOT be eligible for a ResourceCPU
// worker.
func TestScore_GPUJobExcludesCPUWorker(t *testing.T) {
	w := WorkerProfile{
		WorkerID:      "w-cpu",
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
	}
	j := JobRequirements{
		ResourceClass: ResourceGPU,
		TemporalMode:  TemporalFrameLocal,
	}
	c, exp := Score(w, j)
	if c.Eligible {
		t.Fatalf("expected ineligible (gpu required, cpu worker), got %+v exp=%+v", c, exp)
	}
	if exp.IneligibilityReason == "" {
		t.Fatalf("expected non-empty IneligibilityReason, got %+v", exp)
	}
}

// TestScore_DrainingExcluded: draining workers are
// excluded regardless of capability profile.
func TestScore_DrainingExcluded(t *testing.T) {
	w := WorkerProfile{
		WorkerID:      "w-drain",
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		IsDraining:    true,
	}
	j := DefaultRequirements()
	c, exp := Score(w, j)
	if c.Eligible {
		t.Fatalf("draining worker should be ineligible, got %+v exp=%+v", c, exp)
	}
	if exp.IneligibilityReason != "worker is draining" {
		t.Fatalf("expected reason=worker is draining, got %q", exp.IneligibilityReason)
	}
}

// TestScore_OfflineExcluded: offline workers are
// excluded regardless of capability profile.
func TestScore_OfflineExcluded(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		IsOffline:     true,
	}
	c, exp := Score(w, DefaultRequirements())
	if c.Eligible {
		t.Fatalf("offline worker should be ineligible, exp=%+v", exp)
	}
}

// TestScore_AtCapacityExcluded: capacity gate.
func TestScore_AtCapacityExcluded(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		ActiveJobs:    4,
		MaxParallel:   4,
	}
	c, exp := Score(w, DefaultRequirements())
	if c.Eligible {
		t.Fatalf("at-capacity worker should be ineligible, exp=%+v", exp)
	}
	if exp.IneligibilityReason != "worker at capacity" {
		t.Fatalf("expected `worker at capacity`, got %q", exp.IneligibilityReason)
	}
}

// TestScore_GPUJobOnMixedDegraded: ResourceMixed is a degraded fallback
// for ResourceGPU-required jobs (Model admits with penalty=1 rather
// than rejecting outright).
func TestScore_GPUJobOnMixedDegraded(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceMixed,
		TemporalMode:  TemporalFrameLocal,
	}
	j := JobRequirements{
		ResourceClass: ResourceGPU,
		TemporalMode:  TemporalFrameLocal,
	}
	c, exp := Score(w, j)
	if !c.Eligible {
		t.Fatalf("mixed worker should serve gpu job at degraded quality, exp=%+v", exp)
	}
	if exp.CapabilityFit != 1 {
		t.Fatalf("expected CapabilityFit=1 (degraded penalty), got %v", exp.CapabilityFit)
	}
}

// ── ResourceClass compatibility matrix ──────────────────────────────────────

func TestScore_CompatibilityMatrix(t *testing.T) {
	cases := []struct {
		name   string
		w, j   ResourceClass
		wantOK bool
	}{
		{"cpu job → cpu worker", ResourceCPU, ResourceCPU, true},
		{"cpu job → mixed worker", ResourceMixed, ResourceCPU, true},
		{"cpu job → io worker", ResourceIO, ResourceCPU, true},
		{"cpu job → gpu worker", ResourceGPU, ResourceCPU, false},
		{"mixed job → gpu worker", ResourceGPU, ResourceMixed, true},
		{"gpu job → gpu worker", ResourceGPU, ResourceGPU, true},
		{"gpu job → mixed (degraded)", ResourceMixed, ResourceGPU, true},
		{"gpu job → cpu (rejected)", ResourceCPU, ResourceGPU, false},
		{"io job → io worker", ResourceIO, ResourceIO, true},
		{"io job → cpu worker", ResourceCPU, ResourceIO, true},
		{"io job → gpu (rejected)", ResourceGPU, ResourceIO, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := resourceClassSatisfies(tc.w, tc.j)
			if got != tc.wantOK {
				t.Fatalf("resourceClassSatisfies(%q, %q) = %v, want %v", tc.w, tc.j, got, tc.wantOK)
			}
		})
	}
}

// ── Bridge — heartbeat capabilities → WorkerProfile ─────────────────────────

// TestBuildWorkerProfile_LegacySynthesized: legacy heartbeats that
// only carry `supported_job_types` (no `executors` array) MUST
// synthesize {cpu, frame_local, false, false}.
func TestBuildWorkerProfile_LegacySynthesized(t *testing.T) {
	caps := map[string]interface{}{
		"supported_job_types": []interface{}{"process_video"},
	}
	w := BuildWorkerProfile("legacy", true, false, "online", 0, 0, caps)
	if w.ResourceClass != ResourceCPU {
		t.Fatalf("legacy profile ResourceClass = %q, want cpu", w.ResourceClass)
	}
	if w.TemporalMode != TemporalFrameLocal {
		t.Fatalf("legacy profile TemporalMode = %q, want frame_local", w.TemporalMode)
	}
	if w.Deterministic || w.Cacheable || w.SupportsAlpha {
		t.Fatalf("legacy profile should be conservative, got %+v", w)
	}
}

// TestBuildWorkerProfile_MergeExecutors: workers surface
// multiple executors via `executors` array — the merge policy
// produces the canonical aggregate per worker_profile.go.
func TestBuildWorkerProfile_MergeExecutors(t *testing.T) {
	caps := map[string]interface{}{
		"schema_version": 2,
		"executors": []interface{}{
			map[string]interface{}{
				"id":             "scene.composite.v1",
				"version":        1,
				"resource_class": "cpu",
				"temporal_mode":  "frame_local",
				"deterministic":  true,
				"cacheable":      false,
			},
			map[string]interface{}{
				"id":             "scene.composite.v1",
				"version":        2,
				"resource_class": "gpu",
				"temporal_mode":  "global",
				"deterministic":  false,
				"cacheable":      true,
			},
		},
	}
	w := BuildWorkerProfile("w1", true, false, "online", 1, 4, caps)
	if w.ResourceClass != ResourceGPU {
		t.Fatalf("merged ResourceClass = %q, want gpu (most-permissive)", w.ResourceClass)
	}
	if w.TemporalMode != TemporalGlobal {
		t.Fatalf("merged TemporalMode = %q, want global (most-permissive)", w.TemporalMode)
	}
	if w.Deterministic {
		t.Errorf("Deterministic = true after AND; expected false (one executor reports false)")
	}
	if !w.Cacheable {
		t.Errorf("Cacheable = false after OR; expected true (one executor reports true)")
	}
}

// TestBuildWorkerProfile_NotSchedulableBecomesDraining: the legacy
// boolean-AND's "Schedulable=false" signal is treated as
// IsDraining=true so the cost model gates this case explicitly.
func TestBuildWorkerProfile_NotSchedulableBecomesDraining(t *testing.T) {
	w := BuildWorkerProfile("w", false /* schedulable */, false, "online", 0, 0, nil)
	if !w.IsDraining {
		t.Fatalf("expected IsDraining=true when schedulable=false, got %+v", w)
	}
}

// ── Determinism semantics ────────────────────────────────────────

// TestScore_DeterministicStrict_PR04_4: a non-deterministic worker is
// admitted for a Deterministic-true job — the determinism field is
// informational-only at the eligibility layer.
func TestScore_DeterministicStrict_PR04_4(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		Deterministic: false,
	}
	j := JobRequirements{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		Deterministic: true,
	}
	c, _ := Score(w, j)
	if !c.Eligible {
		t.Fatalf("non-deterministic worker should be eligible for a Deterministic-true job (informational-only at eligibility)")
	}
}

// ── Determinism of Score ────────────────────────────────────────────────────

// TestScore_Reproductive: identical inputs ⇒ identical outputs.
// This invariant is the foundation for the parity test in the
// worker-side mirror.
func TestScore_Reproductive(t *testing.T) {
	w := WorkerProfile{
		WorkerID:      "w",
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		ActiveJobs:    1,
		MaxParallel:   4,
	}
	j := JobRequirements{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
	}
	c1, e1 := Score(w, j)
	c2, e2 := Score(w, j)
	if c1 != c2 {
		t.Fatalf("non-deterministic Cost: %+v vs %+v", c1, c2)
	}
	if e1 != e2 {
		t.Fatalf("non-deterministic Explanation: %+v vs %+v", e1, e2)
	}
}

// ── Permissiveness invariant ────────────────

// TestDefaultRequirements_EmptyFieldsPreserveLegacyRouting: the
// permissive default MUST NOT filter by the four canonical fields
// when the requirement is empty. This is the load-bearing invariant
// — tightening this default silently breaks every legacy worker.
func TestDefaultRequirements_EmptyFieldsPreserveLegacyRouting(t *testing.T) {
	j := DefaultRequirements()
	if j.ResourceClass != "" {
		t.Fatalf("DefaultRequirements().ResourceClass should be empty (permissive), got %q", j.ResourceClass)
	}
	if j.TemporalMode != "" {
		t.Fatalf("DefaultRequirements().TemporalMode should be empty (permissive), got %q", j.TemporalMode)
	}

	// Legacy worker (no `executors` capability field) — synthesized
	// profile defaults to {cpu, frame_local}. Eligibility with the
	// permissive default MUST remain true so today's queue routing
	// doesn't regress.
	w := BuildWorkerProfile("legacy", true, false, "online", 0, 0, map[string]interface{}{
		"supported_job_types": []interface{}{"process_video"},
	})
	c, exp := Score(w, j)
	if !c.Eligible {
		t.Fatalf("permissive default + legacy worker should remain eligible: exp=%+v", exp)
	}
	if exp.IneligibilityReason != "" {
		t.Fatalf("expected empty IneligibilityReason, got %q", exp.IneligibilityReason)
	}
}

// TestScore_LoadFactor: scoring well-populated active jobs vs spare
// produces a strictly-increasing LoadFactor per the lower-is-better
// convention.
func TestScore_LoadFactor(t *testing.T) {
	w0 := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, ActiveJobs: 0, MaxParallel: 4}
	w3 := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, ActiveJobs: 3, MaxParallel: 4}
	j := DefaultRequirements()
	_, e0 := Score(w0, j)
	_, e3 := Score(w3, j)
	if e0.LoadFactor >= e3.LoadFactor {
		t.Fatalf("LoadFactor not monotonic with load: e0=%v e3=%v", e0.LoadFactor, e3.LoadFactor)
	}
}

// ── Bandwidth math ────────────────────────────────────────────────

// TestScore_BandwidthSufficient_PR04_6: worker link meets the
// job's MinBandwidthMbps -> BandwidthFit stays at 0, score stays
// at 0. Mirrors CapabilityFit's degraded-fallback convention for
// "tolerable" by being clean here for "sufficient".
func TestScore_BandwidthSufficient_PR04_6(t *testing.T) {
	w := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, LinkBandwidthMbps: 1000}
	j := JobRequirements{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, MinBandwidthMbps: 500}
	c, exp := Score(w, j)
	if !c.Eligible {
		t.Fatalf("sufficient bandwidth should remain eligible, exp=%+v", exp)
	}
	if exp.BandwidthFit != 0 {
		t.Fatalf("BandwidthFit=%v, want 0 (sufficient)", exp.BandwidthFit)
	}
	if c.Score != 0 {
		t.Fatalf("clean score=0 expected, got %v", c.Score)
	}
}

// TestScore_BandwidthInsufficient_PR04_6: worker link is positive
// but below the job's MinBandwidthMbps -> BandwidthFit = 1
// (penalty, NOT rejection).
func TestScore_BandwidthInsufficient_PR04_6(t *testing.T) {
	w := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, LinkBandwidthMbps: 50}
	j := JobRequirements{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, MinBandwidthMbps: 100}
	c, exp := Score(w, j)
	if !c.Eligible {
		t.Fatalf("under-bandwidth worker is tolerable (penalty, not reject), exp=%+v", exp)
	}
	if exp.BandwidthFit != 1 {
		t.Fatalf("BandwidthFit=%v, want 1 (under-bandwidth penalty)", exp.BandwidthFit)
	}
	if c.Score != 1 {
		t.Fatalf("score=%v, want 1 (penalty-only)", c.Score)
	}
}

// TestScore_BandwidthBothZero_PR04_6: legacy job (MinBandwidthMbps=0)
// + legacy worker (LinkBandwidthMbps=0) -> BandwidthFit stays at 0.
func TestScore_BandwidthBothZero_PR04_6(t *testing.T) {
	w := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}
	j := JobRequirements{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}
	c, exp := Score(w, j)
	if !c.Eligible {
		t.Fatalf("default-zero bandwidth should be eligible, exp=%+v", exp)
	}
	if exp.BandwidthFit != 0 {
		t.Fatalf("BandwidthFit=%v, want 0", exp.BandwidthFit)
	}
}

// TestScore_BandwidthLegacyWorker_PR04_6: A worker that hasn't
// published link_bandwidth_mbps (w=0) MUST pass a MinBandwidthMbps// job without penalty.
func TestScore_BandwidthLegacyWorker_PR04_6(t *testing.T) {
	w := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal /* LinkBandwidthMbps=0 */}
	j := JobRequirements{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, MinBandwidthMbps: 1000}
	c, exp := Score(w, j)
	if !c.Eligible {
		t.Fatalf("legacy worker (w.Link=0) must remain eligible for Min>0 jobs, exp=%+v", exp)
	}
	if exp.BandwidthFit != 0 {
		t.Fatalf("BandwidthFit=%v on legacy worker, want 0 (pass-through)", exp.BandwidthFit)
	}
}

// TestScore_BandwidthEqual_PR04_6: worker link exactly meets the
// minimum -> BandwidthFit = 0 (>=, not strict).
func TestScore_BandwidthEqual_PR04_6(t *testing.T) {
	w := WorkerProfile{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, LinkBandwidthMbps: 100}
	j := JobRequirements{ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal, MinBandwidthMbps: 100}
	_, exp := Score(w, j)
	if exp.BandwidthFit != 0 {
		t.Fatalf("equal link/min: BandwidthFit=%v, want 0", exp.BandwidthFit)
	}
}

// ── BuildWorkerProfile bandwidth merge ────────────────────────────

// TestBuildWorkerProfile_BandwidthMerge: per-executor
// link_bandwidth_mbps merges most-permissive (max) across the
// executors array, mirroring the `max-wins` rule applied to
// ResourceClass / TemporalMode.
func TestBuildWorkerProfile_BandwidthMerge(t *testing.T) {
	caps := map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{
				"resource_class":      "cpu",
				"temporal_mode":       "frame_local",
				"link_bandwidth_mbps": 100.0,
			},
			map[string]interface{}{
				"resource_class":      "gpu",
				"temporal_mode":       "global",
				"link_bandwidth_mbps": 5000.0,
			},
		},
	}
	w := BuildWorkerProfile("w", true, false, "online", 0, 0, caps)
	if w.LinkBandwidthMbps != 5000 {
		t.Fatalf("merged LinkBandwidthMbps=%v, want 5000 (max-most-permissive)", w.LinkBandwidthMbps)
	}
}

// TestBuildWorkerProfile_BandwidthLegacy: heartbeats that omit
// `link_bandwidth_mbps` MUST leave LinkBandwidthMbps at 0 so Score
// treats them as "unknown" = pass-through.
func TestBuildWorkerProfile_BandwidthLegacy(t *testing.T) {
	caps := map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{
				"resource_class": "cpu",
				"temporal_mode":  "frame_local",
			},
		},
	}
	w := BuildWorkerProfile("legacy", true, false, "online", 0, 0, caps)
	if w.LinkBandwidthMbps != 0 {
		t.Fatalf("legacy profile LinkBandwidthMbps=%v, want 0", w.LinkBandwidthMbps)
	}
}
