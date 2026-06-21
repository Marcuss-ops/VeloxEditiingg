// Package costmodel — parity tests.
//
// These vectors MUST byte-for-byte match the assertions in
// DataServer/internal/costmodel/cost_test.go. When that file
// changes, update this file in lock-step. The two packages
// intentionally share neither type nor package — they only share
// the formula. Drift between the two files indicates a bug.
package costmodel

import "testing"

// ── Eligibility gate vectors (mirror of master tests) ───────────────────────

func TestScore_GPUJobExcludesCPUWorker_Mirror(t *testing.T) {
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
		t.Fatalf("mirror: expected ineligible, exp=%+v", exp)
	}
	if exp.IneligibilityReason == "" {
		t.Fatalf("mirror: expected non-empty IneligibilityReason, got %+v", exp)
	}
}

func TestScore_DrainingExcluded_Mirror(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		IsDraining:    true,
	}
	c, exp := Score(w, DefaultRequirements())
	if c.Eligible {
		t.Fatalf("mirror: draining should be ineligible, exp=%+v", exp)
	}
	if exp.IneligibilityReason != "worker is draining" {
		t.Fatalf("mirror: reason=%q", exp.IneligibilityReason)
	}
}

func TestScore_OfflineExcluded_Mirror(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		IsOffline:     true,
	}
	c, _ := Score(w, DefaultRequirements())
	if c.Eligible {
		t.Fatalf("mirror: offline should be ineligible")
	}
}

func TestScore_AtCapacityExcluded_Mirror(t *testing.T) {
	w := WorkerProfile{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		ActiveJobs:    4,
		MaxConcurrent: 4,
	}
	c, _ := Score(w, DefaultRequirements())
	if c.Eligible {
		t.Fatalf("mirror: at-capacity should be ineligible")
	}
}

func TestScore_GPUJobOnMixedDegraded_Mirror(t *testing.T) {
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
		t.Fatalf("mirror: mixed serves gpu degraded, exp=%+v", exp)
	}
	if exp.CapabilityFit != 1 {
		t.Fatalf("mirror: degraded penalty=%v, want 1", exp.CapabilityFit)
	}
}

// ── Compatibility matrix (mirror) ───────────────────────────────────────────

func TestScore_CompatibilityMatrix_Mirror(t *testing.T) {
	cases := []struct {
		name   string
		w, j   ResourceClass
		wantOK bool
	}{
		{"cpu job → cpu", ResourceCPU, ResourceCPU, true},
		{"cpu job → mixed", ResourceMixed, ResourceCPU, true},
		{"cpu job → io", ResourceIO, ResourceCPU, true},
		{"cpu job → gpu", ResourceGPU, ResourceCPU, false},
		{"gpu job → gpu", ResourceGPU, ResourceGPU, true},
		{"gpu job → mixed (degraded)", ResourceMixed, ResourceGPU, true},
		{"gpu job → cpu (rejected)", ResourceCPU, ResourceGPU, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := resourceClassSatisfies(tc.w, tc.j)
			if got != tc.wantOK {
				t.Fatalf("mirror: resourceClassSatisfies(%q, %q) = %v, want %v",
					tc.w, tc.j, got, tc.wantOK)
			}
		})
	}
}

// ── Score determinism (mirror) ──────────────────────────────────────────────

func TestScore_Reproductive_Mirror(t *testing.T) {
	w := WorkerProfile{
		WorkerID:      "w",
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		ActiveJobs:    1,
		MaxConcurrent: 4,
	}
	j := JobRequirements{
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
	}
	c1, e1 := Score(w, j)
	c2, e2 := Score(w, j)
	if c1 != c2 {
		t.Fatalf("mirror: non-deterministic Cost %+v vs %+v", c1, c2)
	}
	if e1 != e2 {
		t.Fatalf("mirror: non-deterministic Explanation %+v vs %+v", e1, e2)
	}
}
