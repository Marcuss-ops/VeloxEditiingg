// Package taskgraph provides TaskSpec validation and hashing tests.
package taskgraph

import "testing"

func TestSpecValidate(t *testing.T) {
	valid := &TaskSpec{Version: 1, JobID: "j1", ExecutorID: "exec1"}
	if err := valid.Validate(); err != nil {
		t.Errorf("valid spec: %v", err)
	}

	noVersion := &TaskSpec{JobID: "j1", ExecutorID: "exec1"}
	if err := noVersion.Validate(); err == nil {
		t.Error("spec without version should fail")
	}

	noJob := &TaskSpec{Version: 1, ExecutorID: "exec1"}
	if err := noJob.Validate(); err == nil {
		t.Error("spec without job_id should fail")
	}

	noExecutor := &TaskSpec{Version: 1, JobID: "j1"}
	if err := noExecutor.Validate(); err == nil {
		t.Error("spec without executor_id should fail")
	}
}

func TestSpecHashDeterministic(t *testing.T) {
	a := &TaskSpec{Version: 1, JobID: "j1", ExecutorID: "exec1"}
	b := &TaskSpec{Version: 1, JobID: "j1", ExecutorID: "exec1"}
	ha, _ := a.SpecHash()
	hb, _ := b.SpecHash()
	if ha != hb {
		t.Errorf("identical specs should produce same hash: %s != %s", ha, hb)
	}
}

func TestSpecHashDifferent(t *testing.T) {
	a := &TaskSpec{Version: 1, JobID: "j1"}
	b := &TaskSpec{Version: 1, JobID: "j2"}
	ha, _ := a.SpecHash()
	hb, _ := b.SpecHash()
	if ha == hb {
		t.Error("different specs should produce different hashes")
	}
}
