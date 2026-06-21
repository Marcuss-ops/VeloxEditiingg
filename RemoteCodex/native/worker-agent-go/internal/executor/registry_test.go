package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// ── Fakes ─────────────────────────────────────────────────────────────────────

// fakeExec lets us register arbitrary descriptors without pulling in a
// real renderer. The Descriptor field IS the executor's behaviour:
// what gets validated and returned is whatever the test puts in.
type fakeExec struct {
	desc Descriptor
}

func (f *fakeExec) Descriptor() Descriptor { return f.desc }
func (f *fakeExec) Validate(_ TaskSpec) error {
	return nil
}
func (f *fakeExec) Execute(_ context.Context, _ ExecutionContext, _ TaskSpec) (ExecutionResult, error) {
	return ExecutionResult{Status: "succeeded"}, nil
}

func newFake(id string, version int, rc ResourceClass, tm TemporalMode) *fakeExec {
	return &fakeExec{
		desc: Descriptor{
			ID:            id,
			Version:       version,
			ResourceClass: rc,
			TemporalMode:  tm,
		},
	}
}

// ── Validation table ──────────────────────────────────────────────────────────

func TestRegistry_Register_Validation(t *testing.T) {
	tests := []struct {
		name string
		desc Descriptor
	}{
		{"empty id", Descriptor{ID: "", Version: 1, ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}},
		{"whitespace id", Descriptor{ID: "   ", Version: 1, ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}},
		{"zero version", Descriptor{ID: "ok", Version: 0, ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}},
		{"negative version", Descriptor{ID: "ok", Version: -3, ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}},
		{"id with @", Descriptor{ID: "a@b", Version: 1, ResourceClass: ResourceCPU, TemporalMode: TemporalFrameLocal}},
		{"unknown resource class", Descriptor{ID: "ok", Version: 1, ResourceClass: ResourceClass("quantum"), TemporalMode: TemporalFrameLocal}},
		{"unknown temporal mode", Descriptor{ID: "ok", Version: 1, ResourceClass: ResourceCPU, TemporalMode: TemporalMode("twisted")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			err := r.Register(&fakeExec{desc: tt.desc})
			if err == nil {
				t.Fatalf("expected error for %q, got nil", tt.name)
			}
			if !errors.Is(err, ErrInvalidDescriptor) {
				t.Errorf("err = %v, want ErrInvalidDescriptor", err)
			}
			if r.Len() != 0 {
				t.Errorf("Len = %d, want 0 after rejected registration", r.Len())
			}
		})
	}
}

func TestRegistry_Register_NilExecutorOrRegistry(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(nil); !errors.Is(err, ErrInvalidDescriptor) {
		t.Errorf("nil executor: err = %v, want ErrInvalidDescriptor", err)
	}
	var nilR *Registry
	err := nilR.Register(newFake("ok", 1, ResourceCPU, TemporalFrameLocal))
	if err == nil {
		t.Errorf("nil registry: expected error, got nil")
	}
}

// ── Duplicate handling ─────────────────────────────────────────────────────────

func TestRegistry_Register_Duplicate(t *testing.T) {
	r := NewRegistry()
	a := newFake("asset.prepare.v1", 1, ResourceCPU, TemporalFrameLocal)
	if err := r.Register(a); err != nil {
		t.Fatalf("first register: %v", err)
	}
	b := newFake("asset.prepare.v1", 1, ResourceCPU, TemporalFrameLocal) // same key, different instance
	err := r.Register(b)
	if !errors.Is(err, ErrExecutorExists) {
		t.Errorf("err = %v, want ErrExecutorExists", err)
	}
	if r.Len() != 1 {
		t.Errorf("Len = %d, want 1 after rejected duplicate", r.Len())
	}
}

func TestRegistry_Register_SameIDDifferentVersion(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(newFake("scene.composite.v1", 1, ResourceCPU, TemporalFrameLocal)); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if err := r.Register(newFake("scene.composite.v1", 2, ResourceCPU, TemporalFrameLocal)); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if r.Len() != 2 {
		t.Errorf("Len = %d, want 2", r.Len())
	}
	// Both Versions must resolve independently.
	if _, err := r.Resolve("scene.composite.v1", 1); err != nil {
		t.Errorf("Resolve v1: %v", err)
	}
	if _, err := r.Resolve("scene.composite.v1", 2); err != nil {
		t.Errorf("Resolve v2: %v", err)
	}
}

// ── Order determinism ─────────────────────────────────────────────────────────

func TestRegistry_Descriptors_SortedByIDThenVersion(t *testing.T) {
	r := NewRegistry()
	// Insert deliberately out of order.
	inserts := []struct {
		id      string
		version int
	}{
		{"scene.composite.v1", 1},
		{"audio.mix.v1", 1},
		{"video.encode-h264.v1", 1},
		{"asset.prepare.v1", 1},
		{"text.compile.v1", 1},
		{"video.concat.v1", 1},
		// Same ID, later version — must come AFTER the first version of its ID.
		{"asset.prepare.v1", 2},
	}
	for _, in := range inserts {
		if err := r.Register(newFake(in.id, in.version, ResourceCPU, TemporalFrameLocal)); err != nil {
			t.Fatalf("register %s@%d: %v", in.id, in.version, err)
		}
	}
	descs := r.Descriptors()
	want := []struct {
		id      string
		version int
	}{
		{"asset.prepare.v1", 1},
		{"asset.prepare.v1", 2},
		{"audio.mix.v1", 1},
		{"scene.composite.v1", 1},
		{"text.compile.v1", 1},
		{"video.concat.v1", 1},
		{"video.encode-h264.v1", 1},
	}
	if len(descs) != len(want) {
		t.Fatalf("Descriptors len = %d, want %d", len(descs), len(want))
	}
	for i, d := range descs {
		if d.ID != want[i].id || d.Version != want[i].version {
			t.Errorf("[%d] = %s@%d, want %s@%d", i, d.ID, d.Version, want[i].id, want[i].version)
		}
	}

	// IDs() must ALSO be sorted lexicographically.
	ids := r.IDs()
	wantIDs := []string{
		"asset.prepare.v1@1",
		"asset.prepare.v1@2",
		"audio.mix.v1@1",
		"scene.composite.v1@1",
		"text.compile.v1@1",
		"video.concat.v1@1",
		"video.encode-h264.v1@1",
	}
	if fmt.Sprint(ids) != fmt.Sprint(wantIDs) {
		t.Errorf("IDs = %v, want %v", ids, wantIDs)
	}
}

func TestRegistry_All_Sorted(t *testing.T) {
	r := NewRegistry()
	r.MustRegister(newFake("zeta", 1, ResourceCPU, TemporalFrameLocal))
	r.MustRegister(newFake("alpha", 1, ResourceCPU, TemporalFrameLocal))
	r.MustRegister(newFake("mu", 1, ResourceCPU, TemporalFrameLocal))
	all := r.All()
	if len(all) != 3 {
		t.Fatalf("All len = %d, want 3", len(all))
	}
	want := []string{"alpha", "mu", "zeta"}
	for i, e := range all {
		if got := e.Descriptor().ID; got != want[i] {
			t.Errorf("[%d] = %q, want %q", i, got, want[i])
		}
	}
}

// ── Resolve / Has ─────────────────────────────────────────────────────────────

func TestRegistry_Resolve(t *testing.T) {
	r := NewRegistry()
	a := newFake("asset.prepare.v1", 1, ResourceCPU, TemporalFrameLocal)
	if err := r.Register(a); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, err := r.Resolve("asset.prepare.v1", 1)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != a {
		t.Errorf("Resolve returned different pointer than registered")
	}
	if !r.Has("asset.prepare.v1", 1) {
		t.Errorf("Has = false, want true")
	}

	_, err = r.Resolve("missing.v1", 1)
	if !errors.Is(err, ErrExecutorNotFound) {
		t.Errorf("missing: err = %v, want ErrExecutorNotFound", err)
	}
	_, err = r.Resolve("asset.prepare.v1", 999)
	if !errors.Is(err, ErrExecutorNotFound) {
		t.Errorf("wrong version: err = %v, want ErrExecutorNotFound", err)
	}
}

// ── Functional Descriptor fields round-trip ───────────────────────────────────

func TestRegistry_Descriptor_Key(t *testing.T) {
	d := Descriptor{ID: "scene.composite.v1", Version: 7}
	if got, want := d.Key(), "scene.composite.v1@7"; got != want {
		t.Errorf("Key() = %q, want %q", got, want)
	}
}

func TestRegistry_TaskSpec_Validate(t *testing.T) {
	good := TaskSpec{Version: 1, JobID: "j", ExecutorID: "asset.prepare.v1"}
	if err := good.Validate(); err != nil {
		t.Errorf("good: err = %v, want nil", err)
	}

	noJob := good
	noJob.JobID = ""
	if err := noJob.Validate(); !errors.Is(err, ErrInvalidDescriptor) {
		t.Errorf("noJob: err = %v, want ErrInvalidDescriptor", err)
	}

	noExec := good
	noExec.ExecutorID = ""
	if err := noExec.Validate(); !errors.Is(err, ErrInvalidDescriptor) {
		t.Errorf("noExec: err = %v, want ErrInvalidDescriptor", err)
	}

	zeroV := good
	zeroV.Version = 0
	if err := zeroV.Validate(); !errors.Is(err, ErrInvalidDescriptor) {
		t.Errorf("zeroV: err = %v, want ErrInvalidDescriptor", err)
	}

	var nilSpec *TaskSpec
	if err := nilSpec.Validate(); !errors.Is(err, ErrInvalidDescriptor) {
		t.Errorf("nil: err = %v, want ErrInvalidDescriptor", err)
	}
}

func TestRegistry_Enum_Validity(t *testing.T) {
	classes := []ResourceClass{ResourceCPU, ResourceGPU, ResourceMixed, ResourceIO}
	for _, c := range classes {
		if !c.Valid() {
			t.Errorf("ResourceClass %q should be valid", c)
		}
	}
	bogus := []ResourceClass{"qpu", "", "CP", "CPU "}
	for _, c := range bogus {
		if c.Valid() {
			t.Errorf("ResourceClass %q should not be valid", c)
		}
	}

	modes := []TemporalMode{TemporalFrameLocal, TemporalWindowed, TemporalStateful, TemporalGlobal}
	for _, m := range modes {
		if !m.Valid() {
			t.Errorf("TemporalMode %q should be valid", m)
		}
	}
	bogusM := []TemporalMode{"", "frame", "GLOBAL", "windowed "}
	for _, m := range bogusM {
		if m.Valid() {
			t.Errorf("TemporalMode %q should not be valid", m)
		}
	}
}

// ── Concurrency ───────────────────────────────────────────────────────────────

func TestRegistry_ConcurrentRegisterAndResolve(t *testing.T) {
	r := NewRegistry()
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = r.Register(newFake(fmt.Sprintf("exec.%d", i), 1, ResourceCPU, TemporalFrameLocal))
		}(i)
		go func(i int) {
			defer wg.Done()
			// Resolve may not find anything on the first pass — that's expected,
			// we just need it NOT to race.
			_, _ = r.Resolve(fmt.Sprintf("exec.%d", i), 1)
		}(i)
	}
	wg.Wait()
	if r.Len() != n {
		t.Errorf("Len = %d, want %d", r.Len(), n)
	}
}
