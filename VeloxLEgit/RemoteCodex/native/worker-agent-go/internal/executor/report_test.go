package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"testing"

	"velox-worker-agent/pkg/api"
) // stubExecutor is a minimal Executor for testing the report builder.
// We deliberately avoid pulling in a real renderer adapter to keep the
// test surface purely about the capability mapping.
type stubExecutor struct {
	d Descriptor
}

func (s *stubExecutor) Descriptor() Descriptor    { return s.d }
func (s *stubExecutor) Validate(_ TaskSpec) error { return nil }
func (s *stubExecutor) Execute(_ context.Context, _ ExecutionContext, _ TaskSpec) (ExecutionResult, error) {
	return ExecutionResult{}, nil
}

// Validate suite covers the 5 PR-3.5 invariants:
//
//  1. Empty registry -> valid CapabilityReport with no Executors.
//  2. Single executor -> exactly one ExecutorCapability, fields mapped.
//  3. Multiple executors -> sorted by (ID, Version) regardless of
//     registration order.
//  4. Determinism: JSON encoding of two reports built from equivalent
//     registries produces identical byte sequences ("stable hash").
//  5. SchemaVersion == api.CapabilitySchemaVersion (closed constant).

func TestBuildCapabilityReport_EmptyRegistry(t *testing.T) {
	reg := NewRegistry()
	host := api.HostInfo{WorkerID: "w-1", Hostname: "host-a"}

	got := BuildCapabilityReport(reg, host)

	if got.SchemaVersion != api.CapabilitySchemaVersion {
		t.Fatalf("schema_version mismatch: got %d, want %d", got.SchemaVersion, api.CapabilitySchemaVersion)
	}
	if got.Host.WorkerID != "w-1" || got.Host.Hostname != "host-a" {
		t.Fatalf("host not preserved: %+v", got.Host)
	}
	if len(got.Executors) != 0 {
		t.Fatalf("expected 0 executors, got %d: %+v", len(got.Executors), got.Executors)
	}

	// AsMap must also be deterministic with empty executors.
	m := got.AsMap()
	if _, ok := m["executors"]; !ok {
		t.Fatalf("AsMap missing 'executors' key: %v", m)
	}
	arr, ok := m["executors"].([]interface{})
	if !ok {
		t.Fatalf("AsMap 'executors' not a slice: %T", m["executors"])
	}
	if len(arr) != 0 {
		t.Fatalf("AsMap 'executors' length %d, want 0", len(arr))
	}
}

func TestBuildCapabilityReport_SingleExecutor(t *testing.T) {
	reg := NewRegistry()
	reg.MustRegister(&stubExecutor{d: Descriptor{
		ID:            "scene.composite.v1",
		Version:       1,
		ResourceClass: ResourceGPU,
		TemporalMode:  TemporalGlobal,
		Deterministic: true,
		Cacheable:     true,
		SupportsAlpha: false,
		OutputTypes:   []string{"video/mp4"},
	}})

	got := BuildCapabilityReport(reg, api.HostInfo{WorkerID: "w-x"})

	if len(got.Executors) != 1 {
		t.Fatalf("expected 1 executor, got %d", len(got.Executors))
	}
	e := got.Executors[0]
	if e.ID != "scene.composite.v1" || e.Version != 1 {
		t.Fatalf("id/version mismatch: %+v", e)
	}
	if e.ResourceClass != string(ResourceGPU) {
		t.Fatalf("resource_class mismatch: got %q want %q", e.ResourceClass, ResourceGPU)
	}
	if e.TemporalMode != string(TemporalGlobal) {
		t.Fatalf("temporal_mode mismatch: got %q want %q", e.TemporalMode, TemporalGlobal)
	}
	if !e.Deterministic || !e.Cacheable || e.SupportsAlpha {
		t.Fatalf("flag mismatch: %+v", e)
	}
	if len(e.OutputTypes) != 1 || e.OutputTypes[0] != "video/mp4" {
		t.Fatalf("output_types mismatch: %+v", e.OutputTypes)
	}
}

func TestBuildCapabilityReport_SortedByIDThenVersion(t *testing.T) {
	// Register in deliberately-unsorted order to assert that the report
	// order matches Registry.Descriptors() (which sorts by (ID, Version))
	// rather than registration order.
	reg := NewRegistry()
	register := func(id string, version int) {
		reg.MustRegister(&stubExecutor{d: Descriptor{
			ID:            id,
			Version:       version,
			ResourceClass: ResourceCPU,
			TemporalMode:  TemporalFrameLocal,
		}})
	}
	register("zzz.last", 1)
	register("aaa.first", 2)
	register("aaa.first", 1)
	register("middle.one", 9)

	got := BuildCapabilityReport(reg, api.HostInfo{})

	// Expected order: aaa.first@1, aaa.first@2, middle.one@9, zzz.last@1
	want := []string{"aaa.first@1", "aaa.first@2", "middle.one@9", "zzz.last@1"}
	if len(got.Executors) != len(want) {
		t.Fatalf("length mismatch: got %d, want %d", len(got.Executors), len(want))
	}
	for i, w := range want {
		gotKey := got.Executors[i].ID + "@" + strconv.Itoa(got.Executors[i].Version)
		if gotKey != w {
			t.Fatalf("position %d: got %s, want %s", i, gotKey, w)
		}
	}

	// Sanity: ensure Descriptors() is actually the source of truth.
	descs := reg.Descriptors()
	seen := make([]string, 0, len(descs))
	for _, d := range descs {
		seen = append(seen, d.Key())
	}
	if !sort.StringsAreSorted(seen) {
		t.Fatalf("Descriptors() not sorted (it should be by Key()): %v", seen)
	}
}

func TestBuildCapabilityReport_DeterministicAcrossBoots(t *testing.T) {
	// Same inputs -> same bytes. This is the "stable hello" invariant
	// the contract promises across worker restarts.
	reg := NewRegistry()
	reg.MustRegister(&stubExecutor{d: Descriptor{
		ID:            "render.image.v1",
		Version:       3,
		ResourceClass: ResourceCPU,
		TemporalMode:  TemporalFrameLocal,
		Deterministic: true,
	}})
	reg.MustRegister(&stubExecutor{d: Descriptor{
		ID:            "scene.composite.v1",
		Version:       1,
		ResourceClass: ResourceGPU,
		TemporalMode:  TemporalGlobal,
	}})

	host := api.HostInfo{
		WorkerID:        "w-stable",
		Hostname:        "render-001",
		CPUCount:        8,
		MaxParallelJobs: 4,
		HasGPU:          false,
	}

	a := BuildCapabilityReport(reg, host)
	b := BuildCapabilityReport(reg, host)

	hashJSON := func(in api.CapabilityReport) string {
		jb, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		sum := sha256.Sum256(jb)
		return hex.EncodeToString(sum[:])
	}

	h1, h2 := hashJSON(a), hashJSON(b)
	if h1 != h2 {
		t.Fatalf("non-deterministic: %s vs %s", h1, h2)
	}

	// Also assert the AsMap envelope is byte-stable through the same path
	// the worker.hello / heartbeat use.
	am, err := json.Marshal(a.AsMap())
	if err != nil {
		t.Fatalf("marshal AsMap: %v", err)
	}
	bm, err := json.Marshal(b.AsMap())
	if err != nil {
		t.Fatalf("marshal AsMap: %v", err)
	}
	if string(am) != string(bm) {
		t.Fatalf("AsMap non-deterministic:\nA=%s\nB=%s", am, bm)
	}
}

func TestBuildCapabilityReport_SchemaVersionConstant(t *testing.T) {
	// SchemaVersion must be the closed constant.
	reg := NewRegistry()
	got := BuildCapabilityReport(reg, api.HostInfo{})
	if got.SchemaVersion != 1 {
		t.Fatalf("expected schema_version=1, got %d", got.SchemaVersion)
	}
	if api.CapabilitySchemaVersion != 1 {
		t.Fatalf("constant drift: api.CapabilitySchemaVersion=%d, baked expectation=1",
			api.CapabilitySchemaVersion)
	}
}

func TestBuildCapabilityReport_NilRegistryPanics(t *testing.T) {
	// PR-3.5 consistency: nil-registry calls panic loudly to match
	// WithRegistry(nil) — both surface operator bugs at startup
	// instead of silently emitting zero-capability hello.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("BuildCapabilityReport(nil) should panic; got nil recovery")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value type %T, want string", r)
		}
		if msg != "executor.BuildCapabilityReport: registry must not be nil — pass executor.NewRegistry() for an empty default" {
			t.Fatalf("unexpected panic message: %q", msg)
		}
	}()
	_ = BuildCapabilityReport(nil, api.HostInfo{WorkerID: "w-nil"})
}
