package performance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	attempttelemetry "velox-worker-agent/internal/telemetry"
)

// TestReceiptBuildIsDeterministic pins the "PerformanceReceipt build is
// deterministic" invariant: assembling the SAME canonical snapshot (or the
// same pipeline run + assembly context) twice must yield byte-identical JSON.
//
// The receipt is the artifact that benchmark runs diff across attempts; any
// nondeterminism (map iteration order, wall-clock capture inside the
// assembler, timestamp generation) would make every comparison flaky. Both
// assembly paths — the canonical AttemptSnapshot projection and the legacy
// RunMetrics assembler — are pinned.
func TestReceiptBuildIsDeterministic(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		Identity: attempttelemetry.AttemptIdentity{JobID: "job-1", AttemptID: "attempt-1", WorkerID: "worker-1"},
		Resources: attempttelemetry.TypedExecutionMetrics{
			CpuTimeMs:        2000,
			PeakRssBytes:     4096,
			DiskReadBytes:    1000,
			DiskWriteBytes:   500,
			WallClockSeconds: 10,
		},
		Process: attempttelemetry.ProcessFacts{EngineSpawnCount: 1},
		Media:   attempttelemetry.MediaFacts{BytesOut: 42, FramesOut: 500},
		WallMs:  10000,
		Events: []attempttelemetry.RecordedPhase{
			{Component: "engine", Action: "render", Phase: "render", DurationMS: 900, Status: attempttelemetry.StatusOK},
			{Component: "engine.video", Action: "decode", Phase: "decode", DurationMS: 700, Status: attempttelemetry.StatusOK},
		},
	}

	first, err := AssembleFromSnapshot(snapshot).ToJSON()
	if err != nil {
		t.Fatalf("first assembly ToJSON: %v", err)
	}
	second, err := AssembleFromSnapshot(snapshot).ToJSON()
	if err != nil {
		t.Fatalf("second assembly ToJSON: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("AssembleFromSnapshot is not deterministic:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}

	// Repeat the identical input object to catch stateful assembly (the
	// snapshot pointer must not be mutated across calls).
	third, err := AssembleFromSnapshot(snapshot).ToJSON()
	if err != nil {
		t.Fatalf("third assembly ToJSON: %v", err)
	}
	if !bytes.Equal(first, third) {
		t.Fatal("AssembleFromSnapshot mutates its input across calls (nondeterministic)")
	}
}

// TestRunMetricsAssemblyIsDeterministic pins the same invariant on the
// legacy RunMetrics assembler path.
func TestRunMetricsAssemblyIsDeterministic(t *testing.T) {
	ctx := AssemblyContext{
		Identity: PerformanceIdentity{JobID: "job-1", AttemptID: "attempt-1"},
		Workload: WorkloadProfile{ClipCount: 2, DurationUS: 2_000_000},
	}
	run := sampleRun()

	first, err := NewAssembler().Assemble(run, ctx).ToJSON()
	if err != nil {
		t.Fatalf("first assembly ToJSON: %v", err)
	}
	second, err := NewAssembler().Assemble(run, ctx).ToJSON()
	if err != nil {
		t.Fatalf("second assembly ToJSON: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("Assemble is not deterministic:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
}

// TestAssembledReceiptHasStableFieldOrder pins that the JSON field order of
// an assembled receipt is stable and section-complete: a receipt built twice
// from the same facts must marshal with identical key sequences, so diffs
// across benchmark runs never churn on ordering.
func TestAssembledReceiptHasStableFieldOrder(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		Identity: attempttelemetry.AttemptIdentity{AttemptID: "order"},
		WallMs:   1000,
		Resources: attempttelemetry.TypedExecutionMetrics{
			CpuTimeMs:     250,
			DiskReadBytes: 10,
		},
	}
	a, err := AssembleFromSnapshot(snapshot).ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	b, err := AssembleFromSnapshot(snapshot).ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("field order changed between identical assemblies")
	}

	// Key order sanity: decode only the top-level object keys instead of
	// searching raw bytes. A byte search can accidentally find a nested
	// field with the same name and let a reordered receipt pass.
	// encoding/json preserves struct field order, which is part of the
	// deterministic benchmark artifact contract.
	wantOrder := []string{"version", "identity", "workload", "timing", "process", "cpu", "io", "media", "memory", "scheduling", "derived"}
	gotOrder, err := topLevelJSONKeys(a)
	if err != nil {
		t.Fatalf("decode receipt top-level keys: %v", err)
	}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("top-level key count = %d, want %d (%v)", len(gotOrder), len(wantOrder), gotOrder)
	}
	for i, key := range wantOrder {
		if gotOrder[i] != key {
			t.Fatalf("top-level key %d = %q, want %q (all keys: %v)", i, gotOrder[i], key, gotOrder)
		}
	}
}

// topLevelJSONKeys returns object keys in encoded order. It deliberately
// consumes each value as RawMessage so nested keys cannot affect the
// ordering assertion while malformed/truncated receipts still fail loudly.
func topLevelJSONKeys(data []byte) ([]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	start, ok := token.(json.Delim)
	if !ok || start != '{' {
		return nil, fmt.Errorf("receipt root is %T %v, want object", token, token)
	}
	keys := make([]string, 0, 12)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("receipt object key is %T %v, want string", keyToken, keyToken)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return nil, fmt.Errorf("receipt root did not close with object delimiter: %v", end)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("receipt contains trailing JSON after root object")
		}
		return nil, fmt.Errorf("receipt has trailing data: %w", err)
	}
	return keys, nil
}

// TestDeriveIsDeterministicAndPure pins that Derive is a pure function of
// its RawMetrics input: identical raw facts yield identical DerivedMetrics.
func TestDeriveIsDeterministicAndPure(t *testing.T) {
	raw := RawMetrics{
		WallMs: 10000,
		Phases: []PhaseTiming{
			{Name: "render", DurationMS: 9000, TimingMode: "exclusive"},
			{Name: "decode", DurationMS: 7000, TimingMode: "span_child"},
		},
		CPUWallMS:            2000,
		TotalBytesRead:       5000,
		TotalBytesWritten:    3000,
		OutputBytes:          1000,
		ExternalProcessCount: 0,
		ClipCount:            5,
	}
	a := Derive(raw)
	b := Derive(raw)
	if a != b {
		t.Fatalf("Derive not deterministic: %+v vs %+v", a, b)
	}
	// The input must not be mutated (pure function).
	if len(raw.Phases) != 2 || raw.Phases[0].DurationMS != 9000 {
		t.Fatalf("Derive mutated its input: %+v", raw)
	}
}
