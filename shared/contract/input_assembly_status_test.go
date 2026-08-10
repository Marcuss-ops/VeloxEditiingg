package contract

import "testing"

func TestInputAssemblyStatusValidity(t *testing.T) {
	if !InputAssemblyCompleted.Valid() || string(InputAssemblyCompleted) != "completed" {
		t.Fatalf("completed input-assembly status = %q", InputAssemblyCompleted)
	}
	if InputAssemblyStatus("SUCCEEDED").Valid() {
		t.Fatal("SUCCEEDED is not an input-assembly status")
	}
}

func TestParseInputAssemblyStatusAcceptsLegacyWireSpellings(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want InputAssemblyStatus
	}{
		{raw: "PENDING", want: InputAssemblyPending},
		{raw: " completed ", want: InputAssemblyCompleted},
		{raw: "COMPLETED_WITH_WARNINGS", want: InputAssemblyCompletedWithWarnings},
	} {
		got, ok := ParseInputAssemblyStatus(tc.raw)
		if !ok || got != tc.want {
			t.Fatalf("ParseInputAssemblyStatus(%q) = %q, %v; want %q, true", tc.raw, got, ok, tc.want)
		}
	}
	if _, ok := ParseInputAssemblyStatus("SUCCEEDED"); ok {
		t.Fatal("job status must not parse as input-assembly status")
	}
}

func TestCompletedWireStatusNeverBecomesJobOrPublicationTerminal(t *testing.T) {
	payload := NewJobPayloadV2(map[string]any{"status": string(InputAssemblyCompleted)})
	if got := payload.InputAssemblyStatus(); got != InputAssemblyCompleted {
		t.Fatalf("input assembly status = %q, want %q", got, InputAssemblyCompleted)
	}
	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("ToMap: %v", err)
	}
	if got := mapped["status"]; got != string(InputAssemblyCompleted) {
		t.Fatalf("wire status = %v, want completed", got)
	}
	for _, terminal := range []string{"SUCCEEDED", "PUBLISHED"} {
		if mapped["status"] == terminal {
			t.Fatalf("completed input handoff emitted lifecycle terminal %q", terminal)
		}
	}
}

func TestJobPayloadInputAssemblyStatusPreservesWireValue(t *testing.T) {
	incoming := NewJobPayloadV2(map[string]any{"status": "completed"})
	if got := incoming.InputAssemblyStatus(); got != InputAssemblyCompleted {
		t.Fatalf("incoming completed status = %q, want %q", got, InputAssemblyCompleted)
	}

	payload := NewJobPayloadV2(map[string]any{})
	if got := payload.InputAssemblyStatus(); got != InputAssemblyPending {
		t.Fatalf("default payload input assembly status = %q, want %q", got, InputAssemblyPending)
	}
	if !payload.SetInputAssemblyStatus(InputAssemblyPending) {
		t.Fatal("SetInputAssemblyStatus rejected pending")
	}
	pending, err := payload.ToMap()
	if err != nil {
		t.Fatalf("ToMap pending: %v", err)
	}
	if got := pending["status"]; got != "PENDING" {
		t.Fatalf("pending wire status = %v, want PENDING", got)
	}
	if !payload.SetInputAssemblyStatus(InputAssemblyCompleted) {
		t.Fatal("SetInputAssemblyStatus rejected a valid status")
	}
	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("ToMap: %v", err)
	}
	if got := mapped["status"]; got != "completed" {
		t.Fatalf("wire status = %v, want completed", got)
	}
}
