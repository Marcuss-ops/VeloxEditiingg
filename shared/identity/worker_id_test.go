package identity

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseWorkerIDTrims(t *testing.T) {
	got := ParseWorkerID("  worker-abc123  ")
	if got != "worker-abc123" {
		t.Fatalf("ParseWorkerID = %q, want worker-abc123", got)
	}
	if got.String() != "worker-abc123" {
		t.Fatalf("String() = %q, want worker-abc123", got.String())
	}
}

func TestParseWorkerIDKeepsUnnormalizedInput(t *testing.T) {
	// Parse is deliberately pure: normalization is opt-in.
	got := ParseWorkerID("host_host_57_129_132_133")
	if got.String() != "host_host_57_129_132_133" {
		t.Fatalf("ParseWorkerID mutated input: %q", got.String())
	}
	if n := got.Normalized(); n.String() != "host_57_129_132_133" {
		t.Fatalf("Normalized() = %q, want host_57_129_132_133", n.String())
	}
}

func TestWorkerIDIsEmpty(t *testing.T) {
	if !WorkerID("").IsEmpty() {
		t.Fatal("empty WorkerID should report IsEmpty")
	}
	if WorkerID("worker-1").IsEmpty() {
		t.Fatal("non-empty WorkerID should not report IsEmpty")
	}
}

func TestWorkerIDIsValid(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"worker-8e98ce85", true},
		{"host_57_129_132_133", true},
		{"w1", false},
		{"Worker-001", false},
		{"", false},
		{"worker.1", false},
	}
	for _, tc := range cases {
		if got := WorkerID(tc.in).IsValid(); got != tc.want {
			t.Errorf("IsValid(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestWorkerIDValidate(t *testing.T) {
	if err := WorkerID("").Validate(); err == nil {
		t.Fatal("empty ID should fail Validate")
	}
	if err := WorkerID("Worker-001").Validate(); err == nil {
		t.Fatal("uppercase ID should fail Validate")
	}
	if err := WorkerID("worker-001").Validate(); err != nil {
		t.Fatalf("valid ID should pass Validate: %v", err)
	}
}

func TestNewWorkerIDShape(t *testing.T) {
	id := NewWorkerID()
	if !strings.HasPrefix(id.String(), "worker-") {
		t.Fatalf("NewWorkerID = %q, want worker- prefix", id.String())
	}
	if !id.IsValid() {
		t.Fatalf("NewWorkerID = %q is not valid", id.String())
	}
}

func TestWorkerIdentityValidate(t *testing.T) {
	if err := (WorkerIdentity{}).Validate(); err == nil {
		t.Fatal("empty WorkerIdentity should fail Validate")
	}
	wi := NewWorkerIdentity(ParseWorkerID("worker-001"), "machine-id-1234", "node-7")
	if err := wi.Validate(); err != nil {
		t.Fatalf("valid WorkerIdentity should pass Validate: %v", err)
	}
	if wi.HostID != "machine-id-1234" || wi.NodeName != "node-7" {
		t.Fatalf("WorkerIdentity attributes not preserved: %+v", wi)
	}
}

func TestWorkerIDJSONRoundTrip(t *testing.T) {
	// The typed type serializes as a plain JSON string, so JSON-bound
	// surfaces (raw_json persistence, admin APIs) keep their contract.
	want := `"worker-8e98ce85"`
	b, err := json.Marshal(WorkerID("worker-8e98ce85"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != want {
		t.Fatalf("marshal = %s, want %s", b, want)
	}
}
