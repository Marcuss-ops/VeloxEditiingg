package identity

import (
	"strings"
	"testing"
)

func TestParseWorkerNameTrims(t *testing.T) {
	got := ParseWorkerName("  velox-worker-01  ")
	if got.String() != "velox-worker-01" {
		t.Fatalf("ParseWorkerName trimmed = %q, want %q", got.String(), "velox-worker-01")
	}
}

func TestWorkerNameValidate(t *testing.T) {
	valid := []string{"velox-worker", "Test Worker", "velox_worker_readable", "worker-e2e"}
	for _, in := range valid {
		if err := ParseWorkerName(in).Validate(); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", in, err)
		}
	}

	invalid := []struct {
		name    string
		wantSub string
	}{
		{"", "empty"},
		{"   ", "empty"},
		{"line\nbreak", "control characters"},
		{"tab\tname", "control characters"},
		{"nul\x00byte", "control characters"},
		{strings.Repeat("x", MaxWorkerNameLength+1), "exceeds"},
	}
	for _, tc := range invalid {
		err := ParseWorkerName(tc.name).Validate()
		if err == nil {
			t.Errorf("Validate(%q) = nil, want error containing %q", tc.name, tc.wantSub)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("Validate(%q) error = %q, want substring %q", tc.name, err.Error(), tc.wantSub)
		}
	}
}

func TestWorkerNameIsEmpty(t *testing.T) {
	if !ParseWorkerName("   ").IsEmpty() {
		t.Error("ParseWorkerName(whitespace).IsEmpty() = false, want true")
	}
	if ParseWorkerName("worker").IsEmpty() {
		t.Error("ParseWorkerName(worker).IsEmpty() = true, want false")
	}
}
