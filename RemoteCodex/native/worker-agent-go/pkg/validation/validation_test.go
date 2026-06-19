package validation

import (
	"strings"
	"testing"
)

// FieldErrors aggregation behaviour is covered here; the ID validators
// (IsAlphanumericID/IsHexRun) are exercised in velox-shared/validation and
// re-exported from this package via id.go.

func TestFieldErrorsAggregation(t *testing.T) {
	var errs FieldErrors
	errs.AddMsg("job_id", "is required")
	errs.Add("priority", "must be in [0,3]", "9")
	errs.AddMsg("created_at", "must be RFC3339")

	if !errs.HasErrors() {
		t.Fatal("expected HasErrors")
	}
	s := errs.Error()
	for _, want := range []string{"job_id", "priority", "created_at"} {
		if !strings.Contains(s, want) {
			t.Errorf("error %q missing field %q", s, want)
		}
	}
	if errs.OrNil() == nil {
		t.Fatal("OrNil returned nil despite errors")
	}
	var empty FieldErrors
	if empty.OrNil() != nil {
		t.Fatal("OrNil on empty should return nil")
	}
}

func TestAlphanumericIDReExport(t *testing.T) {
	// Smoke test: ensure the re-export from velox-shared/validation still
	// answers consistently with this package's MaxIdentifierLength constant.
	if !IsAlphanumericID("abc-123") {
		t.Fatal("expected abc-123 to be valid")
	}
	if IsAlphanumericID("abc.def") {
		t.Fatal("expected abc.def to be invalid")
	}
	if MaxIdentifierLength != 128 {
		t.Fatalf("expected MaxIdentifierLength=128, got %d", MaxIdentifierLength)
	}
}
