// Package store / store_delivery_destinations_status_test.go
//
// Unit tests for the 3-state BatchDeliveryDestinationsStatus
// helper introduced alongside the §0.3.4 item 4 split (NIT-2).
//
// The previous 2-state helper (BatchDeliveryDestinationsExistAndEnabled)
// collapsed "row missing" and "row present-but-disabled" into the
// same `false`, which was the root cause of the operator diagnostic
// ambiguity. The new helper covers all three buckets with a single
// round-trip query, which these tests pin.
package store

import (
	"context"
	"testing"
)

// TestBatchDeliveryDestinationsStatus_AllThreeBuckets seeds one
// ENABLED row, one DISABLED row, and queries a third ID that does
// NOT exist. The helper MUST return all three buckets correctly
// from a single call (no second round-trip per bucket).
//
// This is the test (a) of the §0.3.4 split coverage: it pins the
// store-layer contract that allows job_submit.go to emit distinct
// target_error_code values (BLOCKED_VELOX_DISABLED vs
// DESTINATION_NOT_FOUND vs no-error) per bucket.
func TestBatchDeliveryDestinationsStatus_AllThreeBuckets(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/3state.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Seed ENABLED row.
	if err := s.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID:         "dest-enabled",
		Provider:              "social_gateway",
		ExternalDestinationID: "dest-enabled-external",
		Enabled:               true,
	}); err != nil {
		t.Fatalf("seed enabled dest: %v", err)
	}

	// Seed DISABLED row.
	if err := s.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID:         "dest-disabled",
		Provider:              "social_gateway",
		ExternalDestinationID: "dest-disabled-external",
		Enabled:               false,
	}); err != nil {
		t.Fatalf("seed disabled dest: %v", err)
	}

	// Query all three (one ENABLED, one DISABLED, one NOT_FOUND).
	got, err := s.BatchDeliveryDestinationsStatus(ctx, []string{
		"dest-enabled",
		"dest-disabled",
		"dest-missing",
	})
	if err != nil {
		t.Fatalf("BatchDeliveryDestinationsStatus: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("map size = %d, want 3 (one per input id)", len(got))
	}

	if got["dest-enabled"] != DeliveryDestinationEnabled {
		t.Errorf("dest-enabled: got %s, want %s (%d)",
			got["dest-enabled"], DeliveryDestinationEnabled, DeliveryDestinationEnabled)
	}
	if got["dest-disabled"] != DeliveryDestinationDisabled {
		t.Errorf("dest-disabled: got %s, want %s (%d)",
			got["dest-disabled"], DeliveryDestinationDisabled, DeliveryDestinationDisabled)
	}
	if got["dest-missing"] != DeliveryDestinationNotFound {
		t.Errorf("dest-missing: got %s, want %s (%d)",
			got["dest-missing"], DeliveryDestinationNotFound, DeliveryDestinationNotFound)
	}
}

// TestBatchDeliveryDestinationsStatus_StringContracts pins the
// canonical lowercase wire-format names for the three status
// buckets. The handler-layer pre-flight writes these into the
// response envelope under details[].status (see job_submit.go).
// Changing the strings is a wire-format break, so this test
// guards against accidental renames.
func TestBatchDeliveryDestinationsStatus_StringContracts(t *testing.T) {
	cases := []struct {
		status DeliveryDestinationStatus
		want   string
	}{
		{DeliveryDestinationNotFound, "not_found"},
		{DeliveryDestinationDisabled, "disabled"},
		{DeliveryDestinationEnabled, "enabled"},
	}
	for _, tc := range cases {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("DeliveryDestinationStatus(%d).String() = %q, want %q",
				tc.status, got, tc.want)
		}
	}
}

// TestBatchDeliveryDestinationsStatus_DeduplicatesAndTrims pins
// the input normalization (whitespace trim + dup removal) so the
// handler-layer caller can pass raw delivery_plan[].destination_id
// values without pre-cleaning. Mirrors the 2-state helper's
// contract (preserved verbatim — see store_deliveries.go doc).
func TestBatchDeliveryDestinationsStatus_DeduplicatesAndTrims(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/dedupe.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID:         "dest-x",
		Provider:              "social_gateway",
		ExternalDestinationID: "dest-x-external",
		Enabled:               true,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 5 inputs collapse to 1 unique id ("dest-x" after trim).
	got, err := s.BatchDeliveryDestinationsStatus(ctx, []string{
		"  dest-x  ",
		"dest-x",
		"dest-x",
		"",
		"   ",
	})
	if err != nil {
		t.Fatalf("BatchDeliveryDestinationsStatus: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("map size = %d, want 1 (input collapsed to 1 unique id %q)", len(got), "dest-x")
	}
	if got["dest-x"] != DeliveryDestinationEnabled {
		t.Errorf("dest-x: got %s, want ENABLED (3)", got["dest-x"])
	}
}

// TestValidateDeliveryDestinationTx_ErrDestinationDisabledIs pins
// the typed-sentinel contract introduced alongside the §0.3.4
// split. The wrap-via-%w pattern preserves the legacy substring
// "destination_id %q is globally disabled" used by string-match
// assertions in atomic_job_task_test.go AND unlocks errors.Is(...)
// for new callers (e.g. a future resolver-layer mapper that
// surfaces BLOCKED_VELOX_DISABLED envelopes).
func TestValidateDeliveryDestinationTx_ErrDestinationDisabledIs(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/typed_err.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID:         "dest-disabled",
		Provider:              "social_gateway",
		ExternalDestinationID: "dest-disabled-external",
		Enabled:               false,
	}); err != nil {
		t.Fatalf("seed disabled: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	err = validateDeliveryDestinationTx(ctx, tx, "dest-disabled")
	if err == nil {
		t.Fatal("validateDeliveryDestinationTx(disabled): got nil, want non-nil")
	}
	if !errorsHelperIs(err, ErrDestinationDisabled) {
		t.Fatalf("validateDeliveryDestinationTx(disabled): err = %v; want errors.Is(ErrDestinationDisabled)", err)
	}
	// Stable contract string still present for legacy substring-match assertions.
	if !stringsHelperContains(err.Error(), "is globally disabled") {
		t.Errorf("err = %q; want legacy substring %q preserved", err.Error(), "is globally disabled")
	}
}

// errorsHelperIs is a tiny shim so the test file does not pull
// in a top-level "errors" import when the rest of this file's
// imports are scoped to testing. Kept here as a private helper
// to match the pattern at internal/store/store_alert_events_test.go.
func errorsHelperIs(err, target error) bool {
	// minimal stdlib equivalent without an extra import at the
	// top of the file (the package already imports context +
	// testing via NewSQLiteStore + t.Fatalf).
	for err != nil {
		if err == target {
			return true
		}
		type unwrap interface{ Unwrap() error }
		if u, ok := err.(unwrap); ok {
			err = u.Unwrap()
			continue
		}
		return false
	}
	return false
}

// stringsHelperContains mirrors strings.Contains here for the
// same reason as errorsHelperIs above (avoid pulling a
// top-level import into a focused unit test).
func stringsHelperContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
