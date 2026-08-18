package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"velox-shared/contract/domain"

	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// TestFailureTracker_ClosedStoreEscalatesAfterThreshold exercises the full
// production path: a real SQLiteStore is closed, its repository method is
// called, and the resulting DomainError is classified and recorded directly
// by the FailureTracker. No test-side domain.NewInfrastructure wrapping is
// allowed here; the store owns SQL error classification.
func TestFailureTracker_ClosedStoreEscalatesAfterThreshold(t *testing.T) {
	realStore, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "closed-store.db"))
	if err != nil {
		t.Fatalf("open real sqlite store: %v", err)
	}
	if err := realStore.Close(); err != nil {
		t.Fatalf("close real sqlite store: %v", err)
	}

	tracker := supervisor.NewFailureTracker(supervisor.RetryPolicy{
		ConsecutiveErrorThreshold: 3,
		ResetWindow:               0,
	})

	for attempt := 1; attempt <= 3; attempt++ {
		_, err := realStore.Delivery().GetJobDelivery(context.Background(), "delivery-after-close")
		if err == nil {
			t.Fatalf("attempt %d: GetJobDelivery on closed store returned nil error", attempt)
		}
		derr, ok := domain.AsDomainError(err)
		if !ok {
			t.Fatalf("attempt %d: store error = %v, want DomainError", attempt, err)
		}
		if derr.Code != domain.CodeInfrastructure {
			t.Fatalf("attempt %d: DomainError code = %q, want %q", attempt, derr.Code, domain.CodeInfrastructure)
		}

		classified := supervisor.ClassifyError(err)
		if !supervisor.IsInfrastructure(classified) {
			t.Fatalf("attempt %d: classified store error = %v, want infrastructure", attempt, classified)
		}

		// Feed the actual store error through the production classifier;
		// do not construct or re-wrap a test-side infrastructure error.
		escalated := tracker.Record(classified)
		if attempt < 3 && escalated != nil {
			t.Fatalf("attempt %d: unexpected escalation: %v", attempt, escalated)
		}
		if attempt == 3 {
			if escalated == nil {
				t.Fatal("third closed-store failure should escalate")
			}
			if !supervisor.IsInfrastructure(escalated) {
				t.Fatalf("escalation = %v, want infrastructure", escalated)
			}
		}
	}
	if got := tracker.Consecutive(); got != 3 {
		t.Fatalf("consecutive infrastructure failures = %d, want 3", got)
	}
}
