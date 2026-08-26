package deliveries

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/deliverystore"
	"velox-server/internal/store"
	"velox-shared/contract/domain"
)

func TestDomainErrorDrivesDeliveryClassification(t *testing.T) {
	err := domain.NewInvalidPayload("delivery_plan.0.priority", "out_of_range", "priority must be non-negative")
	wrapped := fmt.Errorf("delivery preflight: %w", err)

	if got := ClassifyError(wrapped); got != ErrorClassPermanent {
		t.Fatalf("ClassifyError=%d, want permanent", got)
	}
	if got := classifyErrorCode(wrapped); got != domain.FailureInvalidPayload {
		t.Fatalf("classifyErrorCode=%q, want %q", got, domain.FailureInvalidPayload)
	}
}

// domainErrorProvider returns a wrapped DomainError so this test exercises the
// same error shape adapters use at the provider boundary, rather than calling
// the runner's persistence methods directly.
type domainErrorProvider struct {
	err error
}

func (p domainErrorProvider) Name() string { return "domain-error-test" }

func (p domainErrorProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	return nil, fmt.Errorf("provider delivery: %w", p.err)
}

func seedDomainErrorDelivery(t *testing.T, db *store.SQLiteStore, suffix string, maxAttempts ...int) deliverystore.DeliveryLease {
	t.Helper()
	const providerName = "domain-error-test"
	destinationID := "domain-error-destination-" + suffix
	artifactID := "domain-error-artifact-" + suffix
	deliveryID := "domain-error-delivery-" + suffix
	now := time.Now().UTC().Format(time.RFC3339)

	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{
		DestinationID:         destinationID,
		Provider:              providerName,
		ExternalDestinationID: "external-" + suffix,
		Enabled:               true,
		ConfigurationJSON:     "{}",
	}); err != nil {
		t.Fatalf("InsertDeliveryDestination: %v", err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           "domain-error-job-" + suffix,
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), suffix+".mp4"),
		SHA256:          "domain-error-sha-" + suffix,
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("InsertArtifact: %v", err)
	}
	budget := 3
	if len(maxAttempts) > 0 {
		budget = maxAttempts[0]
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  destinationID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    budget,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertJobDelivery: %v", err)
	}
	if budget == 0 {
		if _, err := db.DB().Exec(`UPDATE job_deliveries SET max_attempts = 0 WHERE delivery_id = ?`, deliveryID); err != nil {
			t.Fatalf("set explicit zero retry budget: %v", err)
		}
	}

	leases, err := db.Delivery().ClaimDeliveries(context.Background(), "domain-error-runner-"+suffix, 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("ClaimDeliveries returned %d leases, want 1", len(leases))
	}
	return leases[0]
}

func TestDeliveryRunnerZeroRetryBudgetFailsAfterInitialAttempt(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "zero-budget.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	lease := seedDomainErrorDelivery(t, db, "zero-budget", 0)
	registry := NewRegistry()
	registry.Register(domainErrorProvider{err: &domain.DomainError{
		Code:        domain.CodeDeliveryDestinationRejected,
		Issue:       "provider_unavailable",
		Retryable:   true,
		PublicText:  "provider temporarily unavailable",
		FailureCode: domain.FailureDestinationUnavailable,
		MetricCode:  domain.MetricDestinationUnavailable,
		Component:   domain.ComponentDelivery,
		Phase:       "provider",
	}})
	runner := NewDeliveryRunner(&RunnerConfig{
		LeaseDuration:   time.Minute,
		MaxAttempts:     3,
		BackoffSchedule: []time.Duration{time.Millisecond},
	}, registry, db.Delivery(), db, "domain-error-runner-zero-budget")

	if runErr := runner.processLease(context.Background(), lease); runErr == nil {
		t.Fatal("processLease returned nil for exhausted zero retry budget")
	}
	row, err := db.Delivery().GetJobDelivery(context.Background(), lease.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if row.Status != "FAILED" {
		t.Fatalf("status=%q, want FAILED", row.Status)
	}
	if row.NextAttemptAt != "" {
		t.Fatalf("next_attempt_at=%q, want empty", row.NextAttemptAt)
	}
}

func TestDomainErrorPropagatesThroughDeliveryRunner(t *testing.T) {
	tests := []struct {
		name          string
		err           *domain.DomainError
		wantStatus    DeliveryStatus
		wantCode      string
		wantMessage   string
		wantRunnerErr bool
		wantRetry     bool
	}{
		{
			name: "retryable domain error schedules retry",
			err: &domain.DomainError{
				Code:        domain.CodeDeliveryDestinationRejected,
				Issue:       "provider_unavailable",
				Retryable:   true,
				PublicText:  "provider temporarily unavailable",
				FailureCode: domain.FailureDestinationUnavailable,
				MetricCode:  domain.MetricDestinationUnavailable,
				Component:   domain.ComponentDelivery,
				Phase:       "provider",
			},
			wantStatus:  DeliveryRetryWait,
			wantCode:    domain.FailureDestinationUnavailable,
			wantMessage: "provider delivery: provider temporarily unavailable",
			wantRetry:   true,
		},
		{
			name:          "terminal domain error fails delivery",
			err:           domain.NewInvalidPayload("delivery_plan.0.priority", "out_of_range", "priority must be non-negative"),
			wantStatus:    DeliveryFailed,
			wantCode:      domain.FailureInvalidPayload,
			wantMessage:   "provider delivery: priority must be non-negative",
			wantRunnerErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "domain-error.sqlite"))
			if err != nil {
				t.Fatalf("NewSQLiteStore: %v", err)
			}
			defer db.Close()

			lease := seedDomainErrorDelivery(t, db, "case")
			registry := NewRegistry()
			registry.Register(domainErrorProvider{err: tt.err})
			runner := NewDeliveryRunner(&RunnerConfig{
				LeaseDuration:   time.Minute,
				MaxAttempts:     3,
				BackoffSchedule: []time.Duration{time.Millisecond},
			}, registry, db.Delivery(), db, "domain-error-runner-case")

			runErr := runner.processLease(context.Background(), lease)
			if tt.wantRunnerErr && runErr == nil {
				t.Fatal("processLease returned nil for terminal DomainError")
			}
			if !tt.wantRunnerErr && runErr != nil {
				t.Fatalf("processLease returned error for retryable DomainError: %v", runErr)
			}

			row, err := db.Delivery().GetJobDelivery(context.Background(), lease.DeliveryID)
			if err != nil {
				t.Fatalf("GetJobDelivery: %v", err)
			}
			if row.Status != tt.wantStatus {
				t.Fatalf("status=%q, want %q", row.Status, tt.wantStatus)
			}
			if row.LastError != tt.wantCode {
				t.Fatalf("last_error=%q, want %q", row.LastError, tt.wantCode)
			}
			if row.LastErrorMessage != tt.wantMessage {
				t.Fatalf("last_error_message=%q, want %q", row.LastErrorMessage, tt.wantMessage)
			}
			if tt.wantRetry {
				if row.NextAttemptAt == "" {
					t.Fatal("retryable DomainError must set next_attempt_at")
				}
				if row.CompletedAt != "" {
					t.Fatalf("retryable DomainError completed_at=%q, want empty", row.CompletedAt)
				}
				var lockedBy, leaseID, leaseExpiresAt string
				if err := db.DB().QueryRowContext(context.Background(), `
					SELECT COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, '')
					FROM job_deliveries WHERE delivery_id = ?`, lease.DeliveryID).
					Scan(&lockedBy, &leaseID, &leaseExpiresAt); err != nil {
					t.Fatalf("read retry lease columns: %v", err)
				}
				if lockedBy != "" || leaseID != "" || leaseExpiresAt != "" {
					t.Fatalf("retryable DomainError retained lease: locked_by=%q lease_id=%q expires=%q", lockedBy, leaseID, leaseExpiresAt)
				}
			} else {
				if row.CompletedAt == "" {
					t.Fatal("terminal DomainError must set completed_at")
				}
				if row.NextAttemptAt != "" {
					t.Fatalf("terminal DomainError next_attempt_at=%q, want empty", row.NextAttemptAt)
				}
				var lockedBy, leaseID, leaseExpiresAt string
				if err := db.DB().QueryRowContext(context.Background(), `
					SELECT COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, '')
					FROM job_deliveries WHERE delivery_id = ?`, lease.DeliveryID).
					Scan(&lockedBy, &leaseID, &leaseExpiresAt); err != nil {
					t.Fatalf("read terminal lease columns: %v", err)
				}
				if lockedBy != "" || leaseID != "" || leaseExpiresAt != "" {
					t.Fatalf("terminal DomainError retained lease: locked_by=%q lease_id=%q expires=%q", lockedBy, leaseID, leaseExpiresAt)
				}
			}
		})
	}
}
