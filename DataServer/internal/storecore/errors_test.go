package storecore

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"velox-server/internal/taskgraph"
	"velox-shared/contract/domain"
)

type rowsAffectedFailureResult struct{}

func (rowsAffectedFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (rowsAffectedFailureResult) RowsAffected() (int64, error) {
	return 0, sql.ErrConnDone
}

func TestReadRowsAffected_ClassifiesDriverFailure(t *testing.T) {
	got, err := ReadRowsAffected(rowsAffectedFailureResult{}, "transition")
	if got != 0 {
		t.Fatalf("ReadRowsAffected count = %d, want 0 on driver failure", got)
	}
	if err == nil {
		t.Fatal("ReadRowsAffected returned nil error for RowsAffected failure")
	}
	derr, ok := domain.AsDomainError(err)
	if !ok || derr.Code != domain.CodeInfrastructure {
		t.Fatalf("ReadRowsAffected error = %v, want infrastructure DomainError", err)
	}
	if !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("ReadRowsAffected error does not preserve driver cause: %v", err)
	}
}

func TestWrapDBInfrastructure_Nil(t *testing.T) {
	if got := WrapDBInfrastructure("storecore.query", nil); got != nil {
		t.Fatalf("WrapDBInfrastructure(nil) = %v, want nil", got)
	}
}

func TestWrapDBInfrastructure_PreservesDomainError(t *testing.T) {
	original := domain.NewLeaseLost(errors.New("lease conflict"))
	if got := WrapDBInfrastructure("storecore.update", original); got != original {
		t.Fatalf("WrapDBInfrastructure changed an existing DomainError: got %p, want %p", got, original)
	}
}

func TestWrapDBInfrastructure_PreservesSentinels(t *testing.T) {
	for name, sentinel := range map[string]error{
		"transition conflict":        ErrTransitionConflict,
		"lease lost":                 ErrLeaseLost,
		"delivery missing":           ErrDeliveryNoRow,
		"forwarding missing":         ErrCreatorForwardingNoRow,
		"forwarding ownership":       ErrCreatorForwardingOwnershipConflict,
		"publication missing":        ErrPublicationStateNotFound,
		"publication phase conflict": ErrPublicationPhaseConflict,
		"task not found":             taskgraph.ErrTaskNotFound,
		"lease mismatch":             taskgraph.ErrLeaseMismatch,
	} {
		t.Run(name, func(t *testing.T) {
			if got := WrapDBInfrastructure("storecore.operation", sentinel); got != sentinel {
				t.Fatalf("WrapDBInfrastructure changed sentinel: got %v, want %v", got, sentinel)
			}
		})
	}
}

func TestWrapDBInfrastructure_ClassifiesUntypedError(t *testing.T) {
	cause := errors.New("sql: database is closed")
	got := WrapDBInfrastructure("storecore.query", cause)
	derr, ok := domain.AsDomainError(got)
	if !ok || derr.Code != domain.CodeInfrastructure {
		t.Fatalf("wrapped database error is not a DomainError: %v", got)
	}
	if !errors.Is(got, cause) {
		t.Fatalf("wrapped error does not preserve the database cause: %v", got)
	}
	if got.Error() != "infrastructure failure" {
		t.Fatalf("public error = %q, want canonical infrastructure text", got.Error())
	}
}

func TestLeaseConflictSentinelsAreLeaseLostMarkers(t *testing.T) {
	// The typed sentinels must satisfy the lease-lost marker so leaf policy
	// packages can recognize them without importing store.
	for name, sentinel := range map[string]error{
		"transition conflict": ErrTransitionConflict,
		"lease lost":          ErrLeaseLost,
	} {
		t.Run(name, func(t *testing.T) {
			marker, ok := sentinel.(interface{ LeaseLost() bool })
			if !ok {
				t.Fatalf("%s sentinel does not implement LeaseLost() bool", name)
			}
			if !marker.LeaseLost() {
				t.Fatalf("%s sentinel LeaseLost() = false, want true", name)
			}
		})
	}
}

func TestWrapDBInfrastructure_PreservesWrappedSentinel(t *testing.T) {
	// A sentinel wrapped with %w must still be recognized (errors.Is) and
	// returned unchanged by the classifier.
	wrapped := fmt.Errorf("repository transition: %w", ErrTransitionConflict)
	if got := WrapDBInfrastructure("storecore.transition", wrapped); got != wrapped {
		t.Fatalf("WrapDBInfrastructure changed a wrapped sentinel: got %p, want %p", got, wrapped)
	}
}
