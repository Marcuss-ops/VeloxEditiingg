package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"velox-shared/contract/domain"
)

type rowsAffectedFailureResult struct{}

func (rowsAffectedFailureResult) LastInsertId() (int64, error) { return 0, nil }
func (rowsAffectedFailureResult) RowsAffected() (int64, error) {
	return 0, sql.ErrConnDone
}

func TestReadRowsAffected_ClassifiesDriverFailure(t *testing.T) {
	got, err := readRowsAffected(rowsAffectedFailureResult{}, "transition")
	if got != 0 {
		t.Fatalf("readRowsAffected count = %d, want 0 on driver failure", got)
	}
	if err == nil {
		t.Fatal("readRowsAffected returned nil error for RowsAffected failure")
	}
	derr, ok := domain.AsDomainError(err)
	if !ok || derr.Code != domain.CodeInfrastructure {
		t.Fatalf("readRowsAffected error = %v, want infrastructure DomainError", err)
	}
	if !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("readRowsAffected error does not preserve driver cause: %v", err)
	}
}

func TestWrapDBInfrastructure_Nil(t *testing.T) {
	if got := wrapDBInfrastructure("store.query", nil); got != nil {
		t.Fatalf("wrapDBInfrastructure(nil) = %v, want nil", got)
	}
}

func TestWrapDBInfrastructure_PreservesDomainError(t *testing.T) {
	cause := errors.New("lease conflict")
	original := domain.NewLeaseLost(cause)

	got := wrapDBInfrastructure("store.update", original)
	if got != original {
		t.Fatalf("wrapDBInfrastructure changed an existing DomainError: got %p, want %p", got, original)
	}
	if derr, ok := domain.AsDomainError(got); !ok || derr.Code != domain.CodeLeaseLost {
		t.Fatalf("preserved error classification = %#v, want lease-lost DomainError", got)
	}
}

func TestWrapDBInfrastructure_PreservesWrappedDomainError(t *testing.T) {
	original := domain.NewLeaseLost(errors.New("lease conflict"))
	wrapped := fmt.Errorf("repository operation: %w", original)

	got := wrapDBInfrastructure("store.update", wrapped)
	if got != wrapped {
		t.Fatalf("wrapDBInfrastructure changed a wrapped DomainError: got %p, want %p", got, wrapped)
	}
	derr, ok := domain.AsDomainError(got)
	if !ok || derr.Code != domain.CodeLeaseLost {
		t.Fatalf("wrapped error classification = %#v, want lease-lost DomainError", got)
	}
}

func TestWrapDBInfrastructure_ClassifiesSQLConnDone(t *testing.T) {
	got := wrapDBInfrastructure("store.query", sql.ErrConnDone)
	derr, ok := domain.AsDomainError(got)
	if !ok {
		t.Fatalf("sql.ErrConnDone is not a DomainError: %v", got)
	}
	if derr.Code != domain.CodeInfrastructure {
		t.Fatalf("DomainError code = %q, want %q", derr.Code, domain.CodeInfrastructure)
	}
	if !errors.Is(got, sql.ErrConnDone) {
		t.Fatalf("wrapped error does not preserve sql.ErrConnDone: %v", got)
	}
}

func TestWrapDBInfrastructure_PreservesStoreSentinels(t *testing.T) {
	for name, sentinel := range map[string]error{
		"transition conflict":        ErrTransitionConflict,
		"lease lost":                 ErrLeaseLost,
		"delivery missing":           ErrDeliveryNoRow,
		"forwarding missing":         ErrCreatorForwardingNoRow,
		"publication missing":        ErrPublicationStateNotFound,
		"publication phase conflict": ErrPublicationPhaseConflict,
	} {
		t.Run(name, func(t *testing.T) {
			if got := wrapDBInfrastructure("store.operation", sentinel); got != sentinel {
				t.Fatalf("wrapDBInfrastructure changed sentinel: got %v, want %v", got, sentinel)
			}
		})
	}
}

func TestSQLiteStore_ClosedDBReturnsInfrastructure(t *testing.T) {
	db := setupDeliveryTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	_, err := db.Delivery().GetJobDelivery(context.Background(), "delivery-closed-db")
	if err == nil {
		t.Fatal("GetJobDelivery on closed DB returned nil error")
	}
	derr, ok := domain.AsDomainError(err)
	if !ok || derr.Code != domain.CodeInfrastructure {
		t.Fatalf("closed DB error = %v, want infrastructure DomainError", err)
	}
}

func TestWrapDBInfrastructure_ClassifiesUntypedError(t *testing.T) {
	cause := errors.New("sql: database is closed")

	got := wrapDBInfrastructure("store.LoadPending.query", cause)
	derr, ok := domain.AsDomainError(got)
	if !ok {
		t.Fatalf("wrapped database error is not a DomainError: %v", got)
	}
	if derr.Code != domain.CodeInfrastructure {
		t.Fatalf("DomainError code = %q, want %q", derr.Code, domain.CodeInfrastructure)
	}
	if !derr.Retryable {
		t.Fatal("infrastructure error must be retryable")
	}
	if !errors.Is(got, cause) {
		t.Fatalf("wrapped error does not preserve the database cause: %v", got)
	}
	if got.Error() != "infrastructure failure" {
		t.Fatalf("public error = %q, want canonical infrastructure text", got)
	}
}
