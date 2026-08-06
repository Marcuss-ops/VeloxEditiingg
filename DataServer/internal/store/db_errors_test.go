package store

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"velox-shared/contract/domain"
)

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
