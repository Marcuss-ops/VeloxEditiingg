package storecore

// errors.go owns the shared persistence error infrastructure promoted out of
// the internal/store god-package: the CAS/lease sentinels, the typed
// lease-lost marker, and the database-failure classification helpers. Leaves
// (and the forwarding leaf this unblocks) depend on these directly instead of
// importing internal/store, while internal/store re-exports them for its
// existing call sites.

import (
	"database/sql"
	"errors"
	"fmt"

	"velox-server/internal/taskgraph"
	"velox-shared/contract/domain"
)

// leaseConflictError is a typed persistence boundary marker. It deliberately
// has no dependency on supervisor or transport packages; leaf policy packages
// can recognize a lease-lost condition without importing store.
type leaseConflictError string

func (e leaseConflictError) Error() string   { return string(e) }
func (e leaseConflictError) LeaseLost() bool { return true }

// Shared error sentinels. The message text retains the historical "store:"
// prefix so the observable error strings stay stable across the promotion.
var (
	// ErrTransitionConflict is returned when a CAS predicate does not match
	// (ExpectedStatus wrong OR Revision stale).
	ErrTransitionConflict error = leaseConflictError("store: job transition conflict (status or revision mismatch)")

	// ErrLeaseLost indicates that a lease-fenced operation no longer owns its
	// row. Callers must stop mutating and let the current lease holder continue.
	ErrLeaseLost error = leaseConflictError("store: forwarding lease lost")

	// ErrDeliveryNoRow is returned when a delivery lookup misses.
	ErrDeliveryNoRow = errors.New("store: delivery row not found")

	// ErrCreatorForwardingNoRow is returned when a forwarding lookup misses.
	ErrCreatorForwardingNoRow = errors.New("store: creator forwarding row not found")

	// ErrCreatorForwardingOwnershipConflict prevents a caller from reusing an
	// idempotency tuple already owned by another M2M client.
	ErrCreatorForwardingOwnershipConflict = errors.New("store: creator forwarding ownership conflict")

	// ErrPublicationStateNotFound is returned when a publication lookup misses.
	ErrPublicationStateNotFound = errors.New("store: publication state not found")

	// ErrPublicationPhaseConflict is returned when a publication phase effect
	// CAS does not match.
	ErrPublicationPhaseConflict = errors.New("store: publication phase effect conflict")
)

// ReadRowsAffected is the store boundary for driver-specific RowsAffected
// failures. A transition must never treat an unreadable row count as zero or
// as a successful CAS decision.
func ReadRowsAffected(result sql.Result, operation string) (int64, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, WrapDBInfrastructure(operation+" rows affected", err)
	}
	return affected, nil
}

// WrapDBInfrastructure converts an unexpected database failure at the store
// boundary into the canonical infrastructure DomainError.
//
// Store methods must handle expected outcomes first (for example,
// sql.ErrNoRows, a domain not-found sentinel, or a CAS conflict), then use
// this helper for the remaining database/driver error. The supervisor must
// not inspect driver error text, so classification belongs here at the
// adapter boundary.
//
// Existing DomainErrors are returned unchanged. This preserves their code,
// retry policy, sentinel identity, and complete unwrap chain instead of
// replacing a more specific domain classification with INFRASTRUCTURE.
func WrapDBInfrastructure(operation string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := domain.AsDomainError(err); ok || isExpectedStoreError(err) {
		return err
	}

	return domain.NewInfrastructure(fmt.Errorf("%s: %w", operation, err))
}

// isExpectedStoreError keeps store-level control-flow sentinels out of the
// infrastructure bucket. Callers should still handle these before invoking
// the wrapper when they need to map a result (for example, sql.ErrNoRows).
func isExpectedStoreError(err error) bool {
	for _, sentinel := range []error{
		ErrTransitionConflict,
		ErrLeaseLost,
		ErrDeliveryNoRow,
		ErrCreatorForwardingNoRow,
		ErrCreatorForwardingOwnershipConflict,
		ErrPublicationStateNotFound,
		ErrPublicationPhaseConflict,
		taskgraph.ErrTaskNotFound,
		taskgraph.ErrLeaseMismatch,
	} {
		if sentinel != nil && errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}
