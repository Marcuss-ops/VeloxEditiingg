package store

// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes — error sentinels and classification helpers delegate to storecore.

// db_errors.go — re-export + delegation of the shared error infrastructure
// promoted into internal/storecore. The sentinels and the
// ReadRowsAffected/WrapDBInfrastructure classification helpers now live in
// storecore so leaf repository packages (including the upcoming forwarding
// leaf) can depend on them without importing internal/store. The store
// god-package keeps the unqualified names and unexported helper signatures so
// its hundreds of call sites and errors.Is checks are unchanged.

import (
	"database/sql"

	"velox-server/internal/storecore"
)

// Shared error sentinels, re-exported with identity preserved so errors.Is
// checks against store.<Sentinel> and storecore.<Sentinel> target the same
// value.
var (
	ErrTransitionConflict                 = storecore.ErrTransitionConflict
	ErrLeaseLost                          = storecore.ErrLeaseLost
	ErrDeliveryNoRow                      = storecore.ErrDeliveryNoRow
	ErrCreatorForwardingNoRow             = storecore.ErrCreatorForwardingNoRow
	ErrCreatorForwardingOwnershipConflict = storecore.ErrCreatorForwardingOwnershipConflict
	ErrPublicationStateNotFound           = storecore.ErrPublicationStateNotFound
	ErrPublicationPhaseConflict           = storecore.ErrPublicationPhaseConflict
)

// readRowsAffected is the store boundary for driver-specific RowsAffected
// failures. A transition must never treat an unreadable row count as zero or
// as a successful CAS decision.
func readRowsAffected(result sql.Result, operation string) (int64, error) {
	return storecore.ReadRowsAffected(result, operation)
}

// wrapDBInfrastructure converts an unexpected database failure at the store
// boundary into the canonical infrastructure DomainError.
func wrapDBInfrastructure(operation string, err error) error {
	return storecore.WrapDBInfrastructure(operation, err)
}
