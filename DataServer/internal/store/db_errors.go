package store

import (
	"fmt"

	"velox-shared/contract/domain"
)

// wrapDBInfrastructure converts an unexpected database failure at the store
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
func wrapDBInfrastructure(operation string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := domain.AsDomainError(err); ok {
		return err
	}

	return domain.NewInfrastructure(fmt.Errorf("%s: %w", operation, err))
}
