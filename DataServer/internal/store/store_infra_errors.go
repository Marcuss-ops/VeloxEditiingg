package store

// store_infra_errors.go — the SQLite adapter boundary classifier.
//
// Supervisor/restart policy (internal/supervisor/policy.go and its tests)
// is explicit: it NEVER parses Error() text. Adapters MUST wrap driver
// failures in domain.NewInfrastructure at their boundary so a closed DB
// escalates as ErrInfrastructure instead of silently degrading to an
// element-scoped error (P0-02). This file owns that classification for
// the store package.
//
// database/sql exports sql.ErrConnDone but deliberately keeps the
// "sql: database is closed" sentinel private, so the adapter — which owns
// the driver relationship — detects it by the driver's stable message
// here, never in policy code.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-shared/contract/domain"
)

// wrapInfrastructureError wraps a driver-level error, classifying known
// infrastructure failures (closed DB, conn done, deadline) into the
// canonical typed DomainError. Operation names the store operation for
// the error message; the original cause stays reachable via errors.Is.
func wrapInfrastructureError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if isInfrastructureDriverError(err) {
		return fmt.Errorf("%s: %w", operation, domain.NewInfrastructure(err))
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// isInfrastructureDriverError reports whether err chains to a known
// driver-level infrastructure failure. The chain walk covers the %w
// single-wrap form used across the store package.
func isInfrastructureDriverError(err error) bool {
	if errors.Is(err, sql.ErrConnDone) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if e.Error() == "sql: database is closed" {
			return true
		}
	}
	return false
}
