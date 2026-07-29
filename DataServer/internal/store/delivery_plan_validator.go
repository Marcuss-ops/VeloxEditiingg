// Package store / delivery_plan_validator.go
//
// Per-destination validation helper for the atomic Job+Task creator.
// Extracted from atomic_job_task.go::insertDeliveryPlanTx so that:
//
//  1. atomic_job_task.go is a pure transaction runner (the file's
//     docstring explicitly states "only the transaction finalizer");
//     inserting-file inline is delegated to validateDeliveryDestinationTx.
//
//  2. The store-layer existence/globally-enabled check is unit-testable
//     in isolation — without having to construct a full Job + TaskSpec
//     + transaction, which the high-level CreateJobWithTask tests
//     already exercise.
//
//  3. Future canonical writers (e.g. completion.coordinator inserts
//     against job_delivery_plans during finalize) can reuse the same
//     guard without re-implementing the SELECT.
//
// Contract:
//   - Returns nil if the destination exists in delivery_destinations AND
//     enabled == 1.
//   - Returns "destination_id %q does not exist" when no row matches.
//   - Returns "destination_id %q is globally disabled" when enabled != 1.
//   - Surfaces the underlying driver error wrapped with the destination_id
//     for any other failure (e.g. connection lost mid-tx).
//
// The error strings are STABLE contract — atomic_job_task.go and the
// caller chain in enqueue/delivery_plan_validator.go rely on them for
// programmatic decisions. Any rename here must be reflected across
// callers and existing test assertions.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrDestinationDisabled is the typed sentinel returned by
// validateDeliveryDestinationTx when the row exists in
// delivery_destinations but its `enabled` column is 0. It is
// wrapped (via %w) on top of the STABLE contract string
// "destination_id %q is globally disabled" so:
//
//  1. legacy atomic_job_task_test.go assertions that match on
//     the substring (a plain `strings.Contains` / t.Fatalf
//     pattern) keep passing;
//  2. future resolver-layer callers can map this to the
//     structured envelope target_error_code=BLOCKED_VELOX_DISABLED
//     (defined at
//     velox-server/internal/handlers/server/pipeline/publishing_error_codes.go)
//     using errors.Is.
//
// The race-condition between the handler-layer pre-flight in
// job_submit.go (block reason already surfaces as
// BLOCKED_VELOX_DISABLED) and the tx-layer gate here (disabled
// flipped between pre-flight and INSERT) now maps to the SAME
// structured envelope, preserving diagnostic granularity in
// operator dashboards per the §0.3.4 item 4 split.
var ErrDestinationDisabled = errors.New("delivery destination disabled")

// ErrDestinationNotFound is the typed sentinel returned by
// validateDeliveryDestinationTx when no row matches the
// destination_id. Mirrors ErrDestinationDisabled's contract:
// the wrapped string ("destination_id %q does not exist")
// remains the STABLE contract; errors.Is enables structured
// mapping for future resolver-layer callers
// (target_error_code=DESTINATION_NOT_FOUND).
var ErrDestinationNotFound = errors.New("delivery destination not found")

// validateDeliveryDestinationTx checks that destID is an existing AND
// globally-enabled row in delivery_destinations. Runs inside the
// caller's transaction so the row it sees is consistent with the
// INSERT into job_delivery_plans performed by the caller.
//
// This is the store-layer half of the canonical-purity gate: the
// enqueue-layer validator (internal/jobs/enqueue/delivery_plan_validator.go)
// rejects malformed payload shapes BEFORE the enqueue path even reaches
// the atomic creator; validateDeliveryDestinationTx rejects destinations
// that no longer exist or were globally disabled BETWEEN payload
// validation and CREATE — i.e. it is the last line of defence against a
// Job becoming visible without a routable destination.
func validateDeliveryDestinationTx(ctx context.Context, tx *sql.Tx, destID string) error {
	var globallyEnabled int
	err := tx.QueryRowContext(ctx,
		`SELECT enabled FROM delivery_destinations WHERE destination_id = ?`,
		destID,
	).Scan(&globallyEnabled)
	if err == sql.ErrNoRows {
		// Stable contract string preserved; wrap ErrDestinationNotFound
		// via %w so resolver-layer callers can errors.Is(...) without
		// breaking the existing t.Fatalf string-match assertions in
		// atomic_job_task_test.go.
		return fmt.Errorf("%w: destination_id %q does not exist", ErrDestinationNotFound, destID)
	}
	if err != nil {
		return fmt.Errorf("validate destination_id %q: %w", destID, err)
	}
	if globallyEnabled != 1 {
		// Stable contract string preserved; wrap ErrDestinationDisabled
		// via %w so resolver-layer callers can errors.Is(...) without
		// breaking the existing t.Fatalf string-match assertions in
		// atomic_job_task_test.go.
		return fmt.Errorf("%w: destination_id %q is globally disabled", ErrDestinationDisabled, destID)
	}
	return nil
}
