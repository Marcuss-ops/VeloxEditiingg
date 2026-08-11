package forwarding

import (
	"errors"
	"fmt"

	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// forwardingStateError preserves the distinction between a row-level CAS
// conflict and a store outage. The former is expected under lease races; the
// latter must reach FailureTracker so the forwarding runner can restart.
func forwardingStateError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrTransitionConflict) || errors.Is(err, store.ErrLeaseLost) {
		return supervisor.ErrLeaseLost
	}
	if errors.Is(err, supervisor.ErrInfrastructure) {
		return fmt.Errorf("%w: %s: %w", supervisor.ErrInfrastructure, operation, err)
	}
	return errors.Join(supervisor.ErrElementScoped, fmt.Errorf("%s: %w", operation, err))
}
