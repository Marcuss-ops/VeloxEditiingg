package forwarding

import (
	"database/sql"
	"errors"
	"testing"

	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

func TestForwardingStateErrorKeepsInfrastructureVisible(t *testing.T) {
	err := forwardingStateError("mark retry", errors.Join(supervisor.ErrInfrastructure, sql.ErrConnDone))
	if !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("infrastructure classification was lost: %v", err)
	}
	if errors.Is(err, supervisor.ErrElementScoped) {
		t.Fatalf("infrastructure error was downgraded to element-scoped: %v", err)
	}
}

func TestForwardingStateErrorMapsCASConflictToLeaseLost(t *testing.T) {
	err := forwardingStateError("mark retry", store.ErrTransitionConflict)
	if !errors.Is(err, supervisor.ErrLeaseLost) {
		t.Fatalf("CAS conflict = %v, want ErrLeaseLost", err)
	}
	if errors.Is(err, supervisor.ErrElementScoped) {
		t.Fatalf("CAS conflict was classified as element-scoped: %v", err)
	}
}
