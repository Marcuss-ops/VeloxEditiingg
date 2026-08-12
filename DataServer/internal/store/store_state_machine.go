package store

import (
	"errors"
	"fmt"
)

// store_state_machine.go is the SINGLE canonical authority for the two
// persistent status vocabularies owned by this package:
//
//   - deployment_records.status   (DeployStatus* — the deployment machine)
//   - fleet_operations.status     (OperationStatus* — the operation machine)
//
// Every persisted transition of either machine MUST satisfy its canonical
// rule. The enforcement points in this package are:
//
//   - updateDeploymentTerminal reads the current row, validates the
//     transition through ValidateDeploymentTransition, and fences the
//     UPDATE with the observed from-state (see store_deployment_records.go).
//   - MarkVerifiedSucceeded is the ONLY terminal-SUCCEEDED writer for
//     forward rollouts: it verifies the authenticated observed digest
//     against the row's target INSIDE the transition transaction and is the
//     only path that advances last_successful_digest (VERIFYING_DIGEST
//     enforcement).
//   - the fleet_operations MarkRunning / MarkSucceeded / MarkFailed
//     methods enforce the operation machine at the DB layer with
//     status-guarded UPDATEs (WHERE status = '<expected from>') — the
//     same table encoded in ValidateOperationTransition.
//
// Keeping the table in one function (instead of scattered WHERE clauses
// and per-caller switch statements) makes the legal vocabulary reviewable
// and testable in a single place. Anyone who adds a status, a transition,
// or a "resurrection" shortcut changes THIS file — and the matrix tests
// in store_state_machine_test.go pin it.
//
// Enforcement of the operation machine lives in the status-guarded UPDATEs
// of MarkRunning / MarkSucceeded / MarkFailed (store_fleet_operations.go);
// ValidateOperationTransition is the single spec those guards implement.
//
// Scope: this module formalizes the PERSISTED ledger vocabularies (the
// 4-state deployment machine and the 4-state operation machine). The
// phase-level rollout sequence (DRAINING → DEPLOYING → RESTARTING →
// WAITING_READY → VERIFYING_DIGEST) remains the UpdateExecutor's pipeline;
// migration 152 persists the CURRENT phase as an observability column
// (worker_deployment_state.last_phase) whose writer contract lives in
// store_worker_deployment_state.go — the phase is NOT a transition source,
// only a read model. The no-resurrection guarantee rests on terminality:
// any re-request is a NEW row, never a transition out of a terminal state.

// ErrIllegalDeploymentTransition is returned by the store when a
// deployment_records transition violates the canonical deployment machine
// (e.g. SUCCEEDED → FAILED, or any transition out of a terminal state).
var ErrIllegalDeploymentTransition = errors.New("deployment state machine: illegal transition")

// ErrIllegalOperationTransition is returned when a fleet_operations
// transition violates the canonical operation machine (e.g. QUEUED →
// SUCCEEDED, or any transition out of a terminal state).
var ErrIllegalOperationTransition = errors.New("fleet operation state machine: illegal transition")

// ── Deployment machine (deployment_records.status) ─────────────────────
//
//	                 ┌──────────→ SUCCEEDED      (terminal)
//	PENDING ─────────┼──────────→ FAILED         (terminal)
//	                 └──────────→ ROLLED_BACK    (terminal)
//
// Retries and rollbacks are NEW rows (InsertDeploymentRecord), never a
// transition out of a terminal state: the ledger is append-only, so an
// operation that ended cannot be resurrected. A re-request must produce a
// fresh PENDING row, preserving the terminal row as immutable audit
// history.
func ValidateDeploymentTransition(from, to string) error {
	switch from {
	case DeployStatusPending:
		switch to {
		case DeployStatusSucceeded, DeployStatusFailed, DeployStatusRolledBack:
			return nil
		}
	case DeployStatusSucceeded, DeployStatusFailed, DeployStatusRolledBack:
		// Terminal: no outgoing transitions, including self-transitions.
	default:
		return fmt.Errorf("%w: unknown source status %q", ErrIllegalDeploymentTransition, from)
	}
	return fmt.Errorf("%w: %s -> %s", ErrIllegalDeploymentTransition, from, to)
}

// IsDeploymentStatusTerminal reports whether a deployment status is
// terminal (SUCCEEDED, FAILED, ROLLED_BACK). Terminal statuses are
// immutable: ValidateDeploymentTransition rejects every transition out of
// them.
func IsDeploymentStatusTerminal(status string) bool {
	switch status {
	case DeployStatusSucceeded, DeployStatusFailed, DeployStatusRolledBack:
		return true
	default:
		return false
	}
}

// ── Operation machine (fleet_operations.status) ────────────────────────
//
//	QUEUED ──→ RUNNING ──┬──→ SUCCEEDED   (terminal)
//	                     └──→ FAILED      (terminal)
//
// A QUEUED row is claimed exactly once (MarkRunning returns whether the
// claim won); a RUNNING row completes to exactly one terminal status. As
// with the deployment machine, re-issuing an operation that already
// terminated is a NEW row — terminal rows are immutable.
func ValidateOperationTransition(from, to string) error {
	switch from {
	case OperationStatusQueued:
		if to == OperationStatusRunning {
			return nil
		}
	case OperationStatusRunning:
		switch to {
		case OperationStatusSucceeded, OperationStatusFailed:
			return nil
		}
	case OperationStatusSucceeded, OperationStatusFailed:
		// Terminal: no outgoing transitions, including self-transitions.
	default:
		return fmt.Errorf("%w: unknown source status %q", ErrIllegalOperationTransition, from)
	}
	return fmt.Errorf("%w: %s -> %s", ErrIllegalOperationTransition, from, to)
}

// IsOperationStatusTerminal reports whether a fleet operation status is
// terminal (SUCCEEDED, FAILED).
func IsOperationStatusTerminal(status string) bool {
	switch status {
	case OperationStatusSucceeded, OperationStatusFailed:
		return true
	default:
		return false
	}
}
