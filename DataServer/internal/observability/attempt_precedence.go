package observability

import (
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// attemptOverlayDecision is the result of the one reconciliation authority
// used by every observability projection. A nil durable pointer means the
// live row is a temporary claim/accept overlay only; it never becomes durable
// history and never overrides a terminal task.
type attemptOverlayDecision struct {
	durable  *taskattempts.TaskAttempt
	eligible bool
}

// reconcileLiveAttempt is the single authority for live/durable precedence.
// Durable terminal state always wins over volatile worker_task_runtime state.
// It also owns task/attempt identity, retry ordering, and worker liveness
// checks so callers cannot implement subtly different reconciliation rules.
func reconcileLiveAttempt(live *LiveAttempt, task *taskgraph.Task, attempts []taskattempts.TaskAttempt) attemptOverlayDecision {
	decision := attemptOverlayDecision{}
	if live == nil || task == nil || task.Status.IsTerminal() || live.AttemptID == "" || live.AttemptNumber <= 0 {
		return decision
	}
	// Keep identity validation in the authority, rather than requiring each
	// projection caller to filter the volatile reader independently. Empty
	// identity fields remain compatible with older adapters and fixtures.
	if (live.TaskID != "" && live.TaskID != task.ID) || (live.JobID != "" && live.JobID != task.JobID) {
		return decision
	}

	// A runtime row is live only while its attempt is in an execution phase.
	// PARTITIONED_SUSPECTED is a disconnect signal, not progress.
	switch live.RuntimeStatus {
	case "ACCEPTED", "STARTING", "RUNNING", "CANCELLING", "UPLOADING", "FINALIZING":
	default:
		return decision
	}
	// A worker-level partition/disconnect state invalidates the volatile row
	// even when its last heartbeat still carried RUNNING.
	switch live.WorkerConnectionState {
	case "", "CONNECTED":
		// Empty preserves compatibility with older adapters/fixtures.
	default:
		return decision
	}

	latestAttemptNumber := 0
	for i := range attempts {
		if attempts[i].AttemptNumber > latestAttemptNumber {
			latestAttemptNumber = attempts[i].AttemptNumber
		}
	}
	// Never resurrect an older attempt after a retry has been created.
	if live.AttemptNumber < latestAttemptNumber || live.AttemptNumber < task.AttemptCount {
		return decision
	}
	for i := range attempts {
		if attempts[i].ID != live.AttemptID {
			continue
		}
		// An attempt ID is canonical identity. If a reader returns an
		// attempt with the right ID but the wrong task/job/ordinal, reject
		// the row rather than treating it as a claim visibility window.
		if attempts[i].TaskID != "" && attempts[i].TaskID != task.ID {
			return decision
		}
		if attempts[i].JobID != "" && attempts[i].JobID != task.JobID {
			return decision
		}
		if attempts[i].AttemptNumber > 0 && attempts[i].AttemptNumber != live.AttemptNumber {
			return decision
		}
		decision.durable = &attempts[i]
		// A durable terminal row is an absorbing authority: no live field
		// may overwrite it, regardless of the order returned by the reader.
		decision.eligible = !attempts[i].Status.IsTerminal()
		return decision
	}

	// Claim/accept can publish the volatile identity just before the durable
	// row is visible (or an adapter can briefly lag the durable reader). Permit
	// this temporary overlay, but never promote it to durable state or use it
	// to override a terminal task.
	decision.eligible = true
	return decision
}

// overlaysAttempt reports whether the reconciled live row may enrich the
// durable attempt with volatile progress. Keeping this predicate on the
// decision prevents projections from reimplementing identity/terminal rules.
func (d attemptOverlayDecision) overlaysAttempt(attemptID string) bool {
	return d.eligible && d.durable != nil && d.durable.ID == attemptID
}

// hasTemporaryOverlay reports the claim/accept window where no durable row
// matched yet. The caller may expose the live row, but only as an explicitly
// temporary projection.
func (d attemptOverlayDecision) hasTemporaryOverlay() bool {
	return d.eligible && d.durable == nil
}

// liveAttemptIsEligible remains a small compatibility wrapper for focused
// callers and tests. All reconciliation decisions flow through the helper
// above; there is no second precedence implementation.
func liveAttemptIsEligible(live *LiveAttempt, task *taskgraph.Task, attempts []taskattempts.TaskAttempt) bool {
	return reconcileLiveAttempt(live, task, attempts).eligible
}
