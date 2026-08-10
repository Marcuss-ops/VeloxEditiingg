package observability

import (
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// liveAttemptIsEligible is the single authority for live overlay precedence.
// Durable terminal state always wins over volatile worker_task_runtime state;
// live data is eligible only for a matching non-terminal attempt (or the
// claim-to-accept visibility window before that durable row exists).
func liveAttemptIsEligible(live *LiveAttempt, task *taskgraph.Task, attempts []taskattempts.TaskAttempt) bool {
	if live == nil || task == nil || task.Status.IsTerminal() || live.AttemptID == "" || live.AttemptNumber <= 0 {
		return false
	}

	// A runtime row is live only while its attempt is in an execution phase.
	// PARTITIONED_SUSPECTED is a disconnect signal, not progress: exposing it
	// as RUNNING would make a dead worker look active until the next retry.
	switch live.RuntimeStatus {
	case "ACCEPTED", "STARTING", "RUNNING", "CANCELLING", "UPLOADING", "FINALIZING":
		// Keep the canonical active execution states below.
	default:
		return false
	}
	// A worker-level partition/disconnect state invalidates the volatile
	// runtime row even if the last heartbeat payload still carried RUNNING.
	// The workers row is the canonical connection-state mirror used by the
	// recovery path, so this prevents stale progress from being presented as
	// active after a heartbeat stream stops entirely.
	switch live.WorkerConnectionState {
	case "", "CONNECTED":
		// Empty preserves compatibility with older adapters/fixtures that do
		// not expose the worker connection-state column.
	default:
		return false
	}

	latestAttemptNumber := 0
	for _, attempt := range attempts {
		if attempt.AttemptNumber > latestAttemptNumber {
			latestAttemptNumber = attempt.AttemptNumber
		}
	}

	// During the Claim→Accept visibility window the durable attempt list can
	// briefly lag the runtime row. Allow a newer live attempt through, but
	// never resurrect an older attempt after a retry has been created.
	if live.AttemptNumber < latestAttemptNumber || live.AttemptNumber < task.AttemptCount {
		return false
	}
	for _, attempt := range attempts {
		if attempt.ID == live.AttemptID {
			// Durable terminal state is strictly authoritative. This check
			// is deliberately independent of task status and row ordering.
			return !attempt.Status.IsTerminal()
		}
	}
	// A runtime row can become visible between claim/accept and durable
	// attempt persistence. Permit that narrow window, but never an older
	// attempt (guarded above by attempt number).
	return true
}
