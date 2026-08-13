package completion

import (
	"context"
	"errors"
	"time"

	"velox-server/internal/logging"
)

// reconcile_dispatch.go owns the supervisor's post-scan work:
// dispatching candidates to Coordinator.ReconcileAttempt and
// translating the outcome into the action dimension, plus the
// seenIDs map GC and the conflict predicate. The scan itself
// lives in reconcile_scan.go; the types + loop live in
// reconcile_supervisor.go.

// dispatch calls Coordinator.ReconcileAttempt and translates the
// outcome into the action dimension. Errors are logged + escalated;
// noop / transition / escalate are the three action labels.
func (s *ReconcileSupervisor) dispatch(ctx context.Context, c ReconcileCandidate) {
	res, err := s.Coord.ReconcileAttempt(ctx, c.CommitID)
	if err != nil {
		// TransitionConflict means a concurrent writer raced us
		// ahead — treat as noop (the desired terminal state was
		// achieved, just not by us).
		if isReconcileConflict(err) {
			s.Metrics.IncReconcile(string(c.Case), string(ActionNoop))
			return
		}
		s.logWarn(ctx, logging.CodeCompletionReconcileDispatchFail, logging.F("commit_id", c.CommitID, "case", c.Case, "err", err))
		s.Metrics.IncReconcile(string(c.Case), string(ActionEscalate))
		return
	}
	// Successful dispatch: read the action from the result. The
	// coordinator's CommitResult surfaces JobStatus and TaskStatus;
	// any non-empty terminal status is "transition", empty is "noop"
	// (the row was already terminal).
	if res == nil || (res.TaskStatus == "" && res.JobStatus == "") {
		s.Metrics.IncReconcile(string(c.Case), string(ActionNoop))
		return
	}
	s.Metrics.IncReconcile(string(c.Case), string(ActionTransition))
}

// gcSeen trims the seenIDs map when it exceeds the cap.
func (s *ReconcileSupervisor) gcSeen() {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if len(s.seenIDs) > s.seenCap {
		s.seenIDs = make(map[string]time.Time, len(s.seenIDs)/2)
	}
}

// isReconcileConflict uses the typed completion sentinels. Reconcile must
// never infer a CAS race from a human-readable error message: arbitrary
// provider/database text must remain an escalation, not a silent no-op.
func isReconcileConflict(err error) bool {
	return errors.Is(err, ErrTransitionConflict) || errors.Is(err, ErrStaleReport)
}
