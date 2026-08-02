package completion

import (
	"context"
	"strings"
	"time"
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
		s.logf("[RECONCILE-SUPERVISOR] dispatch %s (%s): %v", c.CommitID, c.Case, err)
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

// isReconcileConflict uses substring matching to avoid an import
// cycle on the sentinel errors defined in types.go. The wording
// is part of the wire contract.
func isReconcileConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "transition conflict") || strings.Contains(msg, "stale report")
}
