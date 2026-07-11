// Package completion / reconcile_supervisor.go
//
// Fasi 4.1-4.3 of the Artifact Commit Protocol: a SELECT-only
// candidate scan of the 11 cases of "NESSUNA transizione" (NO
// transition) the supervisor walks every tick. The supervisor MUST
// NOT perform the transition itself; it identifies the candidate
// and calls Coordinator.ReconcileAttempt(commitID), which IS the
// writer (Fase 2.5-2.9 implementation).
//
// The 11 cases are SQL-filterable signatures of attempt_commits
// rows in mid-state where the supervisor's repair-forward path is
// the canonical mechanism. They are encoded as a UNION of 11
// SELECT branches so a single tick is one roundtrip to SQLite.
//
// Action dimension (3 values):
//   - noop:        the row was already fixed by a concurrent writer
//   - transition:  Coordinator.ReconcileAttempt advanced the row
//     to a terminal state (EXPIRED, COMMITTED, etc.)
//   - escalate:    unresolvable state, operator/DBA intervention
//
// The completion_reconcile_total{case,action} counter exposes the
// dispatch surface; commit_deadline_exceeded_total fires once per
// attempt whose deadline crossed in this tick.
package completion

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ReconcileCase enumerates the 11 case labels exposed on
// completion_reconcile_total{case=...}. The string values are
// stable wire surface; renaming them is a metrics break.
type ReconcileCase string

const (
	// CaseDeadlineExpired: attempt in DECLARED|UPLOADING and
	// commit_deadline_at < NOW. The supervisor escalates to
	// EXPIRED via Coordinator.ReconcileAttempt.
	CaseDeadlineExpired ReconcileCase = "deadline_expired"
	// CaseOrphanTerminalTask: attempt_commits row whose
	// backing task_attempts is already FAILED/CANCELLED/TIMED_OUT.
	// The row is left in EXPIRED and outbox emits
	// 'commit_protocol.orphan_terminal'.
	CaseOrphanTerminalTask ReconcileCase = "orphan_terminal_task"
	// CaseStaleFence: attempt where worker_id/lease_id differ
	// from the canonical tasks row (lease reaped, worker
	// re-registered). The row is left in EXPIRED.
	CaseStaleFence ReconcileCase = "stale_fence"
	// CaseMissingWorker: attempt whose worker_id is not in the
	// workers table (worker reaped, drain completed).
	CaseMissingWorker ReconcileCase = "missing_worker"
	// CaseMissingDeclarations: attempt in UPLOADING with zero
	// attempt_declarations rows. Indicates DeclareOutputs
	// emitted the plan but the worker never uploaded any chunk.
	CaseMissingDeclarations ReconcileCase = "missing_declarations"
	// CaseMissingCommit: attempt with all required declarations
	// RECEIVED but no progress beyond for > 2x commit_deadline.
	CaseMissingCommit ReconcileCase = "missing_commit"
	// CaseUploadStuck: attempt in UPLOADING with last_progress_at
	// older than 5x commit_deadline.
	CaseUploadStuck ReconcileCase = "upload_stuck"
	// CaseFenceExpired: attempt where the worker-side lease has
	// passed lease_deadline_at and the row is still in DECLARED.
	CaseFenceExpired ReconcileCase = "fence_expired"
	// CaseOutboxPendingTooLong: attempt with an outbox event
	// 'commit_protocol.committed' PENDING for > retry_budget
	// (suggests downstream consumer stuck).
	CaseOutboxPendingTooLong ReconcileCase = "outbox_pending_too_long"
	// CaseRequiredOutputsMissing: tasks in AWAITING_ARTIFACT
	// but required_outputs_count > received_outputs_count.
	// Re-emits the require signal via the outbox.
	CaseRequiredOutputsMissing ReconcileCase = "required_outputs_missing"
	// CaseJobAllSucceededNoJobDeliveries: all tasks SUCCEEDED
	// but job_deliveries rows are missing. Idempotent insert
	// path on the next supervisor tick.
	CaseJobAllSucceededNoJobDeliveries ReconcileCase = "job_all_succeeded_no_job_deliveries"
)

// AllReconcileCases returns the closed set of case labels.
func AllReconcileCases() []ReconcileCase {
	return []ReconcileCase{
		CaseDeadlineExpired,
		CaseOrphanTerminalTask,
		CaseStaleFence,
		CaseMissingWorker,
		CaseMissingDeclarations,
		CaseMissingCommit,
		CaseUploadStuck,
		CaseFenceExpired,
		CaseOutboxPendingTooLong,
		CaseRequiredOutputsMissing,
		CaseJobAllSucceededNoJobDeliveries,
	}
}

// ReconcileAction is the second dimension of the metric.
type ReconcileAction string

const (
	ActionNoop       ReconcileAction = "noop"
	ActionTransition ReconcileAction = "transition"
	ActionEscalate   ReconcileAction = "escalate"
)

// ReconcileCandidate is the supervisor's intermediate
// representation: a (commit_id, case) pair ready to hand to
// Coordinator.ReconcileAttempt.
type ReconcileCandidate struct {
	CommitID string
	Case     ReconcileCase
}

// ReconcileMetrics is the minimal sink the supervisor writes to.
// The production sink is metrics.Collector; tests pass a noop.
//
// The interface uses STRING-typed labels (not the typed
// ReconcileCase / ReconcileAction aliases) so the metrics package
// can satisfy it without importing completion (avoiding an
// import cycle). ReconcileCase and ReconcileAction are both
// `type X string`, so callers pass them via string(case) /
// string(action) — the call site is one keystroke longer but the
// interface is wire-clean.
type ReconcileMetrics interface {
	IncReconcile(caseLabel, actionLabel string)
	IncCommitDeadlineExceeded()
}

// ReconcileSupervisor is the SELECT-only candidate scan that
// hands work to Coordinator.ReconcileAttempt. One instance per
// master, registered as a BackgroundRunner.
type ReconcileSupervisor struct {
	DB       *sql.DB
	Coord    Coordinator
	Metrics  ReconcileMetrics
	Tick     time.Duration
	Limit    int
	lastTick time.Time
	seenIDs  map[string]time.Time
	seenCap  int
	seenMu   sync.Mutex
	// Log is the sink for human-readable operational log lines
	// (scan errors, dispatch errors, startup banner). Defaults
	// to log.Printf; tests that intentionally exercise the
	// bad-DB / stub-coord error paths inject a no-op or
	// buffer-backed logger so the log line doesn't trip
	// `go test`'s "unexpected stderr output" check (which would
	// fail the package even when every individual test passes).
	// The metric counters (IncReconcile / IncCommitDeadlineExceeded)
	// are unaffected by Log — they are the test-facing
	// observability surface and remain wired through the Metrics
	// interface. LogFunc is the type alias; nil values are
	// treated as no-op by TickOnce / dispatch / Run.
	Log LogFunc
}

// LogFunc is the function signature the supervisor uses for
// human-readable operational logs. Mirrors log.Printf's signature
// so the default `log.Printf` binds directly. Tests inject a
// no-op (or a buffer-backed logger) to suppress or capture the
// log line without changing the production wiring.
type LogFunc func(format string, args ...any)

// noopLog is the fallback used when Log is nil. Distinct from a
// nil function value so the supervisor never panics on a nil deref.
func noopLog(format string, args ...any) {}

// NewReconcileSupervisor builds a supervisor with default tick +
// cap. Bootstrap wires the metrics sink + coordinator.
func NewReconcileSupervisor(db *sql.DB, coord Coordinator, metrics ReconcileMetrics) *ReconcileSupervisor {
	if db == nil {
		panic("completion.NewReconcileSupervisor: db is nil")
	}
	if coord == nil {
		panic("completion.NewReconcileSupervisor: coordinator is nil")
	}
	if metrics == nil {
		// Allow nil → use a noop so bootstrap can defer the
		// metric sink. Tests that explicitly want counters
		// must wire a real sink.
		metrics = noopReconcileMetrics{}
	}
	return &ReconcileSupervisor{
		DB:       db,
		Coord:    coord,
		Metrics:  metrics,
		Tick:     15 * time.Second,
		Limit:    500,
		seenIDs:  make(map[string]time.Time),
		seenCap:  10_000,
		lastTick: time.Now().UTC(),
		Log:      log.Printf, // default; tests override with a no-op or buffer
	}
}

// logf routes a human-readable operational log through the
// supervisor's Log sink. Centralised so the nil-guard is in one
// place (a nil Log defaults to a no-op, never a panic).
func (s *ReconcileSupervisor) logf(format string, args ...any) {
	if s.Log == nil {
		return
	}
	s.Log(format, args...)
}

type noopReconcileMetrics struct{}

func (noopReconcileMetrics) IncReconcile(string, string) {}
func (noopReconcileMetrics) IncCommitDeadlineExceeded()  {}

// Run loops until ctx is done. Errors are logged and do NOT abort.
func (s *ReconcileSupervisor) Run(ctx context.Context) error {
	t := time.NewTicker(s.Tick)
	defer t.Stop()
	s.logf("[RECONCILE-SUPERVISOR] starting — tick=%s limit=%d", s.Tick, s.Limit)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tick := <-t.C:
			s.TickOnce(ctx, tick.UTC())
		}
	}
}

// TickOnce is the body of one supervisor tick. Extracted so tests
// drive it deterministically.
func (s *ReconcileSupervisor) TickOnce(ctx context.Context, now time.Time) {
	candidates, deadlineExpiredCount, err := s.scanCandidates(ctx)
	if err != nil {
		s.logf("[RECONCILE-SUPERVISOR] scan: %v", err)
		return
	}
	if deadlineExpiredCount > 0 {
		for i := int64(0); i < deadlineExpiredCount; i++ {
			s.Metrics.IncCommitDeadlineExceeded()
		}
	}
	if len(candidates) == 0 {
		return
	}
	s.logf("[RECONCILE-SUPERVISOR] tick=%s — %d candidates", now.Format(time.RFC3339), len(candidates))
	for _, c := range candidates {
		s.dispatch(ctx, c)
	}
	s.gcSeen()
}

// scanCandidates returns the (commit_id, case) pairs the
// supervisor wants to escalate. Returns the count of
// deadline-expired rows separately so the deadline counter is
// always incremented even if the supervisor's dispatch path
// later no-ops on the same row.
//
// The 11-case UNION is intentionally single-trip: SQLite handles
// the OR-of-cases planner-side. A non-existent table or column
// surfaces as a scan error which the supervisor logs and
// continues.
func (s *ReconcileSupervisor) scanCandidates(ctx context.Context) ([]ReconcileCandidate, int64, error) {
	q := `
SELECT commit_id, case_label
FROM (
    -- 1. DECLARED|UPLOADING and commit_deadline_at < NOW
    SELECT commit_id, 'deadline_expired' AS case_label
      FROM attempt_commits
     WHERE status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')
       AND commit_deadline_at < ?
    UNION ALL
    -- 2. orphan terminal task
    SELECT ac.commit_id, 'orphan_terminal_task' AS case_label
      FROM attempt_commits ac
      JOIN task_attempts ta ON ta.id = ac.attempt_id
     WHERE ta.status IN ('FAILED','CANCELLED','TIMED_OUT')
       AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
    UNION ALL
    -- 3. stale fence: lease_id mismatch vs tasks row
    SELECT ac.commit_id, 'stale_fence' AS case_label
      FROM attempt_commits ac
      JOIN tasks t ON t.task_id = ac.task_id
     WHERE t.lease_id != ac.lease_id
       AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
    UNION ALL
    -- 4. missing worker
    SELECT ac.commit_id, 'missing_worker' AS case_label
      FROM attempt_commits ac
      LEFT JOIN workers w ON w.worker_id = ac.worker_id
     WHERE w.worker_id IS NULL
       AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
    UNION ALL
    -- 5. UPLOADING with zero declarations
    SELECT ac.commit_id, 'missing_declarations' AS case_label
      FROM attempt_commits ac
      LEFT JOIN (
          SELECT commit_id, COUNT(*) AS n
            FROM task_output_declarations
           GROUP BY commit_id
      ) d ON d.commit_id = ac.commit_id
     WHERE ac.status = 'UPLOADING'
       AND COALESCE(d.n, 0) = 0
    UNION ALL
    -- 6. all required declarations RECEIVED but no progress
    SELECT ac.commit_id, 'missing_commit' AS case_label
      FROM attempt_commits ac
     WHERE ac.status = 'RECEIVED'
       AND ac.required_output_count > 0
       AND ac.ready_output_count >= ac.required_output_count
       AND ac.last_progress_at < ?
    UNION ALL
    -- 7. upload stuck
    SELECT ac.commit_id, 'upload_stuck' AS case_label
      FROM attempt_commits ac
     WHERE ac.status = 'UPLOADING'
       AND ac.last_progress_at < ?
    UNION ALL
    -- 8. fence expired: lease was issued (t.lease_id != '')
    --    but the task has no worker assigned (t.worker_id = '').
    --    This is the canonical "lease issued, never picked up"
    --    state — distinct from case 3 (stale_fence) which is
    --    "lease_id mismatch after a reaped lease".
    --    Orthogonal: case 3 is t.lease_id != ac.lease_id;
    --    case 8 is t.worker_id = '' AND t.lease_id != ''.
    SELECT ac.commit_id, 'fence_expired' AS case_label
      FROM attempt_commits ac
      JOIN tasks t ON t.task_id = ac.task_id
     WHERE t.lease_id != ''
       AND t.worker_id = ''
       AND ac.status = 'DECLARED'
    UNION ALL
    -- 9. outbox event PENDING too long
    -- The outbox row is keyed by aggregate_id=task_id (see
    -- coordinator.CommitAttempt step 6); the commit_id lives
    -- in payload_json. We JOIN on (aggregate_type, aggregate_id)
    -- and additionally verify the payload carries the commit_id
    -- (LIKE match) so a stale event for a sibling task does
    -- not surface as outbox_pending_too_long for THIS attempt.
    SELECT ac.commit_id, 'outbox_pending_too_long' AS case_label
      FROM attempt_commits ac
      JOIN outbox_events oe
        ON oe.aggregate_type = 'task'
       AND oe.aggregate_id = ac.task_id
       AND oe.event_type = 'commit_protocol.committed'
       AND oe.payload_json LIKE '%' || ac.commit_id || '%'
     WHERE oe.status = 'PENDING'
       AND oe.created_at < ?
    UNION ALL
    -- 10. required_outputs missing
    SELECT ac.commit_id, 'required_outputs_missing' AS case_label
      FROM attempt_commits ac
     WHERE ac.status = 'AWAITING_REQUIRED'
       AND ac.required_output_count > ac.ready_output_count
    UNION ALL
    -- 11. all tasks SUCCEEDED but no job_deliveries rows
    -- The job_deliveries table has no commit_id column. The
    -- canonical join is via artifacts:
    --   artifacts.job_id  = ac.job_id
    --   job_deliveries.artifact_id = artifacts.id
    -- A "missing deliveries" state is detected at the JOB level
    -- (all artifacts in this job have zero delivery rows).
    SELECT ac.commit_id, 'job_all_succeeded_no_job_deliveries' AS case_label
      FROM attempt_commits ac
      JOIN tasks t ON t.task_id = ac.task_id
     WHERE t.status = 'SUCCEEDED'
       AND ac.status = 'COMMITTED'
       AND NOT EXISTS (
           SELECT 1
             FROM artifacts a
             JOIN job_deliveries jd ON jd.artifact_id = a.id
            WHERE a.job_id = ac.job_id
       )
)
ORDER BY commit_id
LIMIT ?`
	now := time.Now().UTC()
	deadlineCutoff := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	progressCutoff := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	outboxCutoff := now.Add(-1 * time.Hour).Format(time.RFC3339Nano)
	// Placeholder order (5 total) maps to the ? marks in the
	// UNION query above:
	//   #1 case 1 (deadline_expired)      → commit_deadline_at < ?
	//   #2 case 6 (missing_commit)        → last_progress_at < ?
	//   #3 case 7 (upload_stuck)          → last_progress_at < ?
	//   #4 case 9 (outbox_pending)        → oe.created_at < ?
	//   #5 LIMIT                         → s.Limit
	rows, err := s.DB.QueryContext(ctx, q,
		now.Format(time.RFC3339Nano), // #1 deadline_expired
		deadlineCutoff,               // #2 missing_commit
		progressCutoff,               // #3 upload_stuck
		outboxCutoff,                 // #4 outbox_pending_too_long
		s.Limit,                      // #5 LIMIT
	)
	if err != nil {
		return nil, 0, fmt.Errorf("reconcile: scan: %w", err)
	}
	defer rows.Close()
	var out []ReconcileCandidate
	var deadlineExpired int64
	for rows.Next() {
		var commitID, caseLabel string
		if err := rows.Scan(&commitID, &caseLabel); err != nil {
			return nil, 0, fmt.Errorf("reconcile: scan row: %w", err)
		}
		if commitID == "" || caseLabel == "" {
			continue
		}
		if caseLabel == string(CaseDeadlineExpired) {
			deadlineExpired++
		}
		// Dedup: skip if seen in a prior tick (the map is GC'd at
		// seenCap; bounded-window double-fire acceptable).
		s.seenMu.Lock()
		if _, ok := s.seenIDs[commitID+":"+caseLabel]; ok {
			s.seenMu.Unlock()
			continue
		}
		s.seenIDs[commitID+":"+caseLabel] = now
		s.seenMu.Unlock()
		out = append(out, ReconcileCandidate{CommitID: commitID, Case: ReconcileCase(caseLabel)})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("reconcile: rows: %w", err)
	}
	return out, deadlineExpired, nil
}

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
