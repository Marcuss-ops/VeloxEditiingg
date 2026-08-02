// Package completion / reconcile_supervisor.go
//
// Core of the ReconcileSupervisor (Fasi 4.1-4.3 of the Artifact
// Commit Protocol): the supervisor is a SELECT-only candidate scan
// the master walks every tick. The supervisor MUST NOT perform the
// transition itself; it identifies the candidate and calls
// Coordinator.ReconcileAttempt(commitID), which IS the writer
// (Fase 2.5-2.9 implementation).
//
// This file keeps the types (ReconcileCase / ReconcileAction /
// ReconcileCandidate / ReconcileMetrics), the struct, the
// constructor and the Run/TickOnce loop. The candidate scan
// (scanCandidates, the 11-case UNION of SQL signatures) lives in
// reconcile_scan.go; the dispatch/gc/isReconcileConflict helpers
// live in reconcile_dispatch.go.
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
	"log"
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
