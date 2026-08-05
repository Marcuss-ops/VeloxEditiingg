package completion

import (
	"context"
	"log"
	"sync"
	"time"

	"velox-server/internal/store"
)

type ReconcileCase string

const (
	CaseDeadlineExpired                ReconcileCase = "deadline_expired"
	CaseOrphanTerminalTask             ReconcileCase = "orphan_terminal_task"
	CaseStaleFence                     ReconcileCase = "stale_fence"
	CaseMissingWorker                  ReconcileCase = "missing_worker"
	CaseMissingDeclarations            ReconcileCase = "missing_declarations"
	CaseMissingCommit                  ReconcileCase = "missing_commit"
	CaseUploadStuck                    ReconcileCase = "upload_stuck"
	CaseFenceExpired                   ReconcileCase = "fence_expired"
	CaseOutboxPendingTooLong           ReconcileCase = "outbox_pending_too_long"
	CaseRequiredOutputsMissing         ReconcileCase = "required_outputs_missing"
	CaseJobAllSucceededNoJobDeliveries ReconcileCase = "job_all_succeeded_no_job_deliveries"
)

func AllReconcileCases() []ReconcileCase {
	return []ReconcileCase{CaseDeadlineExpired, CaseOrphanTerminalTask, CaseStaleFence, CaseMissingWorker, CaseMissingDeclarations, CaseMissingCommit, CaseUploadStuck, CaseFenceExpired, CaseOutboxPendingTooLong, CaseRequiredOutputsMissing, CaseJobAllSucceededNoJobDeliveries}
}

type ReconcileAction string

const (
	ActionNoop       ReconcileAction = "noop"
	ActionTransition ReconcileAction = "transition"
	ActionEscalate   ReconcileAction = "escalate"
)

type ReconcileCandidate struct {
	CommitID string
	Case     ReconcileCase
}
type ReconcileMetrics interface {
	IncReconcile(caseLabel, actionLabel string)
	IncCommitDeadlineExceeded()
}

type ReconcileSupervisor struct {
	Store    store.CompletionStore
	Coord    Coordinator
	Metrics  ReconcileMetrics
	Tick     time.Duration
	Limit    int
	lastTick time.Time
	seenIDs  map[string]time.Time
	seenCap  int
	seenMu   sync.Mutex
	Log      LogFunc
}

type LogFunc func(format string, args ...any)

func noopLog(format string, args ...any) {}

type noopReconcileMetrics struct{}

func (noopReconcileMetrics) IncReconcile(string, string) {}
func (noopReconcileMetrics) IncCommitDeadlineExceeded()  {}

func NewReconcileSupervisor(completionStore store.CompletionStore, coord Coordinator, metrics ReconcileMetrics) *ReconcileSupervisor {
	if completionStore == nil {
		panic("completion.NewReconcileSupervisor: store is nil")
	}
	if coord == nil {
		panic("completion.NewReconcileSupervisor: coordinator is nil")
	}
	if metrics == nil {
		metrics = noopReconcileMetrics{}
	}
	return &ReconcileSupervisor{Store: completionStore, Coord: coord, Metrics: metrics, Tick: 15 * time.Second, Limit: 500, seenIDs: make(map[string]time.Time), seenCap: 10000, lastTick: time.Now().UTC(), Log: log.Printf}
}

func (s *ReconcileSupervisor) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log(format, args...)
	}
}
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
func (s *ReconcileSupervisor) TickOnce(ctx context.Context, now time.Time) {
	candidates, deadline, err := s.scanCandidates(ctx)
	if err != nil {
		s.logf("[RECONCILE-SUPERVISOR] scan: %v", err)
		return
	}
	for i := int64(0); i < deadline; i++ {
		s.Metrics.IncCommitDeadlineExceeded()
	}
	if len(candidates) == 0 {
		return
	}
	s.logf("[RECONCILE-SUPERVISOR] tick=%s — %d candidates", now.Format(time.RFC3339), len(candidates))
	for _, candidate := range candidates {
		s.dispatch(ctx, candidate)
	}
	s.gcSeen()
}
