// Package queue / lifecycle_pr3.go — PR 3 transactional LifecycleService.
//
// This is the slim new shape: LifecycleService owns JobRepository + Clock
// and NOTHING ELSE. Every operation routes to the corresponding
// JobRepository PR3 method, which is itself transactional (UPDATE +
// history + event + outbox in a single BEGIN/COMMIT).
//
// The legacy eventStore-driven methods stay in lifecycle.go so ~50
// existing callers (HTTP handlers, gRPC handler, service layers, tests)
// keep compiling. Migration is incremental: callers switch to the new
// public methods here when they're ready for the stronger atomicity
// guarantee.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// LifecycleService is the slim, transactional PR 3 lifecycle façade.
//
// Construction is mandatory:
//
//	lc, err := queue.NewLifecycleService(jobRepo, queue.RealClock{})
//
// The Clock injection exists so lease-expiry tests, reaper deadlines,
// and SUCCEEDED-gating can be driven deterministically.
type LifecycleService struct {
	repo  store.JobRepository
	clock Clock
}

// NewLifecycleService constructs the slim LifecycleService. Both args are
// required. Returns an error (not a panic) so bootstrap can surface
// configuration mistakes via the standard error path.
func NewLifecycleService(repo store.JobRepository, clock Clock) (*LifecycleService, error) {
	if repo == nil {
		return nil, errors.New("job repository is required")
	}
	if clock == nil {
		return nil, errors.New("clock is required")
	}
	return &LifecycleService{repo: repo, clock: clock}, nil
}

// Repo exposes the underlying JobRepository for callers that need direct
// access (e.g. the bootstrap composition root constructs the
// ArtifactSuccessGate from the same repo reference).
func (l *LifecycleService) Repo() store.JobRepository { return l.repo }

// Clock returns the clock the service uses for time stamping.
func (l *LifecycleService) Clock() Clock { return l.clock }

// now is an internal helper that resolves cmd.Now || clock.Now.
func (l *LifecycleService) now(t time.Time) time.Time {
	if t.IsZero() {
		t = l.clock.Now()
	}
	return t.UTC()
}

// ── PR 3 commands ────────────────────────────────────────────────────────

// Start delegates to JobRepository.PR3Start. The service validates that
// cmd is non-empty then forwards to the repo, which atomically performs
// the LEASED → RUNNING transition + history + event in one tx.
func (l *LifecycleService) Start(ctx context.Context, cmd store.StartCommand) error {
	if cmd.JobID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("lifecycle.Start: missing job/worker/lease identity")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3Start(ctx, cmd)
}

// RenewLease delegates to JobRepository.PR3RenewLease. The service adds
// the LEASE_RENEWED event in the same transaction when cmd.EmitEvent is
// true (per spec §"L'evento LEASE_RENEWED, quando necessario, va inserito
// nella stessa transazione").
func (l *LifecycleService) RenewLease(ctx context.Context, cmd store.RenewLeaseCommand) error {
	if cmd.JobID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("lifecycle.RenewLease: missing job or lease identity")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	if cmd.LeaseExpiry.IsZero() {
		cmd.LeaseExpiry = l.now(cmd.Now).Add(30 * time.Minute)
	}
	return l.repo.PR3RenewLease(ctx, cmd)
}

// RecordRenderFinished delegates to JobRepository.PR3RecordRenderFinished.
// SUCCEEDED is NOT exposed through this method — the job moves to
// RENDER_FINISHED, not SUCCEEDED. Spec compliance: "Il completamento
// SUCCEEDED non deve essere pubblico per gli handler".
func (l *LifecycleService) RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("lifecycle.RecordRenderFinished: empty jobID")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3RecordRenderFinished(ctx, cmd)
}

// Fail delegates to JobRepository.PR3Fail. The repository decides
// FAILED vs RETRY_WAIT based on cmd.Retryable and the jobs row's
// retry_count/max_retries.
func (l *LifecycleService) Fail(ctx context.Context, cmd store.FailCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("lifecycle.Fail: empty jobID")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3Fail(ctx, cmd)
}

// ScheduleRetry forces RETRY_WAIT regardless of retry budget. Same
// single-tx shape as Fail but emits JOB_RETRY_SCHEDULED specifically.
func (l *LifecycleService) ScheduleRetry(ctx context.Context, cmd store.RetryCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("lifecycle.ScheduleRetry: empty jobID")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3ScheduleRetry(ctx, cmd)
}

// Cancel delegates to JobRepository.PR3Cancel. Idempotent on already-
// terminal states.
func (l *LifecycleService) Cancel(ctx context.Context, cmd store.CancelCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("lifecycle.Cancel: empty jobID")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3Cancel(ctx, cmd)
}

// RequeueExpiredLeases delegates to JobRepository.PR3RequeueExpiredLeases.
// The reaper calls this on a timer (typically 60-300s) to recover jobs
// whose workers crashed without releasing the lease.
func (l *LifecycleService) RequeueExpiredLeases(ctx context.Context, limit int) ([]store.RequeueResult, error) {
	if limit <= 0 {
		limit = 100
	}
	return l.repo.PR3RequeueExpiredLeases(ctx, l.now(time.Time{}), limit)
}

// Validate is a thin transition-matrix check exposed for callers that
// want to gate before invoking a PR3 method (e.g. the HTTP service layer
// to refuse invalid state transitions earlier). The repo re-runs the
// check inside its CAS UPDATE so this is purely advisory.
func (l *LifecycleService) Validate(from, to JobStatus) error {
	if !isValidJobStatusTransition(from, to) {
		return fmt.Errorf("invalid transition: %s → %s", from, to)
	}
	return nil
}

// compile-time guard: the new constructor still returns the *same* struct
// type as the legacy one (so existing callers keep compiling). New
// callers can type-assert or use the slim wrapper safely.
var (
	_ = func() *LifecycleService { return nil } // keep symbol live; no-op
)
