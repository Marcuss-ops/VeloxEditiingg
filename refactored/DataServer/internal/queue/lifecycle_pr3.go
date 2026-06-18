// Package queue / lifecycle_pr3.go — PR 3 transactional LifecycleService.
//
// New PR3-specific methods that route to the corresponding
// JobRepository PR3 methods. Each is itself transactional (UPDATE +
// history + event + outbox in a single BEGIN/COMMIT).
//
// The LifecycleService struct and legacy methods live in lifecycle.go.
package queue

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// NewLifecycleService constructs the slim LifecycleService for PR 3 callers.
// Both args are required. Returns an error (not a panic) so bootstrap can
// surface configuration mistakes via the standard error path.
func NewLifecycleService(repo store.JobRepository, clock Clock) (*LifecycleService, error) {
	if repo == nil {
		return nil, fmt.Errorf("job repository is required")
	}
	if clock == nil {
		return nil, fmt.Errorf("clock is required")
	}
	return &LifecycleService{repo: repo, clock: clock, jobRepo: repo}, nil
}

// Repo exposes the underlying JobRepository for callers that need direct
// access (e.g. the bootstrap composition root constructs the
// ArtifactSuccessGate from the same repo reference).
func (l *LifecycleService) Repo() store.JobRepository { return l.repo }

// Clock returns the clock the service uses for time stamping.
func (l *LifecycleService) Clock() Clock { return l.clock }

// now is an internal helper that resolves cmd.Now || clock.Now.
func (l *LifecycleService) now(t time.Time) time.Time {
	if t.IsZero() && l.clock != nil {
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
