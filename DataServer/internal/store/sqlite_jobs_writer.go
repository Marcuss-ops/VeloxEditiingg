package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
)

// SQLiteJobRepository implements jobs.Repository against *SQLiteStore.
//
// Writer methods (Lease, Start, RenewLease, Fail, FailWithRetry, Cancel,
// RequeueExpiredLeases, ReleaseLease, RecordRenderFinished, SetStatus,
// Delete) and Reader methods (Get, List, Counts) are inherited from the
// embedded baseJobRepository.
//
// ClaimNext and ClaimNextForProfile are NOT shared because they
// delegate to *SQLiteStore CAS helpers that are unique to SQLite.
type SQLiteJobRepository struct {
	baseJobRepository
	store *SQLiteStore
}

var _ jobs.Repository = (*SQLiteJobRepository)(nil)

// NewSQLiteJobRepository wraps a SQLiteStore as a jobs.Repository.
func NewSQLiteJobRepository(store *SQLiteStore) *SQLiteJobRepository {
	return &SQLiteJobRepository{
		baseJobRepository: baseJobRepository{
			db:      store.db,
			dialect: sqliteDialect{store: store},
		},
		store: store,
	}
}

// NewJobsRepository returns the canonical jobs.Repository.
func NewJobsRepository(repo *SQLiteJobRepository) jobs.Repository { return repo }

// ── ClaimNext (SQLite-specific CAS helpers) ──────────────────────────

// claimNext delegates to the well-tested ClaimNextPendingJob.
func (r *SQLiteJobRepository) claimNext(ctx context.Context, claim ClaimParams) (*ClaimResult, error) {
	if claim.WorkerID == "" {
		return nil, fmt.Errorf("job repository: claim with empty workerID")
	}
	now := claim.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	resultJSON, claimedReq, ok, err := r.store.ClaimNextPendingJob(claim.WorkerID, claim.AllowedJobTypes, now)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	if !ok {
		return nil, ErrNoClaimableJob
	}
	out := &ClaimResult{ResultJSON: append([]byte(nil), resultJSON...), Requirements: claimedReq}
	var parsed ClaimResultJSON
	if err := json.Unmarshal(resultJSON, &parsed); err != nil {
		return nil, fmt.Errorf("claim result unmarshal: %w", err)
	}
	if parsed.JobID != "" {
		out.JobID = parsed.JobID
	}
	if parsed.LeaseID != "" {
		out.LeaseID = parsed.LeaseID
	}
	out.Attempt = int(parsed.Attempt)
	if parsed.LeaseExpiry != "" {
		if t, perr := time.Parse(time.RFC3339, parsed.LeaseExpiry); perr == nil {
			out.LeaseExpires = t
		}
	}
	return out, nil
}

// ClaimNext atomically claims the next PENDING job for a worker.
func (r *SQLiteJobRepository) ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*jobs.ClaimNextResult, error) {
	result, err := r.claimNext(ctx, ClaimParams{WorkerID: workerID, AllowedJobTypes: allowedJobTypes})
	if err != nil {
		if errors.Is(err, ErrNoClaimableJob) {
			return nil, err
		}
		return nil, fmt.Errorf("claim next: %w", err)
	}
	return &jobs.ClaimNextResult{
		JobID: result.JobID, Attempt: result.Attempt, LeaseID: result.LeaseID,
		LeaseExpires: result.LeaseExpires, Requirements: result.Requirements,
	}, nil
}

// ClaimNextForProfile is the cost-rank sibling of ClaimNext.
func (r *SQLiteJobRepository) ClaimNextForProfile(
	ctx context.Context, workerID string, allowedJobTypes []string,
	profile costmodel.WorkerProfile, maxCandidates int,
) (*jobs.ClaimNextResult, error) {
	resultJSON, claimedReq, ok, err := r.store.ClaimNextPendingJobForWorker(
		ctx, workerID, allowedJobTypes, profile, maxCandidates, time.Time{})
	if err != nil {
		if errors.Is(err, ErrNoClaimableJob) {
			return nil, err
		}
		return nil, fmt.Errorf("claim next for profile: %w", err)
	}
	if !ok {
		return nil, ErrNoClaimableJob
	}
	out := &jobs.ClaimNextResult{Requirements: claimedReq}
	var parsed ClaimResultJSON
	if err := json.Unmarshal(resultJSON, &parsed); err == nil {
		if parsed.JobID != "" {
			out.JobID = parsed.JobID
		}
		if parsed.LeaseID != "" {
			out.LeaseID = parsed.LeaseID
		}
		out.Attempt = int(parsed.Attempt)
		if parsed.LeaseExpiry != "" {
			if t, perr := time.Parse(time.RFC3339, parsed.LeaseExpiry); perr == nil {
				out.LeaseExpires = t
			}
		}
	}
	return out, nil
}

// ── Legacy helpers (kept for compat) ────────────────────────────────

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Transition wraps *SQLiteStore.TransitionJobStatus (kept for SetStatus compat).
func (r *SQLiteJobRepository) Transition(ctx context.Context, t TransitionParams) error {
	if t.JobID == "" {
		return fmt.Errorf("job repository: empty jobID")
	}
	_, err := r.store.TransitionJobStatus(ctx, t.JobID, string(t.ExpectedStatus), string(t.NewStatus), t.Revision)
	if err != nil {
		if errors.Is(err, ErrTransitionConflict) {
			return ErrTransitionConflict
		}
		return fmt.Errorf("transition: %w", err)
	}
	return nil
}

// ── PR3 methods (kept for backward compat, delegate to base) ────────

func (r *SQLiteJobRepository) nowStr(cmdTime time.Time) string {
	if !cmdTime.IsZero() {
		return cmdTime.UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

// PR3Start performs the LEASED → RUNNING transition via the base.
func (r *SQLiteJobRepository) PR3Start(ctx context.Context, cmd StartCommand) error {
	return r.Start(ctx, cmd.JobID, cmd.WorkerID, cmd.LeaseID, cmd.Attempt, cmd.ExpectedRevision)
}

// PR3RenewLease extends the lease via the base.
func (r *SQLiteJobRepository) PR3RenewLease(ctx context.Context, cmd RenewLeaseCommand) error {
	return r.renewLease(ctx, cmd.JobID, cmd.WorkerID, cmd.LeaseID, cmd.LeaseExpiry, cmd.EmitEvent, cmd.ExpectedRevision, cmd.SkipRevisionCAS)
}

// PR3RecordRenderFinished marks RENDER_FINISHED via the base.
func (r *SQLiteJobRepository) PR3RecordRenderFinished(ctx context.Context, cmd RecordRenderFinishedCommand) error {
	return r.RecordRenderFinished(ctx, cmd.JobID, cmd.WorkerID, cmd.LeaseID, cmd.AttemptNumber, cmd.ExpectedRevision)
}

// PR3Fail marks FAILED/RETRY_WAIT via the base.
func (r *SQLiteJobRepository) PR3Fail(ctx context.Context, cmd FailCommand) error {
	return r.FailWithRetry(ctx, cmd.JobID, cmd.ErrorCode, cmd.ErrorMessage, cmd.Retryable, cmd.ExpectedRevision)
}

// PR3Cancel cancels a job via the base.
func (r *SQLiteJobRepository) PR3Cancel(ctx context.Context, cmd CancelCommand) error {
	return r.Cancel(ctx, cmd.JobID, cmd.Reason, cmd.ExpectedRevision)
}

// PR3RequeueExpiredLeases requeues via the base.
func (r *SQLiteJobRepository) PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error) {
	results, err := r.baseJobRepository.RequeueExpiredLeases(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	out := make([]RequeueResult, len(results))
	for i, res := range results {
		out[i] = RequeueResult{
			JobID:          res.JobID,
			PreviousStatus: JobStatus(res.PreviousStatus),
			NewStatus:      JobStatus(res.NewStatus),
			Reason:         res.Reason,
			Attempt:        res.Attempt,
		}
	}
	return out, nil
}
