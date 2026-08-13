package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/identity"
	"velox-server/internal/repository"
)

type MediaProbeEnqueueParams = repository.MediaProbeEnqueueParams

type MediaProbeJob struct {
	ID                   int64
	ArtifactID           string
	SHA256               string
	StorageKey           string
	ExpectedAudioStreams int
	DestinationID        string
	AttemptCount         int
	MaxAttempts          int
	LeaseID              string
	LeaseUntil           time.Time
}

type MediaProbeEnqueuer = repository.MediaProbeEnqueuer

type MediaProbeRepository struct{ db *sql.DB }

func NewSQLiteMediaProbeRepository(db *sql.DB) *MediaProbeRepository {
	if db == nil {
		panic("store: NewSQLiteMediaProbeRepository requires a non-nil database")
	}
	return &MediaProbeRepository{db: db}
}

func (r *MediaProbeRepository) EnqueueMediaProbe(ctx context.Context, p MediaProbeEnqueueParams) error {
	if p.ArtifactID == "" || p.SHA256 == "" || p.StorageKey == "" {
		return fmt.Errorf("store: enqueue media probe: artifact_id, sha256 and storage_key are required")
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 5
	}
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	now := p.Now.UTC().Format(time.RFC3339Nano)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO media_probe_jobs
			(artifact_id, sha256, storage_key, expected_audio_streams, destination_id, status,
			 attempt_count, max_attempts, next_attempt_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, ?, ?)
		ON CONFLICT(artifact_id, sha256) DO UPDATE SET
			storage_key = excluded.storage_key,
			expected_audio_streams = excluded.expected_audio_streams,
			destination_id = excluded.destination_id,
			max_attempts = excluded.max_attempts,
			status = CASE WHEN media_probe_jobs.status = 'FAILED' THEN 'PENDING' ELSE media_probe_jobs.status END,
			next_attempt_at = CASE WHEN media_probe_jobs.status = 'FAILED' THEN excluded.next_attempt_at ELSE media_probe_jobs.next_attempt_at END,
			last_error = CASE WHEN media_probe_jobs.status = 'FAILED' THEN '' ELSE media_probe_jobs.last_error END,
			updated_at = excluded.updated_at`, p.ArtifactID, p.SHA256, p.StorageKey, p.ExpectedAudioStreams, p.DestinationID,
		p.MaxAttempts, now, now, now)
	if err != nil {
		return fmt.Errorf("store: enqueue media probe: %w", err)
	}
	return nil
}

func (r *MediaProbeRepository) ClaimMediaProbe(ctx context.Context, owner string, leaseTTL time.Duration, now time.Time) (*MediaProbeJob, error) {
	if owner == "" {
		return nil, fmt.Errorf("store: claim media probe: owner is required")
	}
	if leaseTTL <= 0 {
		leaseTTL = 60 * time.Second
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: claim media probe begin: %w", err)
	}
	defer tx.Rollback()
	var job MediaProbeJob
	var next, oldLeaseUntil string
	err = tx.QueryRowContext(ctx, `
		SELECT id, artifact_id, sha256, storage_key, expected_audio_streams,
		       destination_id, attempt_count, max_attempts, next_attempt_at, lease_until
		FROM media_probe_jobs
		WHERE (status = 'PENDING' AND next_attempt_at <= ?)
		   OR (status = 'RUNNING' AND lease_until <> '' AND lease_until < ?)
		ORDER BY next_attempt_at, id LIMIT 1`,
		now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)).
		Scan(&job.ID, &job.ArtifactID, &job.SHA256, &job.StorageKey, &job.ExpectedAudioStreams, &job.DestinationID, &job.AttemptCount, &job.MaxAttempts,
			&next, &oldLeaseUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: claim media probe select: %w", err)
	}
	leaseID, err := identity.NewHex128()
	if err != nil {
		return nil, fmt.Errorf("store: claim media probe lease: %w", err)
	}
	leaseUntil := now.Add(leaseTTL)
	res, err := tx.ExecContext(ctx, `
		UPDATE media_probe_jobs
		SET status='RUNNING', attempt_count=attempt_count+1, lease_id=?, lease_until=?, updated_at=?
		WHERE id=? AND (status='PENDING' OR (status='RUNNING' AND lease_until=?))`,
		leaseID, leaseUntil.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), job.ID, oldLeaseUntil)
	if err != nil {
		return nil, fmt.Errorf("store: claim media probe update: %w", err)
	}
	n, err := readRowsAffected(res, "claim media probe")
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: claim media probe commit: %w", err)
	}
	job.AttemptCount++
	job.LeaseID, job.LeaseUntil = leaseID, leaseUntil
	return &job, nil
}

// CompleteMediaProbe gates publication on a successful probe. The entire
// probe result, artifact transition, parent transition, and delivery rows
// commit atomically under the claimed lease.
func (r *MediaProbeRepository) CompleteMediaProbe(ctx context.Context, job MediaProbeJob, actualAudioStreams int, durationMs int64, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowStr := now.UTC().Format(time.RFC3339Nano)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: complete media probe begin: %w", err)
	}
	defer tx.Rollback()
	probeStatus := "SUCCEEDED"
	if job.ExpectedAudioStreams > 0 && actualAudioStreams != job.ExpectedAudioStreams {
		probeStatus = "FAILED"
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE media_probe_jobs
		SET status=?, actual_audio_streams=?, duration_ms=?, lease_id='', lease_until='', completed_at=?, updated_at=?, last_error=''
		WHERE id=? AND status='RUNNING' AND lease_id=?`, probeStatus, actualAudioStreams, durationMs, nowStr, nowStr, job.ID, job.LeaseID)
	if err != nil {
		return fmt.Errorf("store: complete media probe job: %w", err)
	}
	n, err := readRowsAffected(res, "complete media probe job")
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrMediaProbeLeaseConflict
	}

	if job.ExpectedAudioStreams > 0 && actualAudioStreams != job.ExpectedAudioStreams {
		res, err := tx.ExecContext(ctx, `UPDATE media_probe_jobs SET last_error=? WHERE id=? AND status='FAILED'`, "audio stream count mismatch", job.ID)
		if err != nil {
			return err
		}
		n, err := readRowsAffected(res, "media probe mismatch error")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
		res, err = tx.ExecContext(ctx, `UPDATE artifacts SET status='QUARANTINED', duration_ms=?, duration_seconds=? WHERE id=? AND status='VERIFYING'`, durationMs, float64(durationMs)/1000, job.ArtifactID)
		if err != nil {
			return fmt.Errorf("store: quarantine artifact: %w", err)
		}
		n, err = readRowsAffected(res, "quarantine media probe artifact")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
		jobID, err := queryArtifactJobID(ctx, tx, job.ArtifactID)
		if err != nil {
			return err
		}
		const jobFailed = "FAILED"
		res, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?, completed_at=?, updated_at=? WHERE job_id=? AND status='AWAITING_ARTIFACT'`, jobFailed, nowStr, nowStr, jobID)
		if err != nil {
			return fmt.Errorf("store: fail media probe parent: %w", err)
		}
		n, err = readRowsAffected(res, "fail media probe parent")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: complete media probe quarantine commit: %w", err)
		}
		return nil
	}
	res, err = tx.ExecContext(ctx, `
		UPDATE artifacts SET status='READY', duration_ms=?, duration_seconds=?, verified_at=COALESCE(NULLIF(verified_at,''), ?)
		WHERE id=? AND status='VERIFYING'`, durationMs, float64(durationMs)/1000, nowStr, job.ArtifactID)
	if err != nil {
		return fmt.Errorf("store: ready artifact: %w", err)
	}
	n, err = readRowsAffected(res, "ready media probe artifact")
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrMediaProbeLeaseConflict
	}

	jobID, err := queryArtifactJobID(ctx, tx, job.ArtifactID)
	if err != nil {
		return err
	}
	planQuery := `SELECT destination_id, COALESCE(retry_budget, 5) FROM job_delivery_plans WHERE job_id=? AND enabled=1 ORDER BY priority, destination_id`
	planArgs := []any{jobID}
	if job.DestinationID != "" {
		planQuery = `SELECT destination_id, COALESCE(retry_budget, 5) FROM job_delivery_plans WHERE job_id=? AND destination_id=? AND enabled=1`
		planArgs = append(planArgs, job.DestinationID)
	}
	rows, err := tx.QueryContext(ctx, planQuery, planArgs...)
	if err != nil {
		return fmt.Errorf("store: media probe plans: %w", err)
	}
	var destinations []struct {
		id  string
		max int
	}
	for rows.Next() {
		var d struct {
			id  string
			max int
		}
		if err := rows.Scan(&d.id, &d.max); err != nil {
			rows.Close()
			return err
		}
		if d.max <= 0 {
			d.max = 5
		}
		destinations = append(destinations, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("store: media probe plans iterate: %w", err)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(destinations) == 0 {
		var requestJSON string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(request_json,'{}') FROM jobs WHERE job_id=?`, jobID).Scan(&requestJSON); err != nil {
			return err
		}
		if requestJSON != `{"render_only":true}` && requestJSON != `{"render_only": true}` {
			return fmt.Errorf("store: media probe: missing explicit delivery plan for job %s", jobID)
		}
	} else {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET status='DELIVERING', updated_at=?, revision=revision+1 WHERE job_id=? AND status='AWAITING_ARTIFACT'`, nowStr, jobID)
		if err != nil {
			return err
		}
		n, err := readRowsAffected(res, "media probe parent delivering")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
		for _, d := range destinations {
			id, err := identity.NewHex128()
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO job_deliveries (delivery_id,artifact_id,destination_id,status,max_attempts,idempotency_key,created_at,updated_at) VALUES (?,?,?,'PENDING',?,?,?,?) ON CONFLICT(artifact_id,destination_id) DO NOTHING`, id, job.ArtifactID, d.id, d.max, job.ArtifactID+"_"+d.id, nowStr, nowStr); err != nil {
				return err
			}
		}
	}
	if len(destinations) == 0 {
		const jobSucceeded = "SUCCEEDED"
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?, completed_at=?, updated_at=?, revision=revision+1 WHERE job_id=? AND status='AWAITING_ARTIFACT'`, jobSucceeded, nowStr, nowStr, jobID)
		if err != nil {
			return err
		}
		n, err := readRowsAffected(res, "media probe render-only parent succeeded")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: complete media probe commit: %w", err)
	}
	return nil
}

func queryArtifactJobID(ctx context.Context, tx *sql.Tx, artifactID string) (string, error) {
	var jobID string
	if err := tx.QueryRowContext(ctx, `SELECT job_id FROM artifacts WHERE id=?`, artifactID).Scan(&jobID); err != nil {
		return "", fmt.Errorf("store: media probe artifact job: %w", err)
	}
	return jobID, nil
}

var ErrMediaProbeLeaseConflict = errors.New("store: media probe lease conflict")

func (r *MediaProbeRepository) FailMediaProbe(ctx context.Context, job MediaProbeJob, probeErr error, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	message := "media probe failed"
	if probeErr != nil {
		message = probeErr.Error()
	}
	terminal := job.AttemptCount >= job.MaxAttempts
	status := "PENDING"
	if terminal {
		status = "FAILED"
	}
	next := now.Add(time.Duration(1<<minInt(job.AttemptCount, 6)) * time.Second)
	if terminal {
		next = now
	}
	nowStr := now.UTC().Format(time.RFC3339Nano)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: fail media probe begin: %w", err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE media_probe_jobs SET status=?, next_attempt_at=?, lease_id='', lease_until='', last_error=?, updated_at=?, completed_at=CASE WHEN ?='FAILED' THEN ? ELSE completed_at END WHERE id=? AND status='RUNNING' AND lease_id=?`, status, next.UTC().Format(time.RFC3339Nano), message, nowStr, status, nowStr, job.ID, job.LeaseID)
	if err != nil {
		return fmt.Errorf("store: fail media probe: %w", err)
	}
	n, err := readRowsAffected(res, "fail media probe")
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrMediaProbeLeaseConflict
	}
	if terminal {
		res, err := tx.ExecContext(ctx, `UPDATE artifacts SET status='QUARANTINED' WHERE id=? AND status='VERIFYING'`, job.ArtifactID)
		if err != nil {
			return err
		}
		n, err := readRowsAffected(res, "quarantine terminal media probe artifact")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
		jobID, err := queryArtifactJobID(ctx, tx, job.ArtifactID)
		if err != nil {
			return err
		}
		const jobFailed = "FAILED"
		res, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?, completed_at=?, updated_at=? WHERE job_id=? AND status='AWAITING_ARTIFACT'`, jobFailed, nowStr, nowStr, jobID)
		if err != nil {
			return err
		}
		n, err = readRowsAffected(res, "fail terminal media probe parent")
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMediaProbeLeaseConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: fail media probe commit: %w", err)
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ MediaProbeEnqueuer = (*MediaProbeRepository)(nil)
