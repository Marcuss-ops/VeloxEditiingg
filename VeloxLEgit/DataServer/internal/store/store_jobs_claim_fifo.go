package store

// store_jobs_claim_fifo.go — FIFO claim path.
//
// ClaimNextPendingJob scans PENDING jobs in (updated_at ASC, job_id
// ASC) order, applies the job_type allow-list filter, and feeds each
// candidate to the shared write-side helper (claimJobTx in
// store_jobs_claim_tx.go). The first candidate whose CAS succeeds is
// returned; the rest of the row pool is left untouched inside the
// same tx.
//
// Per-row CAS is owned by `claimJobTx`; this file owns:
//   - row scanning (with the type-filter parse payload step)
//   - job_type allow-list filter (delegated to jobTypeAllowed)
//   - tx.Commit() on success / no-claim
//   - LogJobEvent with the FIFO-shape payload
//     (no rank_score / rank_eligible / rank_bandwidth_fit).
//
// PR #6: Requirements return is from columns only; no _requirements
// in result_json. PR #9: assigned_to, claimed_by, retry_count columns
// dropped — attempt column is the retry-count proxy.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/costmodel"
)

// ClaimNextPendingJob atomically claims the next pending/queued job for a worker.
// Reads columns directly (not raw_json), then writes the claim via result_json.
// Returns the updated result_json blob, per-job Requirements from dedicated
// columns, and true if a job was claimed.
//
// PR #6: Requirements return is from columns only; no _requirements in result_json.
func (s *SQLiteStore) ClaimNextPendingJob(workerID string, allowedJobTypes []string, now time.Time) ([]byte, costmodel.JobRequirements, bool, error) {
	var zeroReq costmodel.JobRequirements
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, zeroReq, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// PR #9: assigned_to, claimed_by, retry_count columns dropped.
	// Use attempt column as retry count proxy; filter only by PENDING status.
	rows, err := tx.Query(
		`SELECT job_id, status, job_fingerprint, run_id, job_run_id,
		        video_name, project_id, COALESCE(attempt, 0) as retry_count, request_json, result_json,
		        COALESCE(job_required_resource_class, ''),
		        COALESCE(job_required_temporal_mode, ''),
		        COALESCE(job_required_deterministic, 0),
		        COALESCE(job_required_cacheable, 0),
		        COALESCE(job_required_min_bandwidth_mbps, 0.0)
		 FROM jobs
		 WHERE UPPER(status) = 'PENDING'
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC`,
	)
	if err != nil {
		return nil, zeroReq, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			jobID, status, jobFingerprint, runID, jobRunID sql.NullString
			videoName, projectID                           sql.NullString
			retryCount                                     sql.NullInt64
			requestJSON, resultJSON                        sql.NullString
			requiredResourceClass, requiredTemporalMode    sql.NullString
			requiredDeterministic, requiredCacheable       sql.NullInt64
			requiredMinBandwidthMbps                       sql.NullFloat64
		)
		if err := rows.Scan(&jobID, &status, &jobFingerprint, &runID, &jobRunID,
			&videoName, &projectID, &retryCount, &requestJSON, &resultJSON,
			&requiredResourceClass, &requiredTemporalMode,
			&requiredDeterministic, &requiredCacheable, &requiredMinBandwidthMbps); err != nil {
			return nil, zeroReq, false, err
		}

		// Check job type filter if specified (parse request_json properly to avoid substring false positives).
		if len(allowedJobTypes) > 0 && requestJSON.Valid && requestJSON.String != "" {
			var req map[string]any
			if err := json.Unmarshal([]byte(requestJSON.String), &req); err == nil {
				if !jobTypeAllowed(req, allowedJobTypes) {
					continue
				}
			}
		}

		newRetry := int(retryCount.Int64) + 1

		requirements := costmodel.JobRequirements{
			ResourceClass:    costmodel.ResourceClass(strings.TrimSpace(requiredResourceClass.String)),
			TemporalMode:     costmodel.TemporalMode(strings.TrimSpace(requiredTemporalMode.String)),
			Deterministic:    requiredDeterministic.Int64 != 0,
			Cacheable:        requiredCacheable.Int64 != 0,
			MinBandwidthMbps: requiredMinBandwidthMbps.Float64,
		}

		outcome, err := s.claimJobTx(
			context.Background(), tx,
			jobID.String, newRetry,
			resultJSON.String, workerID, now,
			fmt.Sprintf("Job assigned to worker %s", workerID),
			requirements,
		)
		if errors.Is(err, ErrClaimCASLost) {
			// PENDING flipped under us — try next candidate.
			continue
		}
		if err != nil {
			return nil, zeroReq, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, zeroReq, false, err
		}

		_ = s.LogJobEvent(jobID.String, "job_claimed", map[string]interface{}{
			"worker_id": workerID, "lease_id": outcome.LeaseID, "attempt": outcome.NewRetry,
		})
		return outcome.ResultJSON, outcome.Requirements, true, nil
	}

	if err := rows.Err(); err != nil {
		return nil, zeroReq, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, zeroReq, false, err
	}
	return nil, zeroReq, false, nil
}
