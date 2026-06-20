package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ClaimNextPendingJob atomically claims the next pending/queued job for a worker.
// Reads columns directly (not raw_json), then writes the claim via result_json.
// Returns the updated result_json blob and true if a job was claimed.
func (s *SQLiteStore) ClaimNextPendingJob(workerID string, allowedJobTypes []string, now time.Time) ([]byte, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Read candidate jobs with their status and assigned_to columns (not raw_json)
	rows, err := tx.Query(
		`SELECT job_id, status, assigned_to, claimed_by, job_fingerprint, run_id, job_run_id,
		        video_name, project_id, retry_count, request_json, result_json
		 FROM jobs
		 WHERE UPPER(status) = 'PENDING'
		   AND COALESCE(assigned_to, '') = ''
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC`,
	)
	if err != nil {
		return nil, false, err
	}

	nowISO := now.UTC().Format(time.RFC3339)
	nowUnix := now.UTC().Unix()

	for rows.Next() {
		var (
			jobID, status, assignedTo, claimedBy, jobFingerprint, runID, jobRunID sql.NullString
			videoName, projectID                                                  sql.NullString
			retryCount                                                            sql.NullInt64
			requestJSON, resultJSON                                               sql.NullString
		)
		if err := rows.Scan(&jobID, &status, &assignedTo, &claimedBy, &jobFingerprint, &runID, &jobRunID,
			&videoName, &projectID, &retryCount, &requestJSON, &resultJSON); err != nil {
			rows.Close()
			return nil, false, err
		}

		// Double-check safety: already claimed
		if assignedTo.Valid && strings.TrimSpace(assignedTo.String) != "" {
			continue
		}
		if claimedBy.Valid && strings.TrimSpace(claimedBy.String) != "" {
			continue
		}

		// Check job type filter if specified (parse request_json properly to avoid substring false positives)
		if len(allowedJobTypes) > 0 && requestJSON.Valid && requestJSON.String != "" {
			var req map[string]any
			if err := json.Unmarshal([]byte(requestJSON.String), &req); err == nil {
				if !jobTypeAllowed(req, allowedJobTypes) {
					continue
				}
			}
		}

		newRetry := int(retryCount.Int64) + 1
		leaseID := uuid.NewString()
		leaseExpiry := now.UTC().Add(30 * time.Minute).Format(time.RFC3339)

		// Build the updated result_json blob with claim data
		resultMap := make(map[string]any)
		if resultJSON.Valid && resultJSON.String != "" {
			_ = json.Unmarshal([]byte(resultJSON.String), &resultMap)
		}
		resultMap["job_id"] = jobID.String
		resultMap["status"] = "LEASED"
		resultMap["assigned_to"] = workerID
		resultMap["worker_name"] = workerID
		resultMap["assigned_at"] = nowISO
		resultMap["claimed_by"] = workerID
		resultMap["claimed_at"] = nowISO
		resultMap["lease_id"] = leaseID
		resultMap["lease_expiry"] = leaseExpiry
		resultMap["lease_expires_at"] = leaseExpiry
		resultMap["attempt"] = newRetry
		resultMap["contract_version"] = 2
		resultMap["updated_at"] = nowUnix
		resultMap["retry_count"] = newRetry

		updatedResult, err := json.Marshal(resultMap)
		if err != nil {
			rows.Close()
			return nil, false, err
		}

		result, err := tx.Exec(
			`UPDATE jobs
			 SET status = ?, assigned_to = ?, worker_name = ?, retry_count = ?, attempt = ?,
			     result_json = ?, updated_at = ?, migrated_at = ?,
			     assigned_at = ?,
			     lease_id = ?, lease_expiry = ?,
			     claimed_by = ?, claimed_at = ?
			 WHERE job_id = ?
			   AND UPPER(status) = 'PENDING'
			   AND COALESCE(assigned_to, '') = ''`,
			"LEASED", workerID, workerID, newRetry, newRetry,
			string(updatedResult), nowISO, nowISO,
			nowISO,
			leaseID, leaseExpiry,
			workerID, nowISO,
			jobID.String,
		)
		if err != nil {
			rows.Close()
			return nil, false, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			rows.Close()
			return nil, false, err
		}
		if affected == 0 {
			continue
		}

		// Record history (not in blob — separate table)
		history := []map[string]any{{
			"status":    "LEASED",
			"timestamp": nowISO,
			"worker_id": workerID,
			"message":   fmt.Sprintf("Job assigned to worker %s", workerID),
		}}
		if err := s.replaceJobHistoryTx(tx, jobID.String, history); err != nil {
			rows.Close()
			return nil, false, err
		}

		// Record job attempt
		insertedID, attemptErr := s.InsertJobAttemptTx(tx, jobID.String, newRetry, workerID, leaseID)
		if attemptErr != nil {
			rows.Close()
			return nil, false, fmt.Errorf("failed to record job attempt: %w", attemptErr)
		}

		if err := tx.Commit(); err != nil {
			rows.Close()
			return nil, false, err
		}
		rows.Close()

		if insertedID > 0 {
			_ = s.LogJobEvent(jobID.String, "job_claimed", map[string]interface{}{
				"worker_id": workerID, "lease_id": leaseID, "attempt": newRetry,
			})
		}
		return bytes.Clone(updatedResult), true, nil
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return nil, false, nil
}

func jobTypeAllowed(payload map[string]any, allowedJobTypes []string) bool {
	if len(allowedJobTypes) == 0 {
		return true
	}

	jobType := strings.TrimSpace(asString(payload["job_type"]))
	if jobType == "" {
		if params, ok := payload["parameters"].(map[string]any); ok {
			jobType = strings.TrimSpace(asString(params["job_type"]))
		}
	}
	if jobType == "" {
		return true
	}

	for _, allowed := range allowedJobTypes {
		if strings.EqualFold(strings.TrimSpace(allowed), jobType) {
			return true
		}
	}
	return false
}

// JobsRepository exposes the minimal job read operations needed by HTTP handlers.
// This is the READ-ONLY interface — for any write operation (create, claim,
// transition, PR3 lifecycle), see JobRepository in jobs_writer_types.go.
// SQLiteJobsRepository is the adapter; SQLiteJobRepository is the full write-capable
// implementation. The naming difference (Jobs vs Job, plural vs singular) is
// intentional: JobsRepository = read-only projection, JobRepository = full CRUD.
type JobsRepository interface {
	ListJobs(ctx context.Context, limit int) ([]map[string]any, error)
	GetJob(ctx context.Context, jobID string) (map[string]any, error)
	JobCounts(ctx context.Context) (map[string]int64, error)
}

// SQLiteJobsRepository adapts SQLiteStore to the JobsRepository interface.
type SQLiteJobsRepository struct {
	store *SQLiteStore
}

// NewSQLiteJobsRepository creates a read-only jobs repository backed by SQLiteStore.
func NewSQLiteJobsRepository(store *SQLiteStore) *SQLiteJobsRepository {
	return &SQLiteJobsRepository{store: store}
}

func (r *SQLiteJobsRepository) ListJobs(ctx context.Context, limit int) ([]map[string]any, error) {
	return r.store.ListJobs(ctx, limit)
}

func (r *SQLiteJobsRepository) GetJob(ctx context.Context, jobID string) (map[string]any, error) {
	return r.store.GetJob(ctx, jobID)
}

func (r *SQLiteJobsRepository) JobCounts(ctx context.Context) (map[string]int64, error) {
	return r.store.JobCounts(ctx)
}
