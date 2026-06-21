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
//
// PR-04.5: the per-job Requirements (columns
// job_required_resource_class + job_required_temporal_mode + the
// `_requirements` JSON sub-object inside request_json) are mirrored
// into the result_json blob under the same `_requirements` key. The
// future-rank site (PR-04.6) reads them straight from the blob; the
// reader path (jobs.Writer.Get → jobs.Job.Requirements) reconstructs
// them from the dedicated columns (canonical) with the JSON fallback
// for legacy rows.
func (s *SQLiteStore) ClaimNextPendingJob(workerID string, allowedJobTypes []string, now time.Time) ([]byte, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Read candidate jobs with their status and assigned_to columns (not raw_json)
	rows, err := tx.Query(
		`SELECT job_id, status, assigned_to, claimed_by, job_fingerprint, run_id, job_run_id,
		        video_name, project_id, retry_count, request_json, result_json,
		        COALESCE(job_required_resource_class, ''),
		        COALESCE(job_required_temporal_mode, '')
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
			requiredResourceClass, requiredTemporalMode                            sql.NullString
		)
		if err := rows.Scan(&jobID, &status, &assignedTo, &claimedBy, &jobFingerprint, &runID, &jobRunID,
			&videoName, &projectID, &retryCount, &requestJSON, &resultJSON,
			&requiredResourceClass, &requiredTemporalMode); err != nil {
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

		// PR-04.5: mirror per-job Requirements into result_json so the
		// dispatch path (handler_workers.sendPushJobOffer + future
		// rank site) can read them straight from the response blob
		// without bouncing through jobs.Writer.Get. ResourceClass +
		// TemporalMode are sourced from the dedicated columns; the
		// rank-only Deterministic + Cacheable come from the JSON
		// sub-object inside request_json (already canonical there).
		if requiredResourceClass.Valid && strings.TrimSpace(requiredResourceClass.String) != "" {
			resultMap["_requirements"] = map[string]any{
				"resource_class": strings.TrimSpace(requiredResourceClass.String),
				"temporal_mode":  strings.TrimSpace(requiredTemporalMode.String),
			}
			if requestJSON.Valid && requestJSON.String != "" {
				var reqParsed map[string]any
				if err := json.Unmarshal([]byte(requestJSON.String), &reqParsed); err == nil {
					if sub, ok := reqParsed["_requirements"].(map[string]any); ok {
						if v, ok := sub["deterministic"].(bool); ok {
							resultMap["_requirements"].(map[string]any)["deterministic"] = v
						}
						if v, ok := sub["cacheable"].(bool); ok {
							resultMap["_requirements"].(map[string]any)["cacheable"] = v
						}
					}
				}
			}
		}

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
