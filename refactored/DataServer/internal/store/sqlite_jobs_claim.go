package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (s *SQLiteStore) ClaimNextPendingJob(workerID string, allowedJobTypes []string, now time.Time) ([]byte, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(
		`SELECT job_id, raw_json
		 FROM jobs
		 WHERE UPPER(status) IN ('PENDING', 'QUEUED')
		   AND COALESCE(assigned_to, '') = ''
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC`,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	nowISO := now.UTC().Format(time.RFC3339)
	nowUnix := now.UTC().Unix()

	for rows.Next() {
		var jobID string
		var raw string
		if err := rows.Scan(&jobID, &raw); err != nil {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}

		status := strings.ToUpper(asString(payload["status"]))
		if status != "PENDING" && status != "QUEUED" {
			continue
		}
		if assigned := strings.TrimSpace(asString(payload["assigned_to"])); assigned != "" {
			continue
		}
		if claimed := strings.TrimSpace(asString(payload["claimed_by"])); claimed != "" {
			continue
		}
		if len(allowedJobTypes) > 0 && !jobTypeAllowed(payload, allowedJobTypes) {
			continue
		}

		retryCount := asInt(payload["retry_count"]) + 1
		history := make([]map[string]any, 0)
		switch entries := payload["history"].(type) {
		case []any:
			for _, entry := range entries {
				if hm, ok := entry.(map[string]any); ok {
					history = append(history, hm)
				}
			}
		}
		history = append(history, map[string]any{
			"status":    "PROCESSING",
			"timestamp": nowISO,
			"worker_id": workerID,
			"message":   fmt.Sprintf("Job assigned to worker %s", workerID),
		})

		payload["status"] = "PROCESSING"
		payload["assigned_to"] = workerID
		payload["assigned_at"] = nowISO
		payload["claimed_by"] = workerID
		payload["claimed_at"] = nowISO
		payload["updated_at"] = nowUnix
		payload["retry_count"] = retryCount
		payload["lease_expiry"] = now.UTC().Add(30 * time.Minute).Format(time.RFC3339)
		payload["history"] = history

		updatedRaw, err := json.Marshal(payload)
		if err != nil {
			return nil, false, err
		}

		result, err := tx.Exec(
			`UPDATE jobs
			 SET status = ?, assigned_to = ?, retry_count = ?, raw_json = ?, updated_at = ?, migrated_at = ?
			 WHERE job_id = ?
			   AND UPPER(status) IN ('PENDING', 'QUEUED')
			   AND COALESCE(assigned_to, '') = ''`,
			"PROCESSING", workerID, retryCount, string(updatedRaw), nowISO, nowISO, jobID,
		)
		if err != nil {
			return nil, false, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, false, err
		}
		if affected == 0 {
			continue
		}

		if err := s.replaceJobHistoryTx(tx, jobID, history); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return bytes.Clone(updatedRaw), true, nil
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
