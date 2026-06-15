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
		leaseID := uuid.NewString()
		leaseExpiry := now.UTC().Add(30 * time.Minute).Format(time.RFC3339)
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
		payload["lease_id"] = leaseID
		payload["lease_expiry"] = leaseExpiry
		payload["lease_expires_at"] = leaseExpiry
		payload["attempt"] = retryCount
		payload["contract_version"] = 2
		payload["updated_at"] = nowUnix
		payload["retry_count"] = retryCount
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
		// Record job_attempt inside the same transaction so claim and attempt are atomic
		insertedID, attemptErr := s.InsertJobAttemptTx(tx, jobID, retryCount, workerID, leaseID)
		if attemptErr != nil {
			return nil, false, fmt.Errorf("failed to record job attempt: %w", attemptErr)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		if insertedID > 0 {
			_ = s.LogJobEvent(jobID, "job_claimed", map[string]interface{}{
				"worker_id": workerID, "lease_id": leaseID, "attempt": retryCount,
			})
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

func (s *SQLiteStore) ReplaceJobs(rawJobs map[string][]byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("DELETE FROM job_logs"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM job_history"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM jobs"); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for jobID, raw := range rawJobs {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		status := asString(m["status"])
		videoName := asString(m["video_name"])
		projectID := asString(m["project_id"])
		createdAt := toISO(m["created_at"])
		updatedAt := toISO(m["updated_at"])
		assignedTo := asString(m["assigned_to"])
		retryCount := asInt(m["retry_count"])
		lastError := asString(m["last_error"])
		if lastError == "" {
			lastError = asString(m["error_message"])
		}
		completedAt := toISO(m["completed_at"])

		if _, err := tx.Exec(
			`INSERT INTO jobs (
				job_id, status, video_name, project_id, created_at, updated_at,
				assigned_to, retry_count, last_error, completed_at, raw_json, migrated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			jobID, status, videoName, projectID, createdAt, updatedAt,
			assignedTo, retryCount, lastError, completedAt, string(raw), now,
		); err != nil {
			return err
		}

		if hist, ok := m["history"].([]any); ok {
			for _, hv := range hist {
				hm, ok := hv.(map[string]any)
				if !ok {
					continue
				}
				hraw, _ := json.Marshal(hm)
				if _, err := tx.Exec(
					`INSERT INTO job_history (job_id, status, event_ts, worker_id, message, raw_json)
					 VALUES (?, ?, ?, ?, ?, ?)`,
					jobID, asString(hm["status"]), toISO(hm["timestamp"]), asString(hm["worker_id"]), asString(hm["message"]), string(hraw),
				); err != nil {
					return err
				}
			}
		}

		if logs, ok := m["logs"].([]any); ok {
			for _, lv := range logs {
				lm, ok := lv.(map[string]any)
				if !ok {
					continue
				}
				lraw, _ := json.Marshal(lm)
				isErr := 0
				if b, ok := lm["is_error"].(bool); ok && b {
					isErr = 1
				}
				logTS := toISO(lm["timestamp"])
				if logTS == "" {
					logTS = toISO(lm["time"])
				}
				if _, err := tx.Exec(
					`INSERT INTO job_logs (job_id, log_ts, message, worker_id, is_error, raw_json)
					 VALUES (?, ?, ?, ?, ?, ?)`,
					jobID, logTS, asString(lm["message"]), asString(lm["worker_id"]), isErr, string(lraw),
				); err != nil {
					return err
				}
			}
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) UpsertJob(jobID string, rawJSON []byte) error {
	var m map[string]any
	if err := json.Unmarshal(rawJSON, &m); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	status := asString(m["status"])
	videoName := asString(m["video_name"])
	projectID := asString(m["project_id"])
	createdAt := toISO(m["created_at"])
	updatedAt := toISO(m["updated_at"])
	assignedTo := asString(m["assigned_to"])
	retryCount := asInt(m["retry_count"])
	lastError := asString(m["last_error"])
	if lastError == "" {
		lastError = asString(m["error_message"])
	}
	completedAt := toISO(m["completed_at"])

	_, err := s.db.Exec(
		`INSERT INTO jobs (
			job_id, status, video_name, project_id, created_at, updated_at,
			assigned_to, retry_count, last_error, completed_at, raw_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			status=excluded.status,
			video_name=excluded.video_name,
			project_id=excluded.project_id,
			updated_at=excluded.updated_at,
			assigned_to=excluded.assigned_to,
			retry_count=excluded.retry_count,
			last_error=excluded.last_error,
			completed_at=excluded.completed_at,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		jobID, status, videoName, projectID, createdAt, updatedAt,
		assignedTo, retryCount, lastError, completedAt, string(rawJSON), now,
	)
	return err
}

func (s *SQLiteStore) DeleteJob(jobID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM job_logs WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM job_history WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM jobs WHERE job_id = ?`, jobID); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SQLiteStore) ArchiveOldJobs(olderThan time.Time) (int64, error) {
	cutoff := olderThan.UTC().Format(time.RFC3339)
	result, err := s.db.Exec(
		`DELETE FROM jobs WHERE UPPER(status) IN ('COMPLETED', 'ERROR', 'FAILED') AND updated_at < ?`,
		cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *SQLiteStore) ListJobs(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT raw_json FROM jobs ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *SQLiteStore) GetJob(ctx context.Context, jobID string) (map[string]any, error) {
	row := s.db.QueryRowContext(ctx, `SELECT raw_json FROM jobs WHERE job_id = ?`, jobID)
	var raw string
	if err := row.Scan(&raw); err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *SQLiteStore) JobCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT UPPER(COALESCE(status, 'UNKNOWN')) AS s, COUNT(*) FROM jobs GROUP BY s`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{"pending": 0, "processing": 0, "completed": 0, "error": 0, "total": 0}
	for rows.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var sname string
		var cnt int64
		if err := rows.Scan(&sname, &cnt); err != nil {
			continue
		}
		out["total"] += cnt
		switch sname {
		case "PENDING", "QUEUED":
			out["pending"] += cnt
		case "PROCESSING", "ASSIGNED", "LEASED":
			out["processing"] += cnt
		case "COMPLETED":
			out["completed"] += cnt
		case "ERROR", "FAILED", "DEAD":
			out["error"] += cnt
		}
	}
	return out, nil
}

func (s *SQLiteStore) ListJobsByStatus(statuses []string, limit int) ([]map[string]any, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10000
	}

	query := `SELECT raw_json FROM jobs WHERE UPPER(status) IN (`
	args := make([]any, len(statuses))
	for i, status := range statuses {
		if i > 0 {
			query += `, `
		}
		query += `?`
		args[i] = status
	}
	query += `) ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *SQLiteStore) GetActiveJobs() (map[string]map[string]any, error) {
	rows, err := s.db.Query(
		`SELECT job_id, raw_json FROM jobs WHERE UPPER(status) IN ('PENDING', 'PROCESSING', 'QUEUED', 'ASSIGNED', 'LEASED')`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]map[string]any)
	for rows.Next() {
		var jobID, raw string
		if err := rows.Scan(&jobID, &raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out[jobID] = m
		}
	}
	return out, nil
}

func (s *SQLiteStore) AddJobHistory(jobID, status, workerID, message string, extra map[string]any) error {
	now := time.Now().UTC().Format(time.RFC3339)
	hm := map[string]any{
		"status":    status,
		"timestamp": now,
		"worker_id": workerID,
		"message":   message,
	}
	for k, v := range extra {
		hm[k] = v
	}
	hraw, _ := json.Marshal(hm)

	_, err := s.db.Exec(
		`INSERT INTO job_history (job_id, status, event_ts, worker_id, message, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, status, now, workerID, message, string(hraw),
	)
	return err
}

func (s *SQLiteStore) replaceJobHistoryTx(tx *sql.Tx, jobID string, history []map[string]any) error {
	if _, err := tx.Exec(`DELETE FROM job_history WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	for _, hm := range history {
		hraw, _ := json.Marshal(hm)
		if _, err := tx.Exec(
			`INSERT INTO job_history (job_id, status, event_ts, worker_id, message, raw_json)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			jobID, asString(hm["status"]), toISO(hm["timestamp"]), asString(hm["worker_id"]), asString(hm["message"]), string(hraw),
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) GetJobHistory(jobID string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT raw_json FROM job_history WHERE job_id = ? ORDER BY id DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *SQLiteStore) AddJobLog(jobID, message, workerID string, isError bool) error {
	now := time.Now().UTC().Format(time.RFC3339)
	isErr := 0
	if isError {
		isErr = 1
	}
	lm := map[string]any{
		"timestamp": now,
		"message":   message,
		"worker_id": workerID,
		"is_error":  isError,
	}
	lraw, _ := json.Marshal(lm)

	_, err := s.db.Exec(
		`INSERT INTO job_logs (job_id, log_ts, message, worker_id, is_error, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, now, message, workerID, isErr, string(lraw),
	)
	return err
}

func (s *SQLiteStore) GetJobLogs(jobID string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.Query(
		`SELECT raw_json FROM job_logs WHERE job_id = ? ORDER BY id DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

// JobsRepository exposes the minimal job read operations needed by HTTP handlers.
type JobsRepository interface {
	ListJobs(ctx context.Context, limit int) ([]map[string]any, error)
	GetJob(ctx context.Context, jobID string) (map[string]any, error)
	JobCounts(ctx context.Context) (map[string]int64, error)
}

type SQLiteJobsRepository struct {
	store *SQLiteStore
}

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
