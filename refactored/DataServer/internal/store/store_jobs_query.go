package store

import (
	"context"
	"database/sql"
	"encoding/json"
)

// jobColumns is the SELECT list for job queries returning authoritative columns + JSON blobs.
const jobColumns = `job_id, status, video_name, project_id,
	created_at, updated_at, started_at,
	completed_at, assigned_at,
	assigned_to, worker_name, claimed_by, claimed_at,
	lease_id, lease_expiry,
	retry_count, attempt, max_retries,
	last_error, last_error_at, error_message, failed_at, failed_by,
	processing_at,
	video_uploaded, master_video_path, last_upload_result, last_upload_attempt_at,
	last_drive_upload_result, remote_status,
	artifact_id, output_sha256, upload_idempotency_key,
	output_video_id, drive_url,
	job_fingerprint, submitted_via, last_activity, run_id, job_run_id,
	logs_updated_at, slot_data,
	request_json, result_json, raw_json`

// scanJobRow scans a job row into a map, handling NULL SQL values gracefully.
func scanJobRow(scanner interface{ Scan(dest ...interface{}) error }) (map[string]any, error) {
	var (
		jobID, status, videoName, projectID                                                                          sql.NullString
		createdAt, updatedAt, startedAt                                                                              sql.NullString
		completedAt, assignedAt                                                                                      sql.NullString
		assignedTo, workerName, claimedBy, claimedAt                                                                 sql.NullString
		leaseID, leaseExpiry                                                                                         sql.NullString
		retryCount, attempt, maxRetries                                                                              sql.NullInt64
		lastError, errorMessage, failedBy                                                                            sql.NullString
		lastErrorAt, failedAt, processingAt                                                                          sql.NullString
		videoUploadedInt                                                                                             sql.NullInt64
		masterVideoPath, lastUploadResult, lastUploadAttemptAt                                                       sql.NullString
		lastDriveUploadResult, remoteStatus                                                                          sql.NullString
		artifactID, outputSHA256, uploadIdempotencyKey                                                               sql.NullString
		outputVideoID, driveURL                                                                                      sql.NullString
		jobFingerprint, submittedVia, lastActivity, runID, jobRunID                                                  sql.NullString
		logsUpdatedAt, slotDataRaw                                                                                   sql.NullString
		requestJSON, resultJSON, rawJSON                                                                             sql.NullString
	)

	dest := []interface{}{
		&jobID, &status, &videoName, &projectID,
		&createdAt, &updatedAt, &startedAt,
		&completedAt, &assignedAt,
		&assignedTo, &workerName, &claimedBy, &claimedAt,
		&leaseID, &leaseExpiry,
		&retryCount, &attempt, &maxRetries,
		&lastError, &lastErrorAt, &errorMessage, &failedAt, &failedBy,
		&processingAt,
		&videoUploadedInt, &masterVideoPath, &lastUploadResult, &lastUploadAttemptAt,
		&lastDriveUploadResult, &remoteStatus,
		&artifactID, &outputSHA256, &uploadIdempotencyKey,
		&outputVideoID, &driveURL,
		&jobFingerprint, &submittedVia, &lastActivity, &runID, &jobRunID,
		&logsUpdatedAt, &slotDataRaw,
		&requestJSON, &resultJSON, &rawJSON,
	}

	if err := scanner.Scan(dest...); err != nil {
		return nil, err
	}

	m := make(map[string]any)

	// String columns
	setStr := func(key string, ns sql.NullString) { if ns.Valid { m[key] = ns.String } }
	setStr("job_id", jobID)
	setStr("status", status)
	setStr("video_name", videoName)
	setStr("project_id", projectID)
	setStr("created_at", createdAt)
	setStr("updated_at", updatedAt)
	setStr("started_at", startedAt)
	setStr("completed_at", completedAt)
	setStr("assigned_at", assignedAt)
	setStr("assigned_to", assignedTo)
	setStr("worker_name", workerName)
	setStr("claimed_by", claimedBy)
	setStr("claimed_at", claimedAt)
	setStr("lease_id", leaseID)
	setStr("lease_expiry", leaseExpiry)
	setStr("last_error", lastError)
	setStr("error_message", errorMessage)
	setStr("failed_by", failedBy)
	setStr("last_error_at", lastErrorAt)
	setStr("failed_at", failedAt)
	setStr("processing_at", processingAt)
	if videoUploadedInt.Valid {
		m["video_uploaded"] = videoUploadedInt.Int64 == 1
	}
	setStr("master_video_path", masterVideoPath)
	setStr("last_upload_result", lastUploadResult)
	setStr("last_upload_attempt_at", lastUploadAttemptAt)
	setStr("last_drive_upload_result", lastDriveUploadResult)
	setStr("remote_status", remoteStatus)
	setStr("artifact_id", artifactID)
	setStr("output_sha256", outputSHA256)
	setStr("upload_idempotency_key", uploadIdempotencyKey)
	setStr("output_video_id", outputVideoID)
	setStr("drive_url", driveURL)
	setStr("job_fingerprint", jobFingerprint)
	setStr("submitted_via", submittedVia)
	setStr("last_activity", lastActivity)
	setStr("run_id", runID)
	setStr("job_run_id", jobRunID)
	setStr("logs_updated_at", logsUpdatedAt)

	// Integer columns
	setInt := func(key string, ni sql.NullInt64) { if ni.Valid { m[key] = int(ni.Int64) } }
	setInt("retry_count", retryCount)
	setInt("attempt", attempt)
	setInt("max_retries", maxRetries)

	// JSON blob columns
	setJSON := func(key string, ns sql.NullString) {
		if ns.Valid && ns.String != "" {
			var parsed map[string]any
			if err := json.Unmarshal([]byte(ns.String), &parsed); err == nil {
				m[key] = parsed
			}
		}
	}
	setJSON("request_json", requestJSON)
	setJSON("result_json", resultJSON)
	setJSON("raw_json", rawJSON)

	// Slot data
	if slotDataRaw.Valid && slotDataRaw.String != "" {
		var slot map[string]any
		if err := json.Unmarshal([]byte(slotDataRaw.String), &slot); err == nil {
			m["slot_data"] = slot
		}
	}

	return m, nil
}

func (s *SQLiteStore) ListJobs(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?`, limit)
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
		m, err := scanJobRow(rows)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *SQLiteStore) GetJob(ctx context.Context, jobID string) (map[string]any, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_id = ?`, jobID)
	return scanJobRow(row)
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

	query := `SELECT ` + jobColumns + ` FROM jobs WHERE UPPER(status) IN (`
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
		m, err := scanJobRow(rows)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *SQLiteStore) GetActiveJobs() (map[string]map[string]any, error) {
	rows, err := s.db.Query(
		`SELECT ` + jobColumns + ` FROM jobs WHERE UPPER(status) IN ('PENDING', 'PROCESSING', 'QUEUED', 'ASSIGNED', 'LEASED')`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]map[string]any)
	for rows.Next() {
		m, err := scanJobRow(rows)
		if err != nil {
			continue
		}
		if id, ok := m["job_id"].(string); ok {
			out[id] = m
		}
	}
	return out, nil
}
