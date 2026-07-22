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
	worker_name, claimed_at,
	attempt, max_retries,
	revision,
	last_error, last_error_at, error_message, failed_at, failed_by,
	processing_at,
	last_upload_attempt_at,
	last_drive_upload_result, remote_status,
	job_fingerprint, submitted_via, last_activity, run_id, job_run_id,
	logs_updated_at, slot_data,
	request_json, result_json,
	workspace_id`

// scanJobRow scans a job row into a map, handling NULL SQL values gracefully.
func scanJobRow(scanner interface {
	Scan(dest ...interface{}) error
}) (map[string]any, error) {
	var (
		jobID, status, videoName, projectID                         sql.NullString
		createdAt, updatedAt, startedAt                             sql.NullString
		completedAt, assignedAt                                     sql.NullString
		workerName, claimedAt                                       sql.NullString
		attempt, maxRetries, revision                               sql.NullInt64
		lastError, errorMessage, failedBy                           sql.NullString
		lastErrorAt, failedAt, processingAt                         sql.NullString
		lastUploadAttemptAt                                         sql.NullString
		lastDriveUploadResult, remoteStatus                         sql.NullString
		jobFingerprint, submittedVia, lastActivity, runID, jobRunID sql.NullString
		logsUpdatedAt, slotDataRaw                                  sql.NullString
		requestJSON, resultJSON                                     sql.NullString
		workspaceID                                                 sql.NullInt64
	)

	dest := []interface{}{
		&jobID, &status, &videoName, &projectID,
		&createdAt, &updatedAt, &startedAt,
		&completedAt, &assignedAt,
		&workerName, &claimedAt,
		&attempt, &maxRetries, &revision,
		&lastError, &lastErrorAt, &errorMessage, &failedAt, &failedBy,
		&processingAt,
		&lastUploadAttemptAt,
		&lastDriveUploadResult, &remoteStatus,
		&jobFingerprint, &submittedVia, &lastActivity, &runID, &jobRunID,
		&logsUpdatedAt, &slotDataRaw,
		&requestJSON, &resultJSON,
		&workspaceID,
	}

	if err := scanner.Scan(dest...); err != nil {
		return nil, err
	}

	m := make(map[string]any)

	// String columns
	setStr := func(key string, ns sql.NullString) {
		if ns.Valid {
			m[key] = ns.String
		}
	}
	setStr("job_id", jobID)
	setStr("status", status)
	setStr("video_name", videoName)
	setStr("project_id", projectID)
	setStr("created_at", createdAt)
	setStr("updated_at", updatedAt)
	setStr("started_at", startedAt)
	setStr("completed_at", completedAt)
	setStr("assigned_at", assignedAt)
	setStr("worker_name", workerName)
	setStr("claimed_at", claimedAt)
	setStr("last_error", lastError)
	setStr("error_message", errorMessage)
	setStr("failed_by", failedBy)
	setStr("last_error_at", lastErrorAt)
	setStr("failed_at", failedAt)
	setStr("processing_at", processingAt)
	setStr("last_upload_attempt_at", lastUploadAttemptAt)
	setStr("last_drive_upload_result", lastDriveUploadResult)
	setStr("remote_status", remoteStatus)
	setStr("job_fingerprint", jobFingerprint)
	setStr("submitted_via", submittedVia)
	setStr("last_activity", lastActivity)
	setStr("run_id", runID)
	setStr("job_run_id", jobRunID)
	setStr("logs_updated_at", logsUpdatedAt)

	// Integer columns
	setInt := func(key string, ni sql.NullInt64) {
		if ni.Valid {
			m[key] = int(ni.Int64)
		}
	}
	setInt("attempt", attempt)
	setInt("max_retries", maxRetries)
	setInt("revision", revision)

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

	// Workspace scoping (InstaEdit BFF)
	if workspaceID.Valid {
		m["workspace_id"] = workspaceID.Int64
	}

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

// ListJobsByWorkspace returns jobs scoped to an InstaEdit workspace.
// Pass workspaceID == 0 to list jobs without an explicit workspace
// (legacy rows).
func (s *SQLiteStore) ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE COALESCE(workspace_id, 0) = ? ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?`, workspaceID, limit)
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
	return out, rows.Err()
}

// GetJobByWorkspace returns a job only if it belongs to the given
// workspace. workspaceID == 0 matches legacy rows with NULL workspace.
func (s *SQLiteStore) GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_id = ? AND COALESCE(workspace_id, 0) = ?`, jobID, workspaceID)
	return scanJobRow(row)
}

// JobCounts returns a status → count map keyed by the canonical UPPER
// jobs.status name. Returns raw bucket counts only — NO binning and NO
// "total" key. toJobsCounts (jobs.Status(k) literal cast) needs canonical
// keys; non-canonical keys land under Status("pending") and get
// double-counted by TotalRuns summation and lost under
// LegacyRunStatusPending in the orchestrator adapter. The PostgreSQL
// path uses the same canonical-key shape.
//
// Binning and totals are the caller's responsibility (getStats already
// declares allRunStatuses / allStepStatuses and seeds the map with
// zeros; TotalRuns = sum(canonical keys) by construction).
func (s *SQLiteStore) JobCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT UPPER(COALESCE(status, 'UNKNOWN')) AS s, COUNT(*) FROM jobs GROUP BY s`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
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
		out[sname] = cnt
	}
	return out, nil
}
