package store

import (
	"context"
	"encoding/json"
)

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
