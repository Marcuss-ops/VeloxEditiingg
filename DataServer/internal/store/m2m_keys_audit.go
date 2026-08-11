// Package store / m2m_keys_audit.go
//
// m2m_audit_log persistence: best-effort append (failures never rewind
// the response) and latest-first list with optional client filter.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// =====================================================================
// Audit log
// =====================================================================

// AppendM2MAuditLog writes one row. The caller (middleware) supplies
// the post-handler status_code + reject_reason so the row reflects
// what the client actually saw. Failures are not propagated — the
// audit log is best-effort: callers log warnings but do NOT rewind
// the response.
func (s *SQLiteStore) AppendM2MAuditLog(ctx context.Context, e M2MAuditEntry) error {
	if e.Method == "" {
		e.Method = "POST"
	}
	if e.Path == "" {
		e.Path = "/api/v1/jobs"
	}
	if e.Scope == "" {
		e.Scope = "jobs.submit"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO m2m_audit_log (
			client_id, idem_key_hash, method, path, status_code, scope,
			scene_count, total_duration_seconds, ip_address, reject_reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		strings.TrimSpace(e.ClientID),
		strings.ToLower(strings.TrimSpace(e.IdemKeyHash)),
		e.Method,
		e.Path,
		e.StatusCode,
		e.Scope,
		e.SceneCount,
		e.TotalDurationSeconds,
		strings.TrimSpace(e.IPAddress),
		e.RejectReason,
	)
	if err != nil {
		return fmt.Errorf("store: AppendM2MAuditLog: %w", err)
	}
	return nil
}

// ListM2MAuditLog returns latest-first audit entries, optionally
// filtered by client_id. Limit caps rows returned (0 → 100).
func (s *SQLiteStore) ListM2MAuditLog(ctx context.Context, clientID string, limit int) ([]M2MAuditEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `
		SELECT id, client_id, idem_key_hash, method, path, status_code, scope,
			scene_count, total_duration_seconds, ip_address, reject_reason, created_at
		FROM m2m_audit_log`
	args := []any{}
	if clientID = strings.TrimSpace(clientID); clientID != "" {
		query += " WHERE client_id = ?"
		args = append(args, clientID)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: ListM2MAuditLog: %w", err)
	}
	defer rows.Close()

	var out []M2MAuditEntry
	for rows.Next() {
		var (
			id, status, sceneCount                      int64
			clientID, idemHash, method, path, scope, ip string
			dur                                         float64
			rejectReason                                sql.NullString
			createdAt                                   string
		)
		if err := rows.Scan(&id, &clientID, &idemHash, &method, &path, &status, &scope,
			&sceneCount, &dur, &ip, &rejectReason, &createdAt); err != nil {
			return nil, fmt.Errorf("store: ListM2MAuditLog scan: %w", err)
		}
		createdT, err := parseSQLiteTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("store: ListM2MAuditLog created_at: %w", err)
		}
		out = append(out, M2MAuditEntry{
			ID:                   id,
			ClientID:             clientID,
			IdemKeyHash:          idemHash,
			Method:               method,
			Path:                 path,
			StatusCode:           int(status),
			Scope:                scope,
			SceneCount:           int(sceneCount),
			TotalDurationSeconds: dur,
			IPAddress:            ip,
			RejectReason:         rejectReason,
			CreatedAt:            createdT,
		})
	}
	return out, rows.Err()
}
