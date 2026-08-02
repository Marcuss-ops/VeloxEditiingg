// Package store / store_worker_sessions_list.go
//
// Read-side listing surface for worker_sessions: the paginated
// ListWorkerSessions query and its WorkerSessionRow shape. The
// mutation surface (insert/validate/revoke/cleanup) stays in
// store_worker_sessions.go.
package store

import (
	"context"
	"database/sql"
	"fmt"
)

// ListWorkerSessions returns up to `limit` sessions for `workerID`,
// newest first (ORDER BY created_at DESC).
//
// Parameters:
//
//	workerID       — exact match. Empty string returns empty.
//	includeRevoked — when false, the query filters out rows where
//	                 revoked=1. When true, both ACTIVE / DISCONNECTED
//	                 / REVOKED / EXPIRED rows are returned. The handler
//	                 uses false by default so operators do not
//	                 accidentally see revoked-session token hashes;
//	                 setting include_revoked=true on the query string
//	                 is an explicit operator action.
//	limit          — caller-supplied upper bound, clamped to [1, 1000].
//
// SECURITY: the returned struct EXCLUDES the token_hash column on
// purpose. token_hash is the SHA-256 of the session token; surfacing
// it would leak credential material. Callers (HTTP handlers) MUST
// NOT re-introduce it.
func ListWorkerSessions(ctx context.Context, db *sql.DB, workerID string, includeRevoked bool, limit int) ([]WorkerSessionRow, error) {
	if workerID == "" {
		return []WorkerSessionRow{}, nil
	}
	if limit < 1 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args := []interface{}{workerID}
	q := `SELECT session_id, worker_id, session_type, ip_address,
	       created_at, expires_at,
	       connected_at, last_seen_at, disconnected_at, disconnect_reason,
	       status, revoked,
	       protocol_version, bundle_version
	  FROM worker_sessions
	 WHERE worker_id = ?`
	if !includeRevoked {
		q += ` AND revoked = 0`
	}
	q += ` ORDER BY created_at DESC, session_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list worker sessions: %w", err)
	}
	defer rows.Close()
	out := make([]WorkerSessionRow, 0, limit)
	for rows.Next() {
		var r WorkerSessionRow
		var ipAddress sql.NullString
		var bundleVersion sql.NullString
		var connectedAt, lastSeenAt, disconnectedAt, disconnectReason sql.NullString
		var revoked int
		if err := rows.Scan(
			&r.SessionID, &r.WorkerID, &r.SessionType, &ipAddress,
			&r.CreatedAt, &r.ExpiresAt,
			&connectedAt, &lastSeenAt, &disconnectedAt, &disconnectReason,
			&r.Status, &revoked,
			&r.ProtocolVersion, &bundleVersion,
		); err != nil {
			return nil, fmt.Errorf("list worker sessions: scan: %w", err)
		}
		if ipAddress.Valid {
			r.IPAddress = ipAddress.String
		}
		if bundleVersion.Valid {
			r.BundleVersion = bundleVersion.String
		}
		if connectedAt.Valid {
			r.ConnectedAt = connectedAt.String
		}
		if lastSeenAt.Valid {
			r.LastSeenAt = lastSeenAt.String
		}
		if disconnectedAt.Valid {
			r.DisconnectedAt = disconnectedAt.String
		}
		if disconnectReason.Valid {
			r.DisconnectReason = disconnectReason.String
		}
		r.Revoked = revoked != 0
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list worker sessions: rows: %w", err)
	}
	return out, nil
}

// WorkerSessionRow is the read-side row shape returned by
// ListWorkerSessions. Mirrors the worker_sessions schema (migrations
// 020 + 094 + 095). Token_hash is deliberately absent — see the
// SECURITY note on ListWorkerSessions.
type WorkerSessionRow struct {
	SessionID        string
	WorkerID         string
	SessionType      string
	IPAddress        string
	CreatedAt        string
	ExpiresAt        string
	ConnectedAt      string
	LastSeenAt       string
	DisconnectedAt   string
	DisconnectReason string
	Status           string
	Revoked          bool
	ProtocolVersion  string
	BundleVersion    string
}
