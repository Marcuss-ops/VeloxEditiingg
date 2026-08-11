// Package store / store_worker_sessions.go — worker_sessions persistence.
// Extracted from store_worker_control.go: the persistent session tokens
// (worker_sessions table) + the session-active read model.
//
// Layout of this package:
//   - store_worker_sessions.go      — session CRUD (collision gate, insert,
//     validate, revoke, cleanup, session-active read model).
//   - store_worker_sessions_list.go — ListWorkerSessions + WorkerSessionRow
//     (read-side listing surface).
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ---------- worker_sessions (persistent tokens) ----------

// ErrWorkerIDCollision is returned by InsertSession when a session for the
// same (worker_id, session_type) is already ACTIVE on a different token
// hash. This indicates two distinct machines attempted to register the
// same worker_id; the second one is rejected to prevent dual-execution
// state corruption (per RW-PROD-005 §3 anti-collision invariant).
//
// The legitimate-reconnect path (same machine, network blip, fresh
// session) is preserved: when the existing ACTIVE session carries the
// SAME token_hash as the incoming InsertSession, the old session is
// demoted to DISCONNECTED and the new one is admitted.
var ErrWorkerIDCollision = errors.New("store: worker_id already active on a different token_hash (collision)")

// PersistedSession represents a worker session in SQLite.
type PersistedSession struct {
	SessionID   string    `json:"session_id"`
	WorkerID    string    `json:"worker_id"`
	SessionType string    `json:"session_type"`
	TokenHash   string    `json:"token_hash"`
	IPAddress   string    `json:"ip_address"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	LastSeen    time.Time `json:"last_seen"`
	Revoked     bool      `json:"revoked"`
}

// CheckActiveSessionCollision returns the token_hash of the existing ACTIVE
// session for (workerID, sessionType), or "" if none. Callers use this to
// distinguish a legitimate reconnect (same token_hash) from a worker_id
// collision (different token_hash) before invoking InsertSession.
//
// Returns:
//   - existingTokenHash != "" + same hash → legit reconnect (proceed with demote + INSERT)
//   - existingTokenHash != "" + different hash → collision (caller MUST reject)
//   - existingTokenHash == "" + no err → no active session (caller proceeds with INSERT)
//
// The SELECT is intentionally NOT wrapped in a transaction with the subsequent
// INSERT: the SQLite trigger `trg_worker_sessions_one_active` (migration 094 +
// 095) is the authoritative race-safe gate. Two concurrent inserts with the
// same worker_id serialize through the trigger; the second one fails with
// `worker already has an active session of this type` and the caller surfaces
// that as ErrWorkerIDCollision via the post-failure SELECT (token_hash probe
// inside the caller-side handleInsertError helper below).
func (s *SQLiteStore) CheckActiveSessionCollision(workerID, sessionType string) (existingTokenHash string, err error) {
	if workerID == "" || sessionType == "" {
		return "", fmt.Errorf("check collision: missing worker_id or session_type")
	}
	row := s.db.QueryRow(
		`SELECT token_hash FROM worker_sessions
		 WHERE worker_id = ? AND session_type = ? AND status = 'ACTIVE' AND revoked = 0
		 ORDER BY last_seen_at DESC, created_at DESC LIMIT 1`,
		workerID, sessionType,
	)
	err = row.Scan(&existingTokenHash)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("check collision: %w", err)
	}
	return existingTokenHash, nil
}

// InsertSession creates a new session record.
//
// Pre-flight anti-collision gate: if a session for (worker_id, session_type)
// is already ACTIVE, the caller MUST have already verified via
// CheckActiveSessionCollision that the existing token_hash matches (legitimate
// reconnect) or rejected the request (collision). InsertSession performs a
// second defensive SELECT + token_hash check before mutating state, so a
// caller that skipped CheckActiveSessionCollision still cannot admit a
// colliding insert. The post-INSERT trigger (`trg_worker_sessions_one_active`,
// migration 095) remains the authoritative race-safe backstop for concurrent
// inserts that slip past the SELECT.
func (s *SQLiteStore) InsertSession(sess *PersistedSession) error {
	if sess.SessionID == "" || sess.WorkerID == "" || sess.TokenHash == "" {
		return fmt.Errorf("insert session: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	sessionType := sess.SessionType
	if sessionType == "" {
		sessionType = "control"
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("insert session begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Keep the collision probe, reconnect demotion, and new-session insert in
	// one transaction. A failed reconnect must not revoke the previously
	// active session halfway through the operation.
	var existingTokenHash string
	err = tx.QueryRow(`
		SELECT token_hash FROM worker_sessions
		 WHERE worker_id = ? AND session_type = ? AND status = 'ACTIVE' AND revoked = 0
		 ORDER BY last_seen_at DESC, created_at DESC LIMIT 1`,
		sess.WorkerID, sessionType).Scan(&existingTokenHash)
	if err == sql.ErrNoRows {
		existingTokenHash = ""
	} else if err != nil {
		return fmt.Errorf("insert session collision probe: %w", err)
	}
	if existingTokenHash != "" && existingTokenHash != sess.TokenHash {
		return fmt.Errorf("%w: worker_id=%s session_type=%s existing_ip_pending=%s",
			ErrWorkerIDCollision, sess.WorkerID, sessionType, sess.SessionID)
	}

	if _, err := tx.Exec(`UPDATE worker_sessions
		SET status='DISCONNECTED', disconnected_at=?, disconnect_reason='replaced', revoked=1
		WHERE worker_id=? AND session_type=? AND status='ACTIVE' AND revoked=0`,
		now, sess.WorkerID, sessionType); err != nil {
		return fmt.Errorf("insert session demote: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO worker_sessions (session_id, worker_id, token_hash, ip_address, created_at, expires_at, last_seen, revoked, status, connected_at, last_seen_at, session_type)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'ACTIVE', ?, ?, ?)`,
		sess.SessionID, sess.WorkerID, sess.TokenHash, sess.IPAddress,
		now, sess.ExpiresAt.UTC().Format(time.RFC3339), now, now, now, sessionType,
	); err != nil {
		if strings.Contains(err.Error(), "worker already has an active session") {
			return fmt.Errorf("%w: concurrent insert collision", ErrWorkerIDCollision)
		}
		return fmt.Errorf("insert session row: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("insert session commit: %w", err)
	}
	return nil
}

// DeleteWorkerRuntimeSnapshotBySession removes a snapshot that was created
// during a failed Hello admission before any TaskAttempt could reference it.
// It is intentionally scoped to the exact worker/session pair.
func (s *SQLiteStore) DeleteWorkerRuntimeSnapshotBySession(workerID, sessionID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("worker runtime snapshot: store not initialized")
	}
	_, err := s.db.Exec(`DELETE FROM worker_runtime_snapshots WHERE worker_id = ? AND session_id = ?`, workerID, sessionID)
	if err != nil {
		return fmt.Errorf("worker runtime snapshot delete: %w", err)
	}
	return nil
}

// ValidateSession checks if a token hash maps to a valid, non-expired, non-revoked session.
func (s *SQLiteStore) ValidateSession(tokenHash string) (*PersistedSession, error) {
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339)
	row := s.db.QueryRow(
		`SELECT session_id, worker_id, token_hash, ip_address, created_at, expires_at, last_seen, revoked
		 FROM worker_sessions
		 WHERE token_hash = ? AND revoked = 0 AND expires_at > ?`,
		tokenHash, now,
	)
	var sess PersistedSession
	var createdAt, expiresAt, lastSeen string
	err := row.Scan(&sess.SessionID, &sess.WorkerID, &sess.TokenHash, &sess.IPAddress,
		&createdAt, &expiresAt, &lastSeen, &sess.Revoked)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sess.CreatedAt, err = parseWorkerSessionTime(createdAt, "created_at")
	if err != nil {
		return nil, err
	}
	sess.ExpiresAt, err = parseWorkerSessionTime(expiresAt, "expires_at")
	if err != nil {
		return nil, err
	}
	sess.LastSeen, err = parseWorkerSessionTime(lastSeen, "last_seen")
	if err != nil {
		return nil, err
	}

	// Updating last_seen is part of successful validation. If it cannot be
	// persisted, do not return a valid session based on stale durable state.
	if _, err := s.db.Exec(`UPDATE worker_sessions SET last_seen = ? WHERE session_id = ?`, now, sess.SessionID); err != nil {
		return nil, fmt.Errorf("validate session update last_seen: %w", err)
	}
	sess.LastSeen = nowTime

	return &sess, nil
}

// ValidateSessionByID looks up a session by ID and returns it if valid (not expired, not revoked).
func (s *SQLiteStore) ValidateSessionByID(sessionID string) (*PersistedSession, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	row := s.db.QueryRow(
		`SELECT session_id, worker_id, token_hash, ip_address, created_at, expires_at, last_seen, revoked
		 FROM worker_sessions
		 WHERE session_id = ? AND revoked = 0 AND expires_at > ?`,
		sessionID, now,
	)
	var sess PersistedSession
	var createdAt, expiresAt, lastSeen string
	err := row.Scan(&sess.SessionID, &sess.WorkerID, &sess.TokenHash, &sess.IPAddress,
		&createdAt, &expiresAt, &lastSeen, &sess.Revoked)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sess.CreatedAt, err = parseWorkerSessionTime(createdAt, "created_at")
	if err != nil {
		return nil, err
	}
	sess.ExpiresAt, err = parseWorkerSessionTime(expiresAt, "expires_at")
	if err != nil {
		return nil, err
	}
	sess.LastSeen, err = parseWorkerSessionTime(lastSeen, "last_seen")
	if err != nil {
		return nil, err
	}
	return &sess, nil
}

func parseWorkerSessionTime(value, field string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("worker session: invalid %s: %w", field, err)
	}
	return parsed, nil
}

// UpdateSessionLastSeen bumps the last_seen timestamp for a session.
func (s *SQLiteStore) UpdateSessionLastSeen(sessionID string) error {
	_, err := s.db.Exec(`UPDATE worker_sessions SET last_seen = ? WHERE session_id = ?`,
		time.Now().UTC().Format(time.RFC3339), sessionID)
	return err
}

// RevokeWorkerSessions revokes all sessions for a worker.
func (s *SQLiteStore) RevokeWorkerSessions(workerID string) error {
	_, err := s.db.Exec(`UPDATE worker_sessions SET revoked = 1 WHERE worker_id = ?`, workerID)
	return err
}

// RevokeSession revokes a single session and closes its immutable runtime
// snapshot, when one exists. Keeping both lifecycle records aligned lets
// historical attempt queries distinguish a connected runtime from one that
// had already disconnected.
func (s *SQLiteStore) RevokeSession(sessionID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("revoke session begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var statusColumnCount, disconnectedColumnCount int
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info('worker_sessions') WHERE name = 'status'`).Scan(&statusColumnCount); err != nil {
		return fmt.Errorf("revoke session status schema probe: %w", err)
	}
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info('worker_sessions') WHERE name = 'disconnected_at'`).Scan(&disconnectedColumnCount); err != nil {
		return fmt.Errorf("revoke session disconnected schema probe: %w", err)
	}

	var revokeQuery string
	switch {
	case statusColumnCount == 1 && disconnectedColumnCount == 1:
		revokeQuery = `UPDATE worker_sessions
			SET revoked = 1, status = CASE WHEN status = 'ACTIVE' THEN 'DISCONNECTED' ELSE status END,
			    disconnected_at = COALESCE(disconnected_at, ?)
			WHERE session_id = ?`
	case statusColumnCount == 1:
		revokeQuery = `UPDATE worker_sessions
			SET revoked = 1, status = CASE WHEN status = 'ACTIVE' THEN 'DISCONNECTED' ELSE status END
			WHERE session_id = ?`
	case disconnectedColumnCount == 1:
		revokeQuery = `UPDATE worker_sessions
			SET revoked = 1, disconnected_at = COALESCE(disconnected_at, ?)
			WHERE session_id = ?`
	default:
		revokeQuery = `UPDATE worker_sessions SET revoked = 1 WHERE session_id = ?`
	}
	var revokeArgs []interface{}
	if statusColumnCount == 1 && disconnectedColumnCount == 1 {
		revokeArgs = []interface{}{now, sessionID}
	} else if statusColumnCount == 1 || disconnectedColumnCount == 1 {
		if disconnectedColumnCount == 1 {
			revokeArgs = []interface{}{now, sessionID}
		} else {
			revokeArgs = []interface{}{sessionID}
		}
	} else {
		revokeArgs = []interface{}{sessionID}
	}
	if _, err := tx.Exec(revokeQuery, revokeArgs...); err != nil {
		return fmt.Errorf("revoke session row: %w", err)
	}
	var snapshotTableCount int
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		 WHERE type = 'table' AND name = 'worker_runtime_snapshots'`).Scan(&snapshotTableCount); err != nil {
		return fmt.Errorf("revoke runtime snapshot schema probe: %w", err)
	}
	if snapshotTableCount == 1 {
		if _, err := tx.Exec(`UPDATE worker_runtime_snapshots
			SET disconnected_at = COALESCE(disconnected_at, ?)
			WHERE session_id = ?`, now, sessionID); err != nil {
			return fmt.Errorf("revoke runtime snapshot: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("revoke session commit: %w", err)
	}
	return nil
}

// CleanupExpiredSessions deletes sessions that are expired or revoked for more than 24h.
func (s *SQLiteStore) CleanupExpiredSessions() (int64, error) {
	cutoff := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	result, err := s.db.Exec(
		`DELETE FROM worker_sessions WHERE expires_at < ? OR (revoked = 1 AND last_seen < ?)`,
		time.Now().UTC().Format(time.RFC3339), cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// WorkerSessionFreshnessWindow — a session is only considered active if its
// `last_seen` is within this window, IN ADDITION to revoked=0 + expires_at>now.
// Matches workers.ConnectionDisconnectedThreshold (5 min) so the canonical
// state derivation can render a CONNECTED worker that has a live session
// AND a recent heartbeat-side bump; without this gate, a worker idle for
// 24h whose session expires in 1h would falsely read as CONNECTED.
const WorkerSessionFreshnessWindow = 5 * time.Minute

// IsSessionActive returns true if workerID has at least one non-revoked,
// non-expired session whose last_seen is inside WorkerSessionFreshnessWindow.
// Used by the registry to plumb `session_active` into the worker read model
// (PR: CONNECTED/STALE/DISCONNECTED semantics). Returns false on DB error
// to keep the conservative (DISCONNECTED) verdict.
func (s *SQLiteStore) IsSessionActive(workerID string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	freshSince := time.Now().UTC().Add(-WorkerSessionFreshnessWindow).Format(time.RFC3339)
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM worker_sessions
		 WHERE worker_id = ? AND revoked = 0 AND expires_at > ?
		   AND last_seen > ?`,
		workerID, now, freshSince,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetActiveSessionsByWorkerIDs bulk-fetches which of the given workerIDs
// currently have a non-revoked, non-expired session. Returns a map keyed by
// workerID with value=true. Used by Registry.List / StatusSnapshot to avoid
// N+1 queries when computing session_active across the fleet.
func (s *SQLiteStore) GetActiveSessionsByWorkerIDs(workerIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(workerIDs))
	if len(workerIDs) == 0 {
		return out, nil
	}
	placeholders := make([]string, len(workerIDs))
	// Bind order MUST be: now, freshSince, worker_ids... matching the SQL
	// placeholders `expires_at > ? AND last_seen > ? AND worker_id IN (?,?,…)`.
	args := make([]interface{}, 0, len(workerIDs)+2)
	nowStr := time.Now().UTC().Format(time.RFC3339)
	freshSinceStr := time.Now().UTC().Add(-WorkerSessionFreshnessWindow).Format(time.RFC3339)
	args = append(args, nowStr, freshSinceStr)
	for i, id := range workerIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	q := `SELECT DISTINCT worker_id FROM worker_sessions
	      WHERE revoked = 0 AND expires_at > ?
	        AND last_seen > ?
	        AND worker_id IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
