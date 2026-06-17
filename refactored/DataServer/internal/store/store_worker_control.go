package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// ---------- worker_commands (persistent command outbox) ----------

// PersistedCommand represents a command stored in SQLite.
type PersistedCommand struct {
	CommandID      string                 `json:"command_id"`
	WorkerID       string                 `json:"worker_id"`
	CommandType    string                 `json:"command_type"`
	Payload        map[string]interface{} `json:"payload,omitempty"`
	Status         string                 `json:"status"`
	SequenceNum    int64                  `json:"sequence_num"`
	CreatedAt      time.Time              `json:"created_at"`
	DeliveredAt    *time.Time             `json:"delivered_at,omitempty"`
	AckedAt        *time.Time             `json:"acked_at,omitempty"`
	ExpiresAt      *time.Time             `json:"expires_at,omitempty"`
	AttemptCount   int                    `json:"attempt_count"`
	LastError      string                 `json:"last_error,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
}

// InsertCommand inserts a new command and returns its sequence number.
func (s *SQLiteStore) InsertCommand(cmd *PersistedCommand) (int64, error) {
	if cmd.CommandID == "" || cmd.WorkerID == "" || cmd.CommandType == "" {
		return 0, fmt.Errorf("insert command: missing required fields")
	}

	payloadJSON := "{}"
	if cmd.Payload != nil {
		b, err := json.Marshal(cmd.Payload)
		if err != nil {
			return 0, fmt.Errorf("insert command: marshal payload: %w", err)
		}
		payloadJSON = string(b)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	var expiresAt sql.NullString
	if cmd.ExpiresAt != nil {
		expiresAt = sql.NullString{String: cmd.ExpiresAt.UTC().Format(time.RFC3339), Valid: true}
	}

	// Get next sequence number for this worker
	seq, err := s.nextSequence(cmd.WorkerID)
	if err != nil {
		return 0, fmt.Errorf("insert command: next sequence: %w", err)
	}

	var idempotencyKey sql.NullString
	if cmd.IdempotencyKey != "" {
		idempotencyKey = sql.NullString{String: cmd.IdempotencyKey, Valid: true}
	}

	_, err = s.db.Exec(
		`INSERT INTO worker_commands
		 (command_id, worker_id, command_type, payload_json, status, sequence_num,
		  created_at, expires_at, attempt_count, idempotency_key)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cmd.CommandID, cmd.WorkerID, cmd.CommandType, payloadJSON,
		"pending", seq, now, expiresAt, cmd.AttemptCount, idempotencyKey,
	)
	if err != nil {
		return 0, fmt.Errorf("insert command: %w", err)
	}
	return seq, nil
}

// GetPendingCommands returns all pending (not yet acked/expired) commands for a worker.
func (s *SQLiteStore) GetPendingCommands(workerID string) ([]*PersistedCommand, error) {
	rows, err := s.db.Query(
		`SELECT command_id, worker_id, command_type, payload_json, status, sequence_num,
		        created_at, delivered_at, acked_at, expires_at, attempt_count, last_error, idempotency_key
		 FROM worker_commands
		 WHERE worker_id = ? AND status IN ('pending', 'delivered')
		 ORDER BY sequence_num ASC`,
		workerID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCommands(rows)
}

// AckCommandByID marks a specific command as acknowledged by its command_id.
func (s *SQLiteStore) AckCommandByID(commandID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.Exec(
		`UPDATE worker_commands SET status = 'acked', acked_at = ? WHERE command_id = ? AND status IN ('pending', 'delivered')`,
		now, commandID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("command %s not found or already acked", commandID)
	}
	return nil
}

// AckCommandByType marks the oldest pending command of a given type for a worker as acknowledged.
func (s *SQLiteStore) AckCommandByType(workerID, commandType string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE worker_commands SET status = 'acked', acked_at = ?
		 WHERE worker_id = ? AND command_type = ? AND status IN ('pending', 'delivered')
		 ORDER BY sequence_num ASC LIMIT 1`,
		now, workerID, commandType,
	)
	return err
}

// MarkCommandDelivered marks a command as delivered.
func (s *SQLiteStore) MarkCommandDelivered(commandID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE worker_commands SET status = 'delivered', delivered_at = ?,
		        attempt_count = attempt_count + 1
		 WHERE command_id = ? AND status = 'pending'`,
		now, commandID,
	)
	return err
}

// ExpireCommands marks commands past their expiry as failed.
func (s *SQLiteStore) ExpireCommands() (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.Exec(
		`UPDATE worker_commands SET status = 'expired'
		 WHERE status IN ('pending', 'delivered') AND expires_at IS NOT NULL AND expires_at < ?`,
		now,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// CleanupOldCommands deletes acknowledged or expired commands older than the given duration.
func (s *SQLiteStore) CleanupOldCommands(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-olderThan).Format(time.RFC3339)
	result, err := s.db.Exec(
		`DELETE FROM worker_commands WHERE status IN ('acked', 'expired', 'failed') AND acked_at < ?`,
		cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// HasPendingCommand checks if a worker already has a pending command of the given type with the given idempotency key.
func (s *SQLiteStore) HasPendingCommand(workerID, commandType, idempotencyKey string) (bool, error) {
	if idempotencyKey == "" {
		// Fallback: check by type only
		var count int
		err := s.db.QueryRow(
			`SELECT COUNT(*) FROM worker_commands
			 WHERE worker_id = ? AND command_type = ? AND status IN ('pending', 'delivered')`,
			workerID, commandType,
		).Scan(&count)
		return count > 0, err
	}
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM worker_commands
		 WHERE worker_id = ? AND command_type = ? AND idempotency_key = ? AND status IN ('pending', 'delivered')`,
		workerID, commandType, idempotencyKey,
	).Scan(&count)
	return count > 0, err
}

func (s *SQLiteStore) nextSequence(workerID string) (int64, error) {
	_, err := s.db.Exec(
		`INSERT INTO worker_sequences (worker_id, next_seq_num) VALUES (?, 1)
		 ON CONFLICT(worker_id) DO UPDATE SET next_seq_num = next_seq_num + 1`,
		workerID,
	)
	if err != nil {
		return 0, err
	}
	var seq int64
	err = s.db.QueryRow(
		`SELECT next_seq_num FROM worker_sequences WHERE worker_id = ?`,
		workerID,
	).Scan(&seq)
	return seq, err
}

func scanCommands(rows *sql.Rows) ([]*PersistedCommand, error) {
	var out []*PersistedCommand
	for rows.Next() {
		var cmd PersistedCommand
		var payloadJSON string
		var createdAt, expiresAt, deliveredAt, ackedAt sql.NullString
		var lastError, idempotencyKey sql.NullString
		err := rows.Scan(
			&cmd.CommandID, &cmd.WorkerID, &cmd.CommandType, &payloadJSON,
			&cmd.Status, &cmd.SequenceNum, &createdAt, &deliveredAt, &ackedAt,
			&expiresAt, &cmd.AttemptCount, &lastError, &idempotencyKey,
		)
		if err != nil {
			continue
		}
		if payloadJSON != "" {
			_ = json.Unmarshal([]byte(payloadJSON), &cmd.Payload)
		}
		if createdAt.Valid {
			cmd.CreatedAt, _ = time.Parse(time.RFC3339, createdAt.String)
		}
		if deliveredAt.Valid {
			t, _ := time.Parse(time.RFC3339, deliveredAt.String)
			cmd.DeliveredAt = &t
		}
		if ackedAt.Valid {
			t, _ := time.Parse(time.RFC3339, ackedAt.String)
			cmd.AckedAt = &t
		}
		if expiresAt.Valid {
			t, _ := time.Parse(time.RFC3339, expiresAt.String)
			cmd.ExpiresAt = &t
		}
		if lastError.Valid {
			cmd.LastError = lastError.String
		}
		if idempotencyKey.Valid {
			cmd.IdempotencyKey = idempotencyKey.String
		}
		out = append(out, &cmd)
	}
	return out, nil
}

// ---------- worker_sessions (persistent tokens) ----------

// PersistedSession represents a worker session in SQLite.
type PersistedSession struct {
	SessionID string    `json:"session_id"`
	WorkerID  string    `json:"worker_id"`
	TokenHash string    `json:"token_hash"`
	IPAddress string    `json:"ip_address"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	LastSeen  time.Time `json:"last_seen"`
	Revoked   bool      `json:"revoked"`
}

// InsertSession creates a new session record.
func (s *SQLiteStore) InsertSession(sess *PersistedSession) error {
	if sess.SessionID == "" || sess.WorkerID == "" || sess.TokenHash == "" {
		return fmt.Errorf("insert session: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO worker_sessions (session_id, worker_id, token_hash, ip_address, created_at, expires_at, last_seen, revoked)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
		sess.SessionID, sess.WorkerID, sess.TokenHash, sess.IPAddress,
		now, sess.ExpiresAt.UTC().Format(time.RFC3339), now,
	)
	return err
}

// ValidateSession checks if a token hash maps to a valid, non-expired, non-revoked session.
func (s *SQLiteStore) ValidateSession(tokenHash string) (*PersistedSession, error) {
	now := time.Now().UTC().Format(time.RFC3339)
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
	sess.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	sess.ExpiresAt, _ = time.Parse(time.RFC3339, expiresAt)
	sess.LastSeen, _ = time.Parse(time.RFC3339, lastSeen)

	// Update last_seen
	_, _ = s.db.Exec(`UPDATE worker_sessions SET last_seen = ? WHERE session_id = ?`,
		time.Now().UTC().Format(time.RFC3339), sess.SessionID)

	return &sess, nil
}

// RevokeWorkerSessions revokes all sessions for a worker.
func (s *SQLiteStore) RevokeWorkerSessions(workerID string) error {
	_, err := s.db.Exec(`UPDATE worker_sessions SET revoked = 1 WHERE worker_id = ?`, workerID)
	return err
}

// RevokeSession revokes a single session.
func (s *SQLiteStore) RevokeSession(sessionID string) error {
	_, err := s.db.Exec(`UPDATE worker_sessions SET revoked = 1 WHERE session_id = ?`, sessionID)
	return err
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

// ---------- worker_credentials (persistent identity) ----------

// SetWorkerCredential stores or updates a worker's credential hash.
func (s *SQLiteStore) SetWorkerCredential(workerID, credentialHash string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO worker_credentials (worker_id, credential_hash, created_at, rotated_at)
		 VALUES (?, ?, ?, NULL)
		 ON CONFLICT(worker_id) DO UPDATE SET
		   credential_hash = excluded.credential_hash,
		   rotated_at = ?`,
		workerID, credentialHash, now, now,
	)
	return err
}

// ValidateWorkerCredential checks if a credential hash matches the stored one.
func (s *SQLiteStore) ValidateWorkerCredential(workerID, credentialHash string) (bool, error) {
	var stored string
	err := s.db.QueryRow(
		`SELECT credential_hash FROM worker_credentials WHERE worker_id = ?`, workerID,
	).Scan(&stored)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stored == credentialHash, nil
}

// HasWorkerCredential returns true if a credential already exists for this worker.
func (s *SQLiteStore) HasWorkerCredential(workerID string) (bool, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM worker_credentials WHERE worker_id = ?`, workerID,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// HashCredential creates a SHA-256 hex digest of a credential string.
func HashCredential(credential string) string {
	h := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(h[:])
}
