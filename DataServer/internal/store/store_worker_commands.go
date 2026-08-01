// Package store / store_worker_commands.go — worker_commands persistence.
// Extracted from store_worker_control.go: the persistent command outbox
// (worker_commands table).
package store

import (
	"database/sql"
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

// AckCommandByID marks a specific command as acknowledged by its command_id AND worker_id.
func (s *SQLiteStore) AckCommandByID(workerID, commandID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.Exec(
		`UPDATE worker_commands SET status = 'acked', acked_at = ? WHERE command_id = ? AND worker_id = ? AND status IN ('pending', 'delivered')`,
		now, commandID, workerID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("command %s not found, not owned by worker %s, or already acked", commandID, workerID)
	}
	return nil
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
