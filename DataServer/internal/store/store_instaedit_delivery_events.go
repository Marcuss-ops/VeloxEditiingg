package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// InstaEditDeliveryEvent is the persisted, already-authenticated callback
// projection. The HTTP signature and timestamp are verified by the handler;
// this type contains only data needed to reconcile job_deliveries.
type InstaEditDeliveryEvent struct {
	EventID          string
	DeliveryID       string
	SocialDeliveryID string
	Sequence         int64
	Status           string
	Phase            string
	RemoteID         string
	RemoteURL        string
	ErrorCode        string
	ErrorMessage     string
	OccurredAt       time.Time
	Payload          json.RawMessage
}

// ApplyInstaEditDeliveryEvent atomically records a callback and applies it
// only when it is newer than the last event for that delivery. Replays and
// lower sequence numbers are successful no-ops, so the HTTP endpoint can
// always acknowledge a valid event quickly.
func (s *SQLiteStore) ApplyInstaEditDeliveryEvent(ctx context.Context, event InstaEditDeliveryEvent) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store: callback event store is not configured")
	}
	if event.EventID == "" || event.DeliveryID == "" || event.Sequence <= 0 || event.Status == "" {
		return false, fmt.Errorf("store: callback event is incomplete")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	payload := event.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: begin callback event: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO instaedit_delivery_events
		(event_id, delivery_id, sequence, status, payload_json, occurred_at, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.EventID, event.DeliveryID, event.Sequence, event.Status,
		string(payload), event.OccurredAt.UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, fmt.Errorf("store: insert callback event: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: callback event rows affected: %w", err)
	}
	if inserted == 0 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("store: commit callback replay: %w", err)
		}
		return false, nil
	}

	var current int64
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(sequence), 0)
		FROM instaedit_delivery_events
		WHERE delivery_id = ?`, event.DeliveryID).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("store: read callback sequence: %w", err)
	}
	if current != event.Sequence {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("store: commit stale callback: %w", err)
		}
		return false, nil
	}

	status := callbackDeliveryStatus(event.Status)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `
		UPDATE job_deliveries
		SET status = ?,
		    remote_id = CASE WHEN ? <> '' THEN ? ELSE remote_id END,
		    remote_url = CASE WHEN ? <> '' THEN ? ELSE remote_url END,
		    last_error_code = CASE WHEN ? <> '' THEN ? ELSE last_error_code END,
		    last_error_message = CASE WHEN ? <> '' THEN ? ELSE last_error_message END,
		    completed_at = CASE WHEN ? IN ('SUCCEEDED', 'FAILED', 'BLOCKED_AUTH', 'CANCELLED') THEN ? ELSE completed_at END,
		    updated_at = ?
		WHERE delivery_id = ?`,
		status,
		event.RemoteID, event.RemoteID,
		event.RemoteURL, event.RemoteURL,
		event.ErrorCode, event.ErrorCode,
		event.ErrorMessage, event.ErrorMessage,
		status, event.OccurredAt.UTC().Format(time.RFC3339Nano), now,
		event.DeliveryID)
	if err != nil {
		return false, fmt.Errorf("store: apply callback delivery: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit callback delivery: %w", err)
	}
	return true, nil
}

func callbackDeliveryStatus(status string) string {
	switch status {
	case "published", "publication_completed", "completed":
		return "SUCCEEDED"
	case "failed", "dead_letter":
		return "FAILED"
	case "blocked_auth":
		return "BLOCKED_AUTH"
	case "retry_wait", "rate_limited":
		return "RETRY_WAIT"
	case "cancel_requested", "cancelled":
		return "CANCELLED"
	default:
		return "RUNNING"
	}
}
