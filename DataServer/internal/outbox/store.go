// Package outbox store — transactional outbox writer/claimer on SQLite.
//
// The store is intentionally minimal: producers call Insert, dispatchers
// call Claim/MarkProcessed/MarkFailed/ExtendLock. Polling/retries live in
// the dispatcher package; this package is concurrency-safe (SQLite writes
// are serialized by the storage engine).
package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Store is the database adapter for outbox events.
type Store struct {
	DB    *sql.DB
	IDGen func() string // optional; defaults to 16-byte random hex
	NowFn func() time.Time
}

// NewStore builds a Store backed by an *sql.DB.
func NewStore(db *sql.DB) *Store {
	return &Store{DB: db}
}

func (s *Store) now() time.Time {
	if s.NowFn != nil {
		return s.NowFn()
	}
	return time.Now().UTC()
}

func (s *Store) newID() string {
	if s.IDGen != nil {
		return s.IDGen()
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("ev-%d", time.Now().UnixNano())
	}
	return "ev-" + hex.EncodeToString(b[:])
}

// Insert writes a new event with status=PENDING.
//
// Producers may be guarded by a transaction with their state-change
// writes (`txn` parameter); callers in the FactoryApp pass a *sql.Tx
// for atomic write-then-enqueue. nil uses the implicit *sql.DB connection.
func (s *Store) Insert(ctx context.Context, txn Executor, p InsertParams) (string, error) {
	if p.EventType == "" || p.AggregateType == "" || p.AggregateID == "" {
		return "", ErrInvalidEvent
	}
	if len(p.Payload) == 0 {
		p.Payload = []byte("{}")
	}
	if !json.Valid(p.Payload) {
		return "", fmt.Errorf("outbox: payload is not valid JSON")
	}
	avail := p.AvailableAt
	if avail.IsZero() {
		avail = s.now()
	}
	id := s.newID()
	// RFC3339Nano (not RFC3339) — we need sub-second precision so that
	// lock_until / available_at in Claim's comparison (`locked_until <
	// now`) works correctly when a dispatcher sleeps for tens of
	// milliseconds during a crash-recovery test. RFC3339 truncates
	// fractional seconds, so two events snapped to the same wall-clock
	// second compare string-equal even though they're 50ms apart.
	now := s.now().UTC().Format(time.RFC3339Nano)

	ex := txn
	if ex == nil {
		ex = s.DB
	}
	// available_at must use the SAME fmt as the comparable nowStr in Claim
	// (`available_at <= ?`); mixing RFC3339 (no fractional) with RFC3339Nano
	// in a lexicographic comparison breaks sub-second ordering.
	q := `INSERT INTO outbox_events
		(event_id, aggregate_type, aggregate_id, event_type,
		 payload_json, status, available_at, created_at)
		VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)`
	if _, err := exec(ctx, ex, q,
		id, p.AggregateType, p.AggregateID, p.EventType,
		string(p.Payload), avail.UTC().Format(time.RFC3339Nano), now,
	); err != nil {
		return "", fmt.Errorf("outbox insert: %w", err)
	}
	return id, nil
}

// Enqueue is a thin wrapper around Insert so legacy `OutboxWriter`-style
// callers (workflow.Repository.SetOutbox wiring) can hand a fully-built
// InsertParams to the store without duplicating validation. Returns the
// generated event_id when the row commits cleanly.
func (s *Store) Enqueue(ctx context.Context, txn Executor, p InsertParams) (string, error) {
	return s.Insert(ctx, txn, p)
}

// Executor is the interface satisfied by both *sql.DB and *sql.Tx.
//
// We accept this in Insert so producers can pass either a top-level
// connection (auto-commit) or a *sql.Tx (transactional with their own
// state changes).
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func exec(ctx context.Context, ex Executor, q string, args ...any) (sql.Result, error) {
	return ex.ExecContext(ctx, q, args...)
}

// Claim atomically transitions a batch of PENDING (or expired-PROCESSING)
// rows to PROCESSING, stamping locked_by/locked_until/attempt_count.
//
// Per the PR 8 spec:
//
//	UPDATE outbox_events
//	SET status='PROCESSING', locked_by=?, locked_until=?, attempt_count=attempt_count+1
//	WHERE event_id IN (
//	    SELECT event_id FROM outbox_events
//	    WHERE status IN ('PENDING','PROCESSING')
//	      AND available_at <= ?
//	      AND (status='PENDING' OR locked_until < ?)
//	    ORDER BY created_at LIMIT ?)
//	RETURNING event_id, event_type, aggregate_type, aggregate_id, payload_json, attempt_count, created_at
//
// SQLite supports RETURNING from 3.35; earlier versions return an error
// here (fail-fast at startup rather than silently dropping events).
func (s *Store) Claim(ctx context.Context, lockedBy string, lockUntil time.Time, batchSize int) ([]Event, error) {
	if batchSize <= 0 {
		batchSize = 32
	}
	query := `
UPDATE outbox_events
SET status = 'PROCESSING',
    locked_by = ?,
    locked_until = ?,
    attempt_count = attempt_count + 1
WHERE event_id IN (
    SELECT event_id FROM outbox_events
    WHERE status IN ('PENDING','PROCESSING')
      AND available_at <= ?
      AND (
          status = 'PENDING'
          OR locked_until IS NULL
          OR locked_until < ?
      )
    ORDER BY created_at ASC
    LIMIT ?
)
RETURNING event_id, event_type, aggregate_type, aggregate_id,
          payload_json, attempt_count, created_at`

	nowStr := s.now().UTC().Format(time.RFC3339Nano)
	rows, err := s.DB.QueryContext(ctx, query,
		lockedBy, lockUntil.UTC().Format(time.RFC3339Nano),
		nowStr, nowStr,
		batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("outbox claim: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var (
			eventID, evtType, aggType, aggID, payload string
			attempt                                   int
			createdAt                                 string
		)
		if err := rows.Scan(&eventID, &evtType, &aggType, &aggID, &payload, &attempt, &createdAt); err != nil {
			return nil, fmt.Errorf("outbox claim scan: %w", err)
		}
		e := Event{
			EventID:       eventID,
			EventType:     evtType,
			AggregateType: aggType,
			AggregateID:   aggID,
			Payload:       []byte(payload),
			AttemptCount:  attempt,
		}
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			e.CreatedAt = t
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox claim rows: %w", err)
	}
	return out, nil
}

// MarkProcessed sets status=PROCESSED and processed_at=now.
//
// Called only after a Handler returned nil. Idempotent: marking an already
// PROCESSED row is a no-op.
func (s *Store) MarkProcessed(ctx context.Context, eventID string) error {
	if eventID == "" {
		return ErrInvalidEvent
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox_events
		 SET status = 'PROCESSED', processed_at = ?, locked_by = NULL,
		     locked_until = NULL, last_error = NULL
		 WHERE event_id = ? AND status = 'PROCESSING'`,
		now, eventID,
	)
	if err != nil {
		return fmt.Errorf("outbox mark processed: %w", err)
	}
	return nil
}

// MarkFailed sets status=FAILED with the recorded error.
//
// Called on permanent Handler errors or after attempt_count crosses
// MaxAttempts.
func (s *Store) MarkFailed(ctx context.Context, eventID string, lastErr string) error {
	if eventID == "" {
		return ErrInvalidEvent
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox_events
		 SET status = 'FAILED', processed_at = ?, last_error = ?,
		     locked_by = NULL, locked_until = NULL
		 WHERE event_id = ?`,
		now, lastErr, eventID,
	)
	if err != nil {
		return fmt.Errorf("outbox mark failed: %w", err)
	}
	return nil
}

// ExtendLock re-stamps locked_until and keeps status=PROCESSING.
//
// Called when a handler returned a transient error so the event remains
// visible to Claim once the lock window opens again. Calling this is
// optional — if the dispatcher just releases the row, the event re-enters
// the ready queue at the next claim tick.
func (s *Store) ExtendLock(ctx context.Context, eventID string, lockUntil time.Time, lastErr string) error {
	if eventID == "" {
		return ErrInvalidEvent
	}
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox_events
		 SET locked_until = ?, last_error = ?, locked_by = NULL
		 WHERE event_id = ? AND status = 'PROCESSING'`,
		lockUntil.UTC().Format(time.RFC3339Nano), lastErr, eventID,
	)
	if err != nil {
		return fmt.Errorf("outbox extend lock: %w", err)
	}
	return nil
}

// CountByStatus is a tiny diagnostic helper.
func (s *Store) CountByStatus(ctx context.Context, status Status) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbox_events WHERE status = ?`,
		string(status),
	).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetByID retrieves an event by primary key; useful for tests/exploration.
func (s *Store) GetByID(ctx context.Context, eventID string) (*Event, string, error) {
	row := s.DB.QueryRowContext(ctx,
		`SELECT event_id, event_type, aggregate_type, aggregate_id,
		        payload_json, status, attempt_count, last_error, created_at
		 FROM outbox_events WHERE event_id = ?`, eventID)
	var (
		eid, et, at, ai, pl, status, createdAt string
		attempt                                int
		lastErr                                sql.NullString
	)
	if err := row.Scan(&eid, &et, &at, &ai, &pl, &status, &attempt, &lastErr, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		return &Event{
			EventID:       eid,
			EventType:     et,
			AggregateType: at,
			AggregateID:   ai,
			Payload:       []byte(pl),
			AttemptCount:  attempt,
			CreatedAt:     t,
		}, status, nil
	}
	return &Event{
		EventID:       eid,
		EventType:     et,
		AggregateType: at,
		AggregateID:   ai,
		Payload:       []byte(pl),
		AttemptCount:  attempt,
	}, status, nil
}
