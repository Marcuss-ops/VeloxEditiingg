package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// store_alert_events.go owns the alert_events table (migration 107).
//
// Step 16/15 of the fleet-operator rollout. The 12-rule evaluator
// (internal/fleet/opsalerts/evaluator.go) writes 0..N ACTIVE rows
// per (worker_id, rule_id); on resolution the same row's
// resolved_at is stamped and state='RESOLVED'. The dashboard
// reads ListActiveAlertEvents (grouped by severity) and
// ListAlertEventsForWorker for drill-down.
//
// Distinct from the existing alertengine (compute-level runtime
// alerts logged + slacked via webhook): the alert_events table
// is the FLEET-OPS surface — workers, deployments, drive
// deliveries, smoke runs. The two engines complement each other;
// alertengine handles compute hot-path signals, this table
// handles fleet-wide operator signals.
//
// Retention: 30 days KEEP-WITHIN-WINDOW convention per the
// Q9 design verdict and matching worker_metrics_snapshots
// (migration 105). Pruning is a follow-up hardening commit.

const (
	AlertSeverityInfo     = "INFO"
	AlertSeverityWarning  = "WARNING"
	AlertSeverityCritical = "CRITICAL"

	AlertStateActive   = "ACTIVE"
	AlertStateResolved = "RESOLVED"
)

// ErrAlertEventNotFound is returned by GetActiveAlertEventForWorkerRule
// when no ACTIVE row exists for that (worker_id, rule_id) tuple.
// Maps to a 404 at the API boundary.
var ErrAlertEventNotFound = errors.New("no active alert event for (worker_id, rule_id)")

// AlertEvent mirrors a single row in alert_events. All time fields
// are RFC3339 in SQL; Go-side conversion at the repository boundary.
//
// Nullable fields (ResolvedAt, CurrentValue) use sql.Null* so the
// dashboard can distinguish "still active" (NULL) from "resolved
// with a value" (timestamp + value).
type AlertEvent struct {
	EventID         string
	WorkerID        string
	RuleID          string
	Severity        string
	State           string
	FiredAt         time.Time
	ResolvedAt      sql.NullTime
	LastObservedAt  time.Time
	CurrentValue    sql.NullString
	Message         string
}

// CreateAlertEventsTableIfNotExists is the test/dev-only bootstrap
// path. Production uses the migration runner from
// internal/store/migrations/sqlite/107_alert_events.sql.
//
// Idempotent; mirrors the migration DDL exactly modulo the
// partial-index on ACTIVE rows (Pragma partial indices work
// the same in CREATE TABLE IF NOT EXISTS).
func (s *SQLiteStore) CreateAlertEventsTableIfNotExists() error {
	ddl := `
CREATE TABLE IF NOT EXISTS alert_events (
  event_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL CHECK (length(worker_id) > 0),
  rule_id TEXT NOT NULL CHECK (length(rule_id) > 0),
  severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
  state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'RESOLVED')),
  fired_at TEXT NOT NULL,
  resolved_at TEXT,
  last_observed_at TEXT NOT NULL,
  current_value TEXT,
  message TEXT NOT NULL CHECK (length(message) > 0)
);
CREATE INDEX IF NOT EXISTS idx_alert_events_worker_status_time
  ON alert_events(worker_id, severity, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_events_active
  ON alert_events(state, severity, fired_at DESC)
  WHERE state = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_alert_events_severity_rule
  ON alert_events(severity, rule_id, fired_at DESC);
`
	_, err := s.db.Exec(ddl)
	return err
}

// InsertAlertEvent persists a new alert_events row.
// event_id is auto-generated if empty (hex-32 of 16 bytes — same
// shape as a uuid without dashes for compactness). fired_at +
// last_observed_at default to time.Now().UTC() if zero.
func (s *SQLiteStore) InsertAlertEvent(ctx context.Context, ev AlertEvent) error {
	if ev.WorkerID == "" {
		return errors.New("InsertAlertEvent: WorkerID empty")
	}
	if ev.RuleID == "" {
		return errors.New("InsertAlertEvent: RuleID empty")
	}
	switch ev.Severity {
	case AlertSeverityInfo, AlertSeverityWarning, AlertSeverityCritical:
	default:
		return fmt.Errorf("InsertAlertEvent: severity must be INFO|WARNING|CRITICAL, got %q", ev.Severity)
	}
	switch ev.State {
	case AlertStateActive, AlertStateResolved:
	default:
		return fmt.Errorf("InsertAlertEvent: state must be ACTIVE|RESOLVED, got %q", ev.State)
	}
	if ev.Message == "" {
		return errors.New("InsertAlertEvent: Message empty")
	}
	if ev.EventID == "" {
		ev.EventID = NewAlertEventID()
	}
	if ev.FiredAt.IsZero() {
		ev.FiredAt = time.Now().UTC()
	}
	if ev.LastObservedAt.IsZero() {
		ev.LastObservedAt = ev.FiredAt
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO alert_events
  (event_id, worker_id, rule_id, severity, state, fired_at, resolved_at, last_observed_at, current_value, message)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.EventID, ev.WorkerID, ev.RuleID, ev.Severity, ev.State,
		ev.FiredAt.UTC().Format(time.RFC3339),
		nullableTime(ev.ResolvedAt),
		ev.LastObservedAt.UTC().Format(time.RFC3339),
		nullableString(ev.CurrentValue),
		ev.Message,
	)
	return err
}

// ResolveAlertEvent stamps resolved_at + flips state to RESOLVED
// on the ACTIVE row keyed by (worker_id, rule_id, severity).
// Returns ErrAlertEventNotFound when no ACTIVE row matches
// (defensive, normally prevented by the dedup layer).
func (s *SQLiteStore) ResolveAlertEvent(ctx context.Context, workerID, ruleID, severity string, resolvedAt time.Time) error {
	if workerID == "" || ruleID == "" {
		return errors.New("ResolveAlertEvent: WorkerID or RuleID empty")
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE alert_events
SET state = 'RESOLVED', resolved_at = ?
WHERE worker_id = ? AND rule_id = ? AND severity = ? AND state = 'ACTIVE'`,
		resolvedAt.UTC().Format(time.RFC3339), workerID, ruleID, severity,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrAlertEventNotFound
	}
	return nil
}

// TouchActiveAlertEvent updates last_observed_at on the ACTIVE
// row keyed by (worker_id, rule_id, severity) without changing
// state. Used by the evaluator when a re-observation confirms
// the rule is still tripping (but the dedup window says "don't
// re-fire" — we just keep the last_observed_at fresh so the
// dashboard shows it's still an ongoing issue).
func (s *SQLiteStore) TouchActiveAlertEvent(ctx context.Context, workerID, ruleID, severity string, observedAt time.Time, currentValue string, message string) error {
	if workerID == "" || ruleID == "" {
		return errors.New("TouchActiveAlertEvent: WorkerID or RuleID empty")
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE alert_events
SET last_observed_at = ?, current_value = ?, message = ?
WHERE worker_id = ? AND rule_id = ? AND severity = ? AND state = 'ACTIVE'`,
		observedAt.UTC().Format(time.RFC3339), nullableString(sql.NullString{String: currentValue, Valid: currentValue != ""}),
		message, workerID, ruleID, severity,
	)
	return err
}

// GetActiveAlertEventForWorkerRule returns the ACTIVE row for
// (worker_id, rule_id, severity). Returns ErrAlertEventNotFound
// when no ACTIVE row matches.
func (s *SQLiteStore) GetActiveAlertEventForWorkerRule(ctx context.Context, workerID, ruleID, severity string) (*AlertEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT event_id, worker_id, rule_id, severity, state, fired_at, resolved_at, last_observed_at, current_value, message
FROM alert_events
WHERE worker_id = ? AND rule_id = ? AND severity = ? AND state = 'ACTIVE'
LIMIT 1`, workerID, ruleID, severity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, ErrAlertEventNotFound
	}
	return scanAlertEvent(rows)
}

// ListActiveAlertEvents returns all ACTIVE rows across the fleet
// for the dashboard's "currently firing" view, ordered by
// severity DESC (CRITICAL first), then fired_at ASC. limit<=0
// means "no cap"; caller caps to a sane upper bound.
func (s *SQLiteStore) ListActiveAlertEvents(ctx context.Context, limit int) ([]AlertEvent, error) {
	q := `
SELECT event_id, worker_id, rule_id, severity, state, fired_at, resolved_at, last_observed_at, current_value, message
FROM alert_events
WHERE state = 'ACTIVE'
ORDER BY
  CASE severity
    WHEN 'CRITICAL' THEN 0
    WHEN 'WARNING'  THEN 1
    WHEN 'INFO'     THEN 2
    ELSE 3
  END,
  fired_at ASC`
	args := []interface{}{}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AlertEvent, 0, 8)
	for rows.Next() {
		ev, err := scanAlertEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ev)
	}
	return out, rows.Err()
}

// ListAlertEventsForWorker returns up to limit rows (ACTIVE +
// RESOLVED) for a single worker, ordered fired_at DESC. limit<=0
// means "no cap".
func (s *SQLiteStore) ListAlertEventsForWorker(ctx context.Context, workerID string, limit int) ([]AlertEvent, error) {
	if workerID == "" {
		return nil, errors.New("ListAlertEventsForWorker: WorkerID empty")
	}
	q := `
SELECT event_id, worker_id, rule_id, severity, state, fired_at, resolved_at, last_observed_at, current_value, message
FROM alert_events
WHERE worker_id = ?
ORDER BY fired_at DESC`
	args := []interface{}{workerID}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AlertEvent, 0, 4)
	for rows.Next() {
		ev, err := scanAlertEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ev)
	}
	return out, rows.Err()
}

// ListRecentAlertEvents returns the most recent N rows across
// the fleet (state-mixed) for the dashboard's "recent events"
// view. Used for the GET /api/v1/admin/alerts/recent endpoint.
func (s *SQLiteStore) ListRecentAlertEvents(ctx context.Context, limit int) ([]AlertEvent, error) {
	q := `
SELECT event_id, worker_id, rule_id, severity, state, fired_at, resolved_at, last_observed_at, current_value, message
FROM alert_events
ORDER BY fired_at DESC`
	args := []interface{}{}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AlertEvent, 0, 8)
	for rows.Next() {
		ev, err := scanAlertEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ev)
	}
	return out, rows.Err()
}

// scanAlertEvent reads one row into an AlertEvent. Tolerates
// NULL on resolved_at + current_value (ACTIVE row has no
// resolved_at; informational rows may have NULL current_value).
func scanAlertEvent(rows *sql.Rows) (*AlertEvent, error) {
	var (
		ev            AlertEvent
		firedAt       string
		resolvedAt    sql.NullString
		lastObserved  string
		currentValue  sql.NullString
	)
	if err := rows.Scan(
		&ev.EventID, &ev.WorkerID, &ev.RuleID, &ev.Severity, &ev.State,
		&firedAt, &resolvedAt, &lastObserved, &currentValue, &ev.Message,
	); err != nil {
		return nil, err
	}
	if t, err := time.Parse(time.RFC3339, firedAt); err == nil {
		ev.FiredAt = t
	}
	if resolvedAt.Valid {
		if t, err := time.Parse(time.RFC3339, resolvedAt.String); err == nil {
			ev.ResolvedAt = sql.NullTime{Time: t, Valid: true}
		}
	}
	if t, err := time.Parse(time.RFC3339, lastObserved); err == nil {
		ev.LastObservedAt = t
	}
	if currentValue.Valid {
		ev.CurrentValue = currentValue
	}
	return &ev, nil
}

// nullableTime converts sql.NullTime to a string for SQL binding.
// Returns nil for Valid=false so the column is written as SQL NULL.
func nullableTime(t sql.NullTime) interface{} {
	if !t.Valid {
		return nil
	}
	return t.Time.UTC().Format(time.RFC3339)
}

// NewAlertEventID generates a 32-char hex string (16 random bytes)
// for the alert_events.event_id PK. Compact alternative to UUID
// with dashes so the row is grep-friendly in the audit dump.
func NewAlertEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Wallclock fallback matches fleet.NewOperationID's
		// contract; never panics on transient entropy failure.
		return fmt.Sprintf("alert-wallclock-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
