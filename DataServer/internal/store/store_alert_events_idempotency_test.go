package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestInsertAlertEventIsIdempotentByEventID(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/alert-events-idempotency.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()
	if err := s.CreateAlertEventsTableIfNotExists(); err != nil {
		t.Fatalf("CreateAlertEventsTableIfNotExists: %v", err)
	}

	event := AlertEvent{
		EventID:        "canonical-event-1",
		WorkerID:       "worker-1",
		RuleID:         "disk_pressure",
		Severity:       AlertSeverityWarning,
		State:          AlertStateActive,
		FiredAt:        time.Unix(100, 0).UTC(),
		LastObservedAt: time.Unix(100, 0).UTC(),
		CurrentValue:   sql.NullString{String: "disk=92%", Valid: true},
		Message:        "disk pressure",
	}
	if err := s.InsertAlertEvent(context.Background(), event); err != nil {
		t.Fatalf("first InsertAlertEvent: %v", err)
	}
	if err := s.InsertAlertEvent(context.Background(), event); err != nil {
		t.Fatalf("identical retry InsertAlertEvent: %v", err)
	}
	event.Message = "different payload"
	if err := s.InsertAlertEvent(context.Background(), event); !errors.Is(err, ErrAlertEventConflict) {
		t.Fatalf("mismatched EventID replay error = %v, want ErrAlertEventConflict", err)
	}

	rows, err := s.ListAlertEventsForWorker(context.Background(), "worker-1", 0)
	if err != nil {
		t.Fatalf("ListAlertEventsForWorker: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want exactly one idempotent event", len(rows))
	}
	if rows[0].Message != "disk pressure" {
		t.Fatalf("retry overwrote original event: message=%q", rows[0].Message)
	}
}

func TestTouchActiveAlertEventFailsClosedWhenRowIsMissing(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/alert-events-touch.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()
	if err := s.CreateAlertEventsTableIfNotExists(); err != nil {
		t.Fatalf("CreateAlertEventsTableIfNotExists: %v", err)
	}

	err = s.TouchActiveAlertEvent(context.Background(), "worker-1", "disk_pressure", AlertSeverityWarning, time.Now().UTC(), "disk=92%", "disk pressure")
	if !errors.Is(err, ErrAlertEventNotFound) {
		t.Fatalf("TouchActiveAlertEvent error = %v, want ErrAlertEventNotFound", err)
	}
}

func TestAlertEventsRejectCorruptTimestamps(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/alert-events-corrupt-time.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()
	if err := s.CreateAlertEventsTableIfNotExists(); err != nil {
		t.Fatalf("CreateAlertEventsTableIfNotExists: %v", err)
	}
	if _, err := s.db.ExecContext(context.Background(), `
INSERT INTO alert_events(event_id, worker_id, rule_id, severity, state, fired_at, resolved_at, last_observed_at, current_value, message)
VALUES ('corrupt-time', 'worker-1', 'disk_pressure', 'WARNING', 'ACTIVE', 'not-a-time', NULL, 'not-a-time', NULL, 'disk pressure')`); err != nil {
		t.Fatalf("insert corrupt event: %v", err)
	}
	if _, err := s.ListActiveAlertEvents(context.Background(), 0); err == nil {
		t.Fatal("ListActiveAlertEvents returned nil error for corrupt timestamp")
	}
}
