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
