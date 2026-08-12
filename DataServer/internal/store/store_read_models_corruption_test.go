package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestAuditReadModelRejectsCorruptTimestampAndMetadata(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "audit-corruption.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO audit_events
		(id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json)
		VALUES ('audit-corrupt', 'not-a-time', 'service', 'test', 'TEST', 'job', 'job-corrupt', '', '', '', '', '{"ok":true}')`); err != nil {
		t.Fatalf("insert corrupt audit event: %v", err)
	}
	if _, err := s.ListAuditEvents(context.Background(), "job-corrupt", 10); err == nil {
		t.Fatal("ListAuditEvents returned nil error for corrupt audit timestamp")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close audit timestamp store: %v", err)
	}
	s, err = NewSQLiteStore(filepath.Join(t.TempDir(), "audit-metadata-corruption.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore metadata case: %v", err)
	}
	defer s.Close()
	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO task_execution_events
		(event_id, task_id, attempt_id, job_id, origin, scope, event_index, event_name, event_type, action, metadata_json, created_at)
		VALUES ('timeline-corrupt', 'task-corrupt', 'attempt-corrupt', 'job-corrupt', 'worker', 'task', 0, 'TEST', 'TEST', 'TEST', '{not-json}', ?)`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert corrupt timeline event: %v", err)
	}
	if _, err := s.ListAuditEvents(context.Background(), "job-corrupt", 10); err == nil {
		t.Fatal("ListAuditEvents returned nil error for corrupt timeline metadata")
	}
}

func TestDeadLetterReadModelsRejectCorruptTimestamps(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "dlq-corruption.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO dead_letter_tasks
		(id, job_id, task_id, last_attempt_id, failure_class, error_code, retryable, payload_snapshot_json, first_failed_at, last_failed_at, replay_count, status)
		VALUES ('dlq-corrupt', 'job-corrupt', 'task-corrupt', 'attempt-corrupt', 'render', 'TEST', 0, '{}', 'not-a-time', 'not-a-time', 0, 'OPEN')`); err != nil {
		t.Fatalf("insert corrupt DLQ row: %v", err)
	}
	if _, err := s.GetDeadLetter(context.Background(), "dlq-corrupt"); err == nil {
		t.Fatal("GetDeadLetter returned nil error for corrupt timestamp")
	}
	if _, err := s.ListDeadLetters(context.Background(), "", 10); err == nil {
		t.Fatal("ListDeadLetters returned nil error for corrupt timestamp")
	}
}
