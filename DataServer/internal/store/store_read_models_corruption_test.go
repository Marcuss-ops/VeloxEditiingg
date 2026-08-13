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

func TestOperatorReadModelsRejectCorruption(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "operator-read-corruption.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO smoke_runs
		(run_id, worker_id, started_at, finished_at, duration_ms, asset_id, status, requested_by)
		VALUES ('smoke-corrupt', 'worker-corrupt', 'not-a-time', 'not-a-time', 0, 'asset', 'PENDING', 'test')`); err != nil {
		t.Fatalf("insert corrupt smoke row: %v", err)
	}
	if _, err := s.ListRecentSmokesForWorker(context.Background(), "worker-corrupt", 10); err == nil {
		t.Fatal("ListRecentSmokesForWorker returned nil error for corrupt timestamp")
	}

	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO worker_metrics_snapshots
		(worker_id, snapshotted_at)
		VALUES ('worker-corrupt', 'not-a-time')`); err != nil {
		t.Fatalf("insert corrupt metrics row: %v", err)
	}
	if _, err := s.GetLatestWorkerMetricsForWorker(context.Background(), "worker-corrupt"); err == nil {
		t.Fatal("GetLatestWorkerMetricsForWorker returned nil error for corrupt timestamp")
	}

	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO fleet_operations
		(operation_id, worker_id, op, requested_by, reason, status, queued_at, payload)
		VALUES ('operation-corrupt', 'worker-corrupt', 'drain', 'test', 'corruption test', 'QUEUED', 'not-a-time', '{not-json}')`); err != nil {
		t.Fatalf("insert corrupt operation row: %v", err)
	}
	if _, err := s.GetOperation(context.Background(), "operation-corrupt"); err == nil {
		t.Fatal("GetOperation returned nil error for corrupt operation row")
	}
}
