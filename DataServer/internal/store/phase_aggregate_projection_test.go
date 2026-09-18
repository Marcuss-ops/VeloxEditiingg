// phase_aggregate_projection_test.go — pins the engine-phase → attempt-metric
// projection. The projection is the only writer of the engine_* scorecard
// columns, so a phase that is not matched here is invisible in the admin
// read model even though the engine measured it.

package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
)

// TestProjectPhaseAggregates_MapsCopyOnlyPacketMuxToConcat pins the fix for
// the copy-only blind spot: in the packet-copy path the mux IS the concat
// step, and the engine measures it as engine/mixed_packet_mux (catalog
// engine.mixed_packet_mux, duration/exclusive/per_attempt). Before the
// mapping existed the row was persisted but projected nowhere, so a job whose
// entire 12.7s render was packet mux reported engine_concat_ms = 0 and the
// only visible engine figure was the opaque aggregate render span.
func TestProjectPhaseAggregates_MapsCopyOnlyPacketMuxToConcat(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	const (
		jobID     = "phase-projection-job"
		taskID    = "phase-projection-task"
		workerID  = "velox-worker-13197"
		leaseID   = "phase-projection-lease"
		attemptID = "phase-projection-attempt"
	)

	execQuery(t, s, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`, jobID, now, now, now)
	execQuery(t, s, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
		 VALUES (?, ?, '', '', 'video.assemble.copy.v1', 1,
		         'RUNNING', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, workerID, leaseID, attemptID, now, now, now)
	execQuery(t, s, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'RUNNING',
		         ?, ?, '', '', 0,
		         ?, ?)`,
		attemptID, taskID, jobID, workerID, leaseID, now, now, now, now)

	attemptRepo := NewSQLiteTaskAttemptRepository(s)
	if err := attemptRepo.PersistMetrics(ctx, taskattempts.AttemptMetrics{
		AttemptID:   attemptID,
		OutputBytes: 177_456_455,
	}); err != nil {
		t.Fatalf("PersistMetrics: %v", err)
	}

	// The two engine rows a copy-only 266s timeline emits: the aggregate render
	// span and the packet mux that produced the artifact.
	rows := []struct {
		action     string
		durationMS int64
	}{
		{"render", 12_784},
		{"mixed_packet_mux", 12_700},
	}
	for _, row := range rows {
		execQuery(t, s, ctx,
			`INSERT INTO task_phase_timings
			 (attempt_id, phase, duration_ms, wall_start, wall_end,
			  phase_order, component, action, status, metadata_json,
			  job_id, task_id, worker_id, worker_snapshot_id, executor_id, executor_version)
			 VALUES (?, 'finalize', ?, ?, ?, 0, 'engine', ?, 'ok', '{}',
			         ?, ?, ?, '', 'video.assemble.copy.v1', 1)`,
			attemptID, row.durationMS, now, now, row.action, jobID, taskID, workerID)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := projectPhaseAggregates(ctx, tx, attemptID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("projectPhaseAggregates: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := attemptRepo.GetMetrics(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if got == nil {
		t.Fatal("GetMetrics returned nil")
	}
	if got.EngineConcatMs != 12_700 {
		t.Errorf("EngineConcatMs = %d; want 12700 (copy-only packet mux is the concat step)", got.EngineConcatMs)
	}
	if got.NativeTotalMs != 12_784 {
		t.Errorf("NativeTotalMs = %d; want 12784 (aggregate engine render span)", got.NativeTotalMs)
	}
}
