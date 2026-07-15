package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// TestE2E_FailedAttempt_PartialPhaseMetrics verifies that a FAILED attempt
// persists partial-progress counters, failure attribution, and per-phase
// timings so operators can diagnose where the attempt stopped.
func TestE2E_FailedAttempt_PartialPhaseMetrics(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)

	const (
		jobID      = "e2e-failed-job-001"
		taskID     = "e2e-failed-task-001"
		workerID   = "worker-failed-01"
		leaseID    = "lease-failed-001"
		attemptID  = "e2e-failed-attempt-001"
		executorID = "scene.composite.v1"
	)

	// ── 0. Seed job, task, and attempt ─────────────────────────────────
	seedDestinations(t, store, map[string]bool{"drive-main": true})

	execQuery(t, store, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`,
		jobID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
		 VALUES (?, ?, '', '', ?, 3,
		         'RUNNING', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, executorID, workerID, leaseID, attemptID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`,
		taskID, executorID, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'RUNNING',
		         ?, ?, '', '', 0,
		         ?, ?)`,
		attemptID, taskID, jobID, workerID, leaseID,
		nowStr, nowStr, nowStr, nowStr,
	)

	// ── 1. Ingest a FAILED result with partial phase metrics ────────────
	taskRepo := NewSQLiteTaskRepository(store)
	cmd := taskgraph.IngestResultCommand{
		TaskID:        taskID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptID:     attemptID,
		TaskStatus:    taskgraph.StatusFailed,
		AttemptStatus: taskattempts.AttemptStatusFailed,
		ErrorCode:     "RENDER_ENGINE_CRASH",
		ErrorMsg:      "native process exited with code 139",
		RawReportJSON: `{"task_id":"` + taskID + `","attempt_id":"` + attemptID + `","status":"failed"}`,
		Metrics: taskattempts.AttemptMetrics{
			AttemptID:           attemptID,
			InputBytes:          1048576,
			OutputBytes:         0,
			CPUTimeMS:           5120,
			PeakRSSBytes:        536870912,
			CompletedSegments:   7,
			WastedCPUMS:         5120,
			WastedDownloadBytes: 1048576,
			ErrorComponent:      "engine",
			ErrorPhase:          "render",
		},
		PartialPhaseMetrics: []taskattempts.PhaseTimingDetailed{
			{
				AttemptID:    attemptID,
				JobID:        jobID,
				TaskID:       taskID,
				WorkerID:     workerID,
				PhaseOrder:   1,
				Component:    "download",
				Action:       "asset_fetch",
				DurationMS:   2500,
				Status:       "ok",
				BytesIn:      1048576,
				BytesOut:     1048576,
				Frames:       0,
				MetadataJSON: `{"source":"drive"}`,
			},
			{
				AttemptID:    attemptID,
				JobID:        jobID,
				TaskID:       taskID,
				WorkerID:     workerID,
				PhaseOrder:   2,
				Component:    "render",
				Action:       "segment_build",
				DurationMS:   3200,
				Status:       "failed",
				ErrorCode:    "SEGMENT_BUILD_TIMEOUT",
				ErrorMessage: "segment 7 exceeded 30s budget",
				BytesIn:      1048576,
				BytesOut:     0,
				Frames:       180,
				MetadataJSON: `{"segment_index":7}`,
			},
		},
	}
	if err := taskRepo.IngestTaskResultAtomic(ctx, cmd); err != nil {
		t.Fatalf("IngestTaskResultAtomic FAILED path: %v", err)
	}

	// ── 2. Verify task and attempt are terminal ────────────────────────
	var taskStatus, attemptStatus, storedErrorCode string
	if err := store.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`, taskID,
	).Scan(&taskStatus); err != nil {
		t.Fatalf("SELECT tasks.status: %v", err)
	}
	if taskStatus != "FAILED" {
		t.Errorf("tasks.status = %q; want FAILED", taskStatus)
	}

	if err := store.db.QueryRowContext(ctx,
		`SELECT status, error_code FROM task_attempts WHERE id = ?`, attemptID,
	).Scan(&attemptStatus, &storedErrorCode); err != nil {
		t.Fatalf("SELECT task_attempts: %v", err)
	}
	if attemptStatus != "FAILED" {
		t.Errorf("task_attempts.status = %q; want FAILED", attemptStatus)
	}
	if storedErrorCode != "RENDER_ENGINE_CRASH" {
		t.Errorf("task_attempts.error_code = %q; want RENDER_ENGINE_CRASH", storedErrorCode)
	}

	// ── 3. Verify failure/partial metrics persisted ───────────────────
	var (
		completedSegments int
		wastedCPU         int64
		wastedDownload    int64
		errorComponent    string
		errorPhase        string
	)
	if err := store.db.QueryRowContext(ctx,
		`SELECT completed_segments, wasted_cpu_ms, wasted_download_bytes,
		        error_component, error_phase
		 FROM task_attempt_metrics WHERE attempt_id = ?`,
		attemptID,
	).Scan(&completedSegments, &wastedCPU, &wastedDownload, &errorComponent, &errorPhase); err != nil {
		t.Fatalf("SELECT task_attempt_metrics: %v", err)
	}
	if completedSegments != 7 {
		t.Errorf("completed_segments = %d; want 7", completedSegments)
	}
	if wastedCPU != 5120 {
		t.Errorf("wasted_cpu_ms = %d; want 5120", wastedCPU)
	}
	if wastedDownload != 1048576 {
		t.Errorf("wasted_download_bytes = %d; want 1048576", wastedDownload)
	}
	if errorComponent != "engine" {
		t.Errorf("error_component = %q; want engine", errorComponent)
	}
	if errorPhase != "render" {
		t.Errorf("error_phase = %q; want render", errorPhase)
	}

	// ── 4. Verify partial phase timings persisted ──────────────────────
	rows, err := store.db.QueryContext(ctx,
		`SELECT phase, component, action, status, error_code, duration_ms, bytes_in, frames
		 FROM task_phase_timings WHERE attempt_id = ? ORDER BY phase_order ASC`,
		attemptID,
	)
	if err != nil {
		t.Fatalf("SELECT task_phase_timings: %v", err)
	}
	defer rows.Close()

	type phaseRow struct {
		phase, component, action, status, errorCode string
		durationMS, bytesIn, frames                 int64
	}
	var got []phaseRow
	for rows.Next() {
		var r phaseRow
		if err := rows.Scan(&r.phase, &r.component, &r.action, &r.status, &r.errorCode,
			&r.durationMS, &r.bytesIn, &r.frames); err != nil {
			t.Fatalf("scan phase row: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("phase timings rows = %d; want 2", len(got))
	}

	want := []phaseRow{
		{phase: "download.asset_fetch", component: "download", action: "asset_fetch", status: "ok", errorCode: "", durationMS: 2500, bytesIn: 1048576, frames: 0},
		{phase: "render.segment_build", component: "render", action: "segment_build", status: "failed", errorCode: "SEGMENT_BUILD_TIMEOUT", durationMS: 3200, bytesIn: 1048576, frames: 180},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("phase[%d] = %+v; want %+v", i, got[i], want[i])
		}
	}

	t.Log("E2E FAILED attempt with partial phase metrics: PASS")
}
