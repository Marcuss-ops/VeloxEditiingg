package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// =====================================================================
// AcceptTaskAtomic tests
// =====================================================================
//
// AcceptTaskAtomic atomically transitions a Task from LEASED → RUNNING
// AND UPDATES the matching PENDING TaskAttempt to RUNNING AND promotes
// the parent Job from PENDING/RETRY_WAIT to RUNNING in ONE transaction.
// The §9.5 invariant guarantees that every Task RUNNING has a matching
// active Attempt. Each test below pins a guard around that contract.

// TestAcceptTaskAtomic_HappyPath: LEASED + matching worker/lease/revision
// ⇒ Task RUNNING AND attempt UPDATE both committed atomically.
func TestAcceptTaskAtomic_HappyPath(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-1", "w-1", "L-1", "A-accept-1", 1, 0)

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-1",
		TaskID:        "T-accept-1",
		JobID:         "job-T-accept-1",
		WorkerID:      "w-1",
		LeaseID:       "L-1",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("AcceptTaskAtomic happy path: %v", err)
	}

	var taskStatus, workerID, leaseID string
	var revision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id, revision FROM tasks WHERE task_id = ?`,
		"T-accept-1").Scan(&taskStatus, &workerID, &leaseID, &revision); err != nil {
		t.Fatalf("post-accept SELECT tasks: %v", err)
	}
	if taskStatus != "RUNNING" {
		t.Errorf("tasks.status = %s; want RUNNING", taskStatus)
	}
	if revision != 1 {
		t.Errorf("tasks.revision = %d; want 1 (CAS increment)", revision)
	}
	if workerID != "w-1" || leaseID != "L-1" {
		t.Errorf("worker/lease drifted: w=%s L=%s", workerID, leaseID)
	}

	// §9.5 invariant (positive case): Task RUNNING ⇒ attempt RUNNING exists.
	att := attemptForTask(t, s.db, "T-accept-1", "w-1", "L-1")
	if att == nil {
		t.Fatal("active attempt missing after AcceptTaskAtomic")
	}
	if att.Status != taskattempts.AttemptStatusRunning {
		t.Errorf("task_attempts.status = %s; want RUNNING", att.Status)
	}

	var jobStatus, jobStartedAt string
	var jobRevision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(started_at, ''), revision FROM jobs WHERE job_id = ?`,
		"job-T-accept-1").Scan(&jobStatus, &jobStartedAt, &jobRevision); err != nil {
		t.Fatalf("post-accept SELECT jobs: %v", err)
	}
	if jobStatus != "RUNNING" {
		t.Errorf("jobs.status = %s; want RUNNING", jobStatus)
	}
	if jobStartedAt == "" {
		t.Errorf("jobs.started_at empty; want RFC3339 timestamp")
	}
	if jobRevision != 1 {
		t.Errorf("jobs.revision = %d; want 1", jobRevision)
	}

	var runtimeWorkerID, runtimeAttemptID, runtimeStatus, runtimeStartedAt string
	if err := s.db.QueryRowContext(ctx,
		`SELECT worker_id, attempt_id, runtime_status, started_at
		   FROM worker_task_runtime WHERE job_id = ?`,
		"job-T-accept-1").Scan(&runtimeWorkerID, &runtimeAttemptID, &runtimeStatus, &runtimeStartedAt); err != nil {
		t.Fatalf("post-accept SELECT worker_task_runtime: %v", err)
	}
	if runtimeWorkerID != "w-1" {
		t.Fatalf("live runtime worker_id = %q; want w-1 (must never be null/empty after RUNNING)", runtimeWorkerID)
	}
	if runtimeAttemptID != "A-accept-1" {
		t.Fatalf("live runtime attempt_id = %q; want A-accept-1", runtimeAttemptID)
	}
	if runtimeStatus != "RUNNING" {
		t.Fatalf("live runtime status = %q; want RUNNING", runtimeStatus)
	}
	if runtimeStartedAt == "" {
		t.Fatal("live runtime started_at empty; want lease acceptance timestamp")
	}
	var runtimeLastProgressAt string
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(last_progress_at, '') FROM worker_task_runtime WHERE job_id = ?`,
		"job-T-accept-1").Scan(&runtimeLastProgressAt); err != nil {
		t.Fatalf("post-accept SELECT worker_task_runtime last_progress_at: %v", err)
	}
	if runtimeLastProgressAt != "" {
		t.Fatalf("live runtime last_progress_at = %q; want empty until first engine progress", runtimeLastProgressAt)
	}

	runtime, err := s.GetWorkerTaskRuntimeByJob(ctx, "job-T-accept-1")
	if err != nil {
		t.Fatalf("canonical runtime reader: %v", err)
	}
	if runtime == nil || runtime.WorkerID != "w-1" || runtime.AttemptID != "A-accept-1" || runtime.StartedAt == "" {
		t.Fatalf("canonical runtime read model = %+v; want worker/attempt/started_at immediately after RUNNING", runtime)
	}
}

func TestUpsertWorkerTaskRuntimeResetsProgressForNewAttempt(t *testing.T) {
	s, _ := openTaskAtomicTestDB(t)
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO worker_task_runtime
		(task_id,job_id,attempt_id,attempt_number,worker_id,session_id,lease_id,executor_id,
		executor_version,runtime_status,progress_percent,progress_stage,current_scene,total_scenes,
		current_segment,total_segments,frames_encoded,frames_decoded,frames_composited,ffmpeg_speed_x,
		elapsed_ms,cumulative_metrics_json,started_at,last_progress_at,cancel_requested_at,updated_at)
		VALUES ('T-runtime-reset','J-runtime-reset','A-old',1,'w-old','s-old','L-old','render',1,
		'RUNNING',88,'building_segments',7,13,12,26,18432,7200,8450,2.37,183421,'{\"frames_encoded\":18432}',
		'2026-08-10T09:00:00Z','2026-08-10T09:03:00Z','2026-08-10T09:03:30Z','2026-08-10T09:03:00Z')`); err != nil {
		t.Fatalf("seed stale runtime: %v", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin runtime reset tx: %v", err)
	}
	if err := upsertWorkerTaskRuntimeTx(ctx, tx, workerTaskRuntimeUpsert{
		TaskID: "T-runtime-reset", JobID: "J-runtime-reset", AttemptID: "A-new", AttemptNumber: 2,
		WorkerID: "w-new", SessionID: "s-new", LeaseID: "L-new", ExecutorID: "render",
		ExecutorVersion: 1, RuntimeStatus: "RUNNING", StartedAt: "2026-08-10T10:00:00Z",
		UpdatedAt: "2026-08-10T10:00:00Z",
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert new runtime: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit runtime reset tx: %v", err)
	}
	var attemptID, workerID, lastProgress string
	var phase sql.NullString
	var percent, scene, segment, frames int
	var cancelRequested sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT attempt_id,worker_id,progress_stage,COALESCE(last_progress_at,''),COALESCE(cancel_requested_at,''),progress_percent,current_scene,current_segment,frames_encoded FROM worker_task_runtime WHERE task_id='T-runtime-reset'`).Scan(&attemptID, &workerID, &phase, &lastProgress, &cancelRequested, &percent, &scene, &segment, &frames); err != nil {
		t.Fatalf("read reset runtime: %v", err)
	}
	if attemptID != "A-new" || workerID != "w-new" || phase.Valid || lastProgress != "" || cancelRequested.String != "" || percent != 0 || scene != 0 || segment != 0 || frames != 0 {
		t.Fatalf("stale progress leaked into new attempt: attempt=%q worker=%q phase=%v last=%q cancel=%v percent=%d scene=%d segment=%d frames=%d", attemptID, workerID, phase, lastProgress, cancelRequested, percent, scene, segment, frames)
	}
}

// TestAcceptTaskAtomic_StaleRevision: wrong revision ⇒ ErrTransitionConflict
// AND no attempt row updated (rolled back).
func TestAcceptTaskAtomic_StaleRevision(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-2", "w-2", "L-2", "A-accept-2", 1, 0)

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-2",
		TaskID:        "T-accept-2",
		JobID:         "job-T-accept-2",
		WorkerID:      "w-2",
		LeaseID:       "L-2",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	err := r.AcceptTaskAtomic(ctx, attempt, 99) // stale revision
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on stale revision, got nil")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Verify rollback: task stayed LEASED, PENDING attempt row remains
	// unchanged (rollback preserved the pre-seeded state).
	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-accept-2").Scan(&taskStatus); err != nil {
		t.Fatalf("post-reject SELECT tasks: %v", err)
	}
	if taskStatus != "LEASED" {
		t.Errorf("tasks.status = %s; want LEASED (rollback)", taskStatus)
	}
	var n int
	var attemptStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(MIN(status), '') FROM task_attempts WHERE task_id = ? GROUP BY task_id`,
		"T-accept-2").Scan(&n, &attemptStatus); err != nil {
		// No rows at all (unexpected — seedLeasedTask pre-inserts one)
		n = 0
	}
	if n != 1 {
		t.Errorf("task_attempts rows = %d; want 1 (pre-seeded PENDING row, rollback preserved it)", n)
	}
	if attemptStatus != "PENDING" {
		t.Errorf("task_attempts status = %s; want PENDING (rollback did NOT promote it)", attemptStatus)
	}

	var jobStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = ?`,
		"job-T-accept-2").Scan(&jobStatus); err != nil {
		t.Fatalf("post-reject SELECT jobs: %v", err)
	}
	if jobStatus != "PENDING" {
		t.Errorf("jobs.status = %s; want PENDING (rollback)", jobStatus)
	}
}

func TestAcceptTaskAtomic_PromotesRetryWaitJob(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-retry", "w-r", "L-r", "A-accept-r", 1, 0)

	if _, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'RETRY_WAIT', revision = 4 WHERE job_id = ?`,
		"job-T-accept-retry"); err != nil {
		t.Fatalf("seed RETRY_WAIT job: %v", err)
	}

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-r",
		TaskID:        "T-accept-retry",
		JobID:         "job-T-accept-retry",
		WorkerID:      "w-r",
		LeaseID:       "L-r",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("AcceptTaskAtomic retry_wait job: %v", err)
	}

	var jobStatus string
	var jobRevision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, revision FROM jobs WHERE job_id = ?`,
		"job-T-accept-retry").Scan(&jobStatus, &jobRevision); err != nil {
		t.Fatalf("post-accept retry_wait SELECT jobs: %v", err)
	}
	if jobStatus != "RUNNING" {
		t.Errorf("jobs.status = %s; want RUNNING", jobStatus)
	}
	if jobRevision != 5 {
		t.Errorf("jobs.revision = %d; want 5", jobRevision)
	}
}

func TestAcceptTaskAtomic_RejectsTerminalJobState(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-terminal", "w-t", "L-t", "A-accept-t", 1, 0)

	if _, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'FAILED', revision = 2 WHERE job_id = ?`,
		"job-T-accept-terminal"); err != nil {
		t.Fatalf("seed FAILED job: %v", err)
	}

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-t",
		TaskID:        "T-accept-terminal",
		JobID:         "job-T-accept-terminal",
		WorkerID:      "w-t",
		LeaseID:       "L-t",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	err := r.AcceptTaskAtomic(ctx, attempt, 0)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on terminal job state, got nil")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-accept-terminal").Scan(&taskStatus); err != nil {
		t.Fatalf("post-terminal-conflict SELECT tasks: %v", err)
	}
	if taskStatus != "LEASED" {
		t.Errorf("tasks.status = %s; want LEASED (rollback)", taskStatus)
	}

	var attemptStatusAfter string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id = ?`,
		"A-accept-t").Scan(&attemptStatusAfter); err != nil {
		t.Fatalf("post-terminal-conflict SELECT task_attempts: %v", err)
	}
	if attemptStatusAfter != "PENDING" {
		t.Errorf("task_attempts.status = %s; want PENDING (rollback)", attemptStatusAfter)
	}
	var runtimeCount int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM worker_task_runtime WHERE task_id = ?`,
		"T-accept-terminal").Scan(&runtimeCount); err != nil {
		t.Fatalf("post-terminal-conflict SELECT worker_task_runtime: %v", err)
	}
	if runtimeCount != 0 {
		t.Fatalf("worker_task_runtime rows = %d; want 0 after rollback", runtimeCount)
	}
}
