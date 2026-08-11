package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
)

// openRenderPlanTestDB builds a task_attempts schema that includes the
// migration-145 plan columns (plan_version, plan_sha256, render_plan_json).
func openRenderPlanTestDB(t *testing.T) *SQLiteTaskAttemptRepository {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE tasks (
	task_id          TEXT PRIMARY KEY,
	job_id           TEXT NOT NULL,
	executor_id      TEXT NOT NULL DEFAULT '',
	executor_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE task_attempts (
	id              TEXT PRIMARY KEY,
	task_id         TEXT NOT NULL,
	job_id          TEXT NOT NULL,
	attempt_number  INTEGER NOT NULL,
	worker_id       TEXT NOT NULL,
	worker_session_id  TEXT NOT NULL DEFAULT '',
	worker_snapshot_id TEXT NOT NULL DEFAULT '',
	lease_id        TEXT NOT NULL,
	status          TEXT NOT NULL,
	started_at      TEXT,
	completed_at    TEXT,
	error_code      TEXT NOT NULL DEFAULT '',
	error_message   TEXT NOT NULL DEFAULT '',
	report_version  INTEGER NOT NULL DEFAULT 0,
	created_at      TEXT NOT NULL,
	updated_at      TEXT NOT NULL,
	git_sha              TEXT NOT NULL DEFAULT '',
	worker_version       TEXT NOT NULL DEFAULT '',
	engine_version       TEXT NOT NULL DEFAULT '',
	ffmpeg_version       TEXT NOT NULL DEFAULT '',
	config_hash          TEXT NOT NULL DEFAULT '',
	docker_image_digest  TEXT NOT NULL DEFAULT '',
	trace_id             TEXT NOT NULL DEFAULT '',
	span_id              TEXT NOT NULL DEFAULT '',
	plan_version         INTEGER NOT NULL DEFAULT 0,
	plan_sha256          TEXT NOT NULL DEFAULT '',
	render_plan_json     TEXT NOT NULL DEFAULT '',
	renderer_version     TEXT NOT NULL DEFAULT '', -- migration 148
	artifact_sha256      TEXT NOT NULL DEFAULT '', -- migration 148
	worker_sha256        TEXT NOT NULL DEFAULT '', -- migration 149
	artifact_sha256_mismatch INTEGER NOT NULL DEFAULT 0 -- migration 149
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return NewSQLiteTaskAttemptRepository(&SQLiteStore{db: db})
}

func TestUpsertRenderPlan_PersistsAndReadsBack(t *testing.T) {
	repo := openRenderPlanTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	if _, err := repo.store.db.ExecContext(ctx, `
		INSERT INTO tasks (task_id, job_id, executor_id, executor_version)
		VALUES ('task-plan', 'job-plan', 'executor.plan', 1)`); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := repo.store.db.ExecContext(ctx, `
		INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id, status,
			created_at, updated_at
		) VALUES ('attempt-plan', 'task-plan', 'job-plan', 1,
			'worker-plan', 'lease-plan', 'PENDING', ?, ?)`, now, now); err != nil {
		t.Fatalf("insert attempt: %v", err)
	}

	const planJSON = `{"plan_version":1,"job_id":"job-plan","attempt_id":"attempt-plan","duration_ms":1000,"media_contract":{"video_codec":"h264","width":1920,"height":1080,"fps_num":30,"fps_den":1},"segments":[{"segment_id":"seg_000","asset_id":"asset-a","timeline_start_ms":0}],"audio":[],"assets":[{"asset_id":"asset-a"}]}`
	const planSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	if err := repo.UpsertRenderPlan(ctx, "attempt-plan", 1, planSHA, planJSON); err != nil {
		t.Fatalf("UpsertRenderPlan: %v", err)
	}

	version, sha, json, err := repo.GetRenderPlan(ctx, "attempt-plan")
	if err != nil {
		t.Fatalf("GetRenderPlan: %v", err)
	}
	if version != 1 || sha != planSHA || json != planJSON {
		t.Fatalf("readback = %d/%q/%q", version, sha, json)
	}

	// Idempotent last-writer-wins on a second stamp.
	if err := repo.UpsertRenderPlan(ctx, "attempt-plan", 1, "f00f", `{"plan_version":1}`); err != nil {
		t.Fatalf("UpsertRenderPlan second stamp: %v", err)
	}
	version, sha, _, err = repo.GetRenderPlan(ctx, "attempt-plan")
	if err != nil {
		t.Fatalf("GetRenderPlan after restamp: %v", err)
	}
	if version != 1 || sha != "f00f" {
		t.Fatalf("restamp readback = %d/%q, want 1/f00f", version, sha)
	}
}

func TestGetRenderPlan_MissingAttemptReturnsZero(t *testing.T) {
	repo := openRenderPlanTestDB(t)
	version, sha, planJSON, err := repo.GetRenderPlan(context.Background(), "no-such-attempt")
	if err != nil {
		t.Fatalf("GetRenderPlan: %v", err)
	}
	if version != 0 || sha != "" || planJSON != "" {
		t.Fatalf("missing attempt readback = %d/%q/%q, want zero", version, sha, planJSON)
	}
}

func TestUpsertRenderPlan_MissingAttemptFailsClosed(t *testing.T) {
	repo := openRenderPlanTestDB(t)
	err := repo.UpsertRenderPlan(context.Background(), "no-such-attempt", 1, "sha", "json")
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("UpsertRenderPlan missing attempt error = %v, want ErrTransitionConflict", err)
	}
}

func TestUpsertRenderPlan_Validation(t *testing.T) {
	repo := openRenderPlanTestDB(t)
	ctx := context.Background()
	if err := repo.UpsertRenderPlan(ctx, "", 1, "sha", "json"); err == nil {
		t.Fatal("empty attempt_id must fail")
	}
	if err := repo.UpsertRenderPlan(ctx, "attempt-x", 0, "sha", "json"); err == nil {
		t.Fatal("zero plan_version must fail")
	}
	if err := repo.UpsertRenderPlan(ctx, "attempt-x", 1, "", "json"); err == nil {
		t.Fatal("empty plan_sha256 must fail")
	}
}

var _ taskattempts.Repository = (*SQLiteTaskAttemptRepository)(nil)
