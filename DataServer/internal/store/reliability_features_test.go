package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/deadletters"
	"velox-server/internal/renderfingerprint"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

func TestReliability_FeaturesPersistAndReplay(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/reliability.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	fp, err := renderfingerprint.Build(renderfingerprint.Input{
		RenderPlan:       map[string]any{"version": 2, "scene": "intro"},
		CanonicalPayload: map[string]any{"title": "demo"},
		InputManifest:    map[string]any{"assets": []string{"clip-a"}},
		AssetHashes:      []string{"sha256:clip-a"}, FontHashes: []string{"sha256:font-a"},
		EngineVersion: "engine-2", FFmpegVersion: "7", WorkerVersion: "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveRenderFingerprint(ctx, tx, "attempt-1", "task-1", "job-1", &fp, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := s.db.QueryRow(`SELECT render_fingerprint FROM task_attempt_render_fingerprints WHERE attempt_id='attempt-1'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != fp.Value {
		t.Fatalf("stored fingerprint=%q want %q", stored, fp.Value)
	}

	if _, err := s.db.Exec(`INSERT INTO artifacts (id,job_id,type,storage_provider,status,created_at) VALUES ('artifact-1','job-1','video','local','FAILED',?)`, time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueArtifactGCCandidate(ctx, "artifact-1", "render_failed", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	candidates, err := s.LeaseArtifactGCCandidates(ctx, "gc-test", time.Now(), time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ArtifactID != "artifact-1" {
		t.Fatalf("unexpected GC lease: %#v", candidates)
	}
	if err := s.CompleteArtifactGC(ctx, "artifact-1", "gc-test", true, ""); err != nil {
		t.Fatal(err)
	}
	var artifactStatus string
	if err := s.db.QueryRow(`SELECT status FROM artifacts WHERE id='artifact-1'`).Scan(&artifactStatus); err != nil {
		t.Fatal(err)
	}
	if artifactStatus != ArtifactGCDeleted {
		t.Fatalf("artifact status=%q want DELETED", artifactStatus)
	}

	if _, err := s.db.Exec(`INSERT INTO tasks (task_id,job_id,status,created_at,updated_at) VALUES ('task-2','job-2','FAILED',?,?)`, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO dead_letter_tasks (id,job_id,task_id,last_attempt_id,failure_class,error_code,payload_snapshot_json,first_failed_at,last_failed_at) VALUES ('dlq-1','job-2','task-2','attempt-2','render','FFMPEG_FAIL','{"x":1}',?,?)`, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplayDeadLetter(ctx, "dlq-1"); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM tasks WHERE task_id='task-2'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(taskgraph.StatusReady) {
		t.Fatalf("replayed task status=%q want READY", status)
	}
	dlq, err := s.GetDeadLetter(ctx, "dlq-1")
	if err != nil {
		t.Fatal(err)
	}
	if dlq == nil || dlq.Status != deadletters.StatusReplayPending || dlq.ReplayCount != 1 {
		t.Fatalf("unexpected DLQ after replay: %#v", dlq)
	}
}

func TestPersistDeadLetterIsIdempotentPerAttempt(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/dlq.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	cmd := taskgraph.IngestResultCommand{TaskID: "task-1", JobID: "job-1", AttemptID: "attempt-1", TaskStatus: taskgraph.StatusFailed, AttemptStatus: taskattempts.AttemptStatusFailed, ErrorCode: "ENCODE_FAILED", RawReportJSON: `{"failure":true}`}
	for i := 0; i < 2; i++ {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := persistDeadLetter(ctx, tx, cmd, now); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM dead_letter_tasks WHERE last_attempt_id='attempt-1'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("DLQ rows=%d want 1", count)
	}
}
