package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/artifactsstore"
	"velox-server/internal/renderfingerprint"
	"velox-server/internal/renderfingerprintstore"
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
	if err := renderfingerprintstore.SaveRenderFingerprint(ctx, tx, "attempt-1", "task-1", "job-1", &fp, time.Now()); err != nil {
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
	if err := artifactsstore.NewArtifactGCStore(s.DB()).EnqueueArtifactGCCandidate(ctx, "artifact-1", "render_failed", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	candidates, err := artifactsstore.NewArtifactGCStore(s.DB()).LeaseArtifactGCCandidates(ctx, "gc-test", time.Now(), time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ArtifactID != "artifact-1" {
		t.Fatalf("unexpected GC lease: %#v", candidates)
	}
	if err := artifactsstore.NewArtifactGCStore(s.DB()).CompleteArtifactGC(ctx, "artifact-1", "gc-test", true, ""); err != nil {
		t.Fatal(err)
	}
	var artifactStatus string
	if err := s.db.QueryRow(`SELECT status FROM artifacts WHERE id='artifact-1'`).Scan(&artifactStatus); err != nil {
		t.Fatal(err)
	}
	if artifactStatus != artifactsstore.ArtifactGCDeleted {
		t.Fatalf("artifact status=%q want DELETED", artifactStatus)
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
