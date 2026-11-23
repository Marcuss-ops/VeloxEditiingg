package artifactsstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestLeaseArtifactGCCandidatesIncludesOrphanedArtifact(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE artifacts (
		id TEXT PRIMARY KEY, status TEXT, storage_provider TEXT, storage_key TEXT, local_path TEXT
	);
	CREATE TABLE artifact_gc_candidates (
		artifact_id TEXT PRIMARY KEY, reason TEXT, eligible_at TEXT,
		lease_owner TEXT DEFAULT '', lease_expires_at TEXT, delete_attempts INTEGER DEFAULT 0,
		last_error TEXT DEFAULT '', status TEXT DEFAULT 'ELIGIBLE'
	);
	INSERT INTO artifact_gc_candidates (artifact_id, reason, eligible_at, status)
	VALUES ('orphaned-artifact', 'stuck_staging', '2026-01-01T00:00:00Z', 'ELIGIBLE');`)
	if err != nil {
		t.Fatal(err)
	}

	store := NewArtifactGCStore(db)
	now := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	candidates, err := store.LeaseArtifactGCCandidates(context.Background(), "test", now, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ArtifactID != "orphaned-artifact" || candidates[0].ArtifactStatus != "" {
		t.Fatalf("unexpected candidates: %+v", candidates)
	}
	if err := store.CompleteArtifactGCNoObject(context.Background(), candidates[0].ArtifactID, "test"); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM artifact_gc_candidates WHERE artifact_id='orphaned-artifact'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != ArtifactGCDeleted {
		t.Fatalf("candidate status = %q, want %q", status, ArtifactGCDeleted)
	}
}

func TestEnqueueReadyArtifactsForRetention(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
		CREATE TABLE artifacts (
			id TEXT PRIMARY KEY, job_id TEXT NOT NULL, status TEXT NOT NULL,
			storage_provider TEXT NOT NULL, created_at TEXT NOT NULL,
			storage_key TEXT NOT NULL DEFAULT '', local_path TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL);
		CREATE TABLE artifact_gc_candidates (
			artifact_id TEXT PRIMARY KEY, reason TEXT, eligible_at TEXT,
			lease_owner TEXT DEFAULT '', lease_expires_at TEXT, delete_attempts INTEGER DEFAULT 0,
			last_error TEXT DEFAULT '', status TEXT DEFAULT 'ELIGIBLE'
		);
		INSERT INTO jobs (job_id, status) VALUES
			('job-done', 'SUCCEEDED'),
			('job-failed', 'FAILED'),
			('job-running', 'RUNNING');
		INSERT INTO artifacts (id, job_id, status, storage_provider, created_at, storage_key) VALUES
			('a-done',       'job-done',    'READY',       'local', '2026-11-01T00:00:00Z', 'artifacts/sha256/aa/a.mp4'),
			('a-failed',     'job-failed',  'READY',       'local', '2026-11-01T00:00:00Z', 'artifacts/sha256/aa/b.mp4'),
			('a-running',    'job-running', 'READY',       'local', '2026-11-01T00:00:00Z', 'artifacts/sha256/aa/c.mp4'),
			('a-recent',     'job-done',    'READY',       'local', '2026-11-22T00:00:00Z', 'artifacts/sha256/aa/d.mp4'),
			('a-remote',     'job-done',    'READY',       'gcs',   '2026-11-01T00:00:00Z', 'gs://bucket/e.mp4'),
			('a-quarantine', 'job-done',    'QUARANTINED', 'local', '2026-11-01T00:00:00Z', 'artifacts/sha256/aa/f.mp4');
	`)
	if err != nil {
		t.Fatal(err)
	}

	store := NewArtifactGCStore(db)
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	cutoff := now.AddDate(0, 0, -7)
	queued, err := store.EnqueueReadyArtifactsForRetention(context.Background(), cutoff, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if queued != 2 {
		t.Fatalf("queued = %d, want 2 (only terminal-job local READY artifacts)", queued)
	}
	rows, err := db.Query(`SELECT artifact_id FROM artifact_gc_candidates ORDER BY artifact_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if len(got) != 2 || got[0] != "a-done" || got[1] != "a-failed" {
		t.Fatalf("queued ids = %v, want [a-done a-failed]", got)
	}

	// Idempotent: a queued row is never re-enqueued while it is in flight.
	queued, err = store.EnqueueReadyArtifactsForRetention(context.Background(), cutoff, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("second pass queued = %d, want 0", queued)
	}
}
