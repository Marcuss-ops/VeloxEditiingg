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
