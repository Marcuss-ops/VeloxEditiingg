package store

import (
	"context"
	"testing"
)

func TestBuildDuplicateDeliveryManifestGroupsDriveRowsByArtifactAndDestination(t *testing.T) {
	db, err := NewSQLiteStore(t.TempDir() + "/manifest.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := "2026-08-04T10:00:00Z"
	if _, err := db.DB().Exec(`
		INSERT INTO jobs (job_id,status,created_at,updated_at,migrated_at) VALUES ('job-manifest','SUCCEEDED',?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`
		INSERT INTO delivery_destinations (destination_id,provider,name,enabled,created_at,updated_at)
		VALUES ('drive-target','drive','Drive',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// Historical databases may contain duplicates from before the
	// artifact/destination uniqueness migration. Reproduce that legacy
	// state only in this fixture so the dry-run manifest can identify it;
	// production finalization retains the unique index and uses
	// ON CONFLICT DO NOTHING.
	if _, err := db.DB().Exec(`DROP INDEX IF EXISTS idx_job_delivery_artifact_destination`); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		artifact, delivery, remote, created, status string
	}{
		{"artifact-canonical", "delivery-canonical", "drive-file-correct", "2026-08-04T10:00:00Z", "SUCCEEDED"},
		{"artifact-canonical", "delivery-duplicate", "drive-file-duplicate", "2026-08-04T10:01:00Z", "SUCCEEDED"},
		{"artifact-other", "delivery-other", "drive-file-other", "2026-08-04T10:02:00Z", "SUCCEEDED"},
	} {
		if item.delivery == "delivery-canonical" || item.delivery == "delivery-other" {
			if _, err := db.DB().Exec(`
				INSERT INTO artifacts (id,job_id,type,storage_provider,storage_key,status,created_at)
				VALUES (?,?,'video','local',?,'READY',?)`, item.artifact, "job-manifest", item.artifact, item.created); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.DB().Exec(`
			INSERT INTO job_deliveries (delivery_id,artifact_id,destination_id,status,idempotency_key,remote_id,created_at,updated_at)
			VALUES (?,?,?,?,?,?,?,?)`, item.delivery, item.artifact, "drive-target", item.status, item.delivery, item.remote, item.created, item.created); err != nil {
			t.Fatal(err)
		}
	}

	got, err := db.BuildDuplicateDeliveryManifest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("manifest records=%d, want 1: %#v", len(got), got)
	}
	record := got[0]
	if record.CanonicalDeliveryID != "delivery-canonical" || record.DeliveryID != "delivery-duplicate" {
		t.Fatalf("wrong canonical/duplicate rows: %#v", record)
	}
	if record.DriveFileIDCorrect != "drive-file-correct" || record.DriveFileIDDuplicate != "drive-file-duplicate" {
		t.Fatalf("wrong remote IDs: %#v", record)
	}
	if record.JobID != "job-manifest" || record.ArtifactID != "artifact-canonical" || record.DestinationID != "drive-target" {
		t.Fatalf("wrong identity fields: %#v", record)
	}
}
