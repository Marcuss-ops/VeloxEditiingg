package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestIngestAssetDownloadProgressUpsertsLatestAndReplacesJobRefs(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/asset-progress.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Unix(1_700_000_000, 0).UTC()
	base := AssetDownloadProgressRecord{
		WorkerID: "worker-1", TransferID: "transfer-1", AssetKey: "sha256:abc",
		AssetID: "stock-1", Role: "stock", State: "DOWNLOADING",
		BytesDownloaded: 100, BytesTotal: 1000, BytesPerSecond: 25.5,
		ETASeconds: 36, Attempt: 1, SharedWaiters: 2, JobIDs: []string{"job-a", "job-b"},
		JobRefs:  []AssetDownloadJobRef{{JobID: "job-a", TaskID: "task-a", SceneIDs: []string{"scene-a"}}, {JobID: "job-b", TaskID: "task-b", SceneIDs: []string{"scene-b"}}},
		SceneIDs: []string{"scene-1"}, UpdatedAt: now, ReceivedAt: now, CheckpointSequence: 1,
	}
	if err := s.IngestAssetDownloadProgress(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	// Replay the same physical transfer at READY, now referenced only by
	// job-b. The latest row must update and job-a must disappear atomically.
	base.State = "READY"
	base.BytesDownloaded = 1000
	base.BytesPerSecond = 0
	base.ETASeconds = 0
	base.SharedWaiters = 0
	base.JobIDs = []string{"job-b", "job-b"}
	base.JobRefs = []AssetDownloadJobRef{{JobID: "job-b", TaskID: "task-b", SceneIDs: []string{"scene-b"}}}
	base.CheckpointSequence = 2
	base.CompletedAt = now.Add(time.Second)
	base.ReceivedAt = now.Add(time.Second)
	if err := s.IngestAssetDownloadProgress(context.Background(), base); err != nil {
		t.Fatal(err)
	}

	var state string
	var downloaded, refs int
	if err := s.db.QueryRow(`SELECT state, bytes_downloaded FROM worker_asset_downloads WHERE worker_id=? AND asset_key=?`, base.WorkerID, base.AssetKey).Scan(&state, &downloaded); err != nil {
		t.Fatal(err)
	}
	if state != "READY" || downloaded != 1000 {
		t.Fatalf("latest state=%q bytes=%d, want READY/1000", state, downloaded)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM job_asset_refs WHERE worker_id=? AND asset_key=?`, base.WorkerID, base.AssetKey).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs != 1 {
		t.Fatalf("job refs=%d, want 1 after replacement/deduplication", refs)
	}
	var jobID string
	if err := s.db.QueryRow(`SELECT job_id FROM job_asset_refs WHERE worker_id=? AND asset_key=?`, base.WorkerID, base.AssetKey).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if jobID != "job-b" {
		t.Fatalf("remaining job ref=%q, want job-b", jobID)
	}

	var cacheHit int
	if err := s.db.QueryRow(`SELECT cache_hit FROM worker_asset_downloads WHERE worker_id=? AND asset_key=?`, base.WorkerID, base.AssetKey).Scan(&cacheHit); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if cacheHit != 0 {
		t.Fatalf("cache_hit=%d, want 0", cacheHit)
	}
}
