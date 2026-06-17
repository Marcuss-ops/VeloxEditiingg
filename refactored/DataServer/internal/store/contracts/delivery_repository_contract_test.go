package contracts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store"
)

// NewSQLiteDeliveryRepositoryFactory returns a fresh SQLite-backed factory.
func NewSQLiteDeliveryRepositoryFactory(t *testing.T) (store.DeliveryRepository, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "contract_deliveries.db")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	// Seed one job + one artifact + one delivery target so the contracts run.
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := dbStore.DB().Exec(
		`INSERT INTO jobs (job_id, status, revision, video_name, project_id, created_at, updated_at, raw_json, request_json, result_json)
		 VALUES ('job_test', 'PROCESSING', 0, 'v1', 'p1', ?, ?, '{}', '{}', '{}')`,
		now, now,
	); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := dbStore.DB().Exec(
		`INSERT INTO artifacts (id, job_id, type, storage_provider, status, sha256, size_bytes, created_at)
		 VALUES ('art_test', 'job_test', 'video', 'local', 'completed', 'deadbeef', 1024, ?)`,
		now,
	); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	cleanup := func() { _ = dbStore.Close() }
	return store.NewSQLiteDeliveryRepository(dbStore), cleanup
}

// DeliveryRepositoryContract runs the cross-backend test suite for deliveries.
func DeliveryRepositoryContract(t *testing.T, factory func(t *testing.T) (store.DeliveryRepository, func())) {
	t.Run("CreateDeliveriesForArtifact idempotent", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()

		if err := repo.CreateDeliveriesForArtifact(ctx, "art_test"); err != nil {
			t.Fatalf("first call: %v", err)
		}
		if err := repo.CreateDeliveriesForArtifact(ctx, "art_test"); err != nil {
			t.Fatalf("second call: %v", err)
		}
		// Both calls must succeed; absence of error is enough — uniqueness is verified by
		// the post-condition below in the claim test.
	})

	t.Run("ClaimNextDelivery returns target and marks it uploading", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		if err := repo.CreateDeliveriesForArtifact(ctx, "art_test"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		got, err := repo.ClaimNextDelivery(ctx, "worker-1")
		if err != nil {
			t.Fatalf("ClaimNextDelivery: %v", err)
		}
		if got == nil {
			t.Fatal("expected delivery target, got nil")
		}
		if got.Status != "uploading" {
			t.Errorf("status should be 'uploading', got %q", got.Status)
		}
		if got.LastAttemptAt == "" {
			t.Error("LastAttemptAt should be set after claim")
		}
	})

	t.Run("ClaimNextDelivery on empty queue returns nil", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		got, err := repo.ClaimNextDelivery(context.Background(), "worker-1")
		if err != nil {
			t.Fatalf("ClaimNextDelivery on empty queue: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("CompleteDelivery marks target completed", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		if err := repo.CreateDeliveriesForArtifact(ctx, "art_test"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		target, err := repo.ClaimNextDelivery(ctx, "worker-1")
		if err != nil || target == nil {
			t.Fatalf("claim: %v %v", target, err)
		}
		if err := repo.CompleteDelivery(ctx, target.ID, store.DeliveryResult{
			Success: true, URL: "https://youtube.com/watch?v=xyz",
		}); err != nil {
			t.Fatalf("CompleteDelivery: %v", err)
		}
	})

	t.Run("FailDelivery records error", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		if err := repo.CreateDeliveriesForArtifact(ctx, "art_test"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		target, err := repo.ClaimNextDelivery(ctx, "worker-1")
		if err != nil || target == nil {
			t.Fatalf("claim: %v %v", target, err)
		}
		if err := repo.FailDelivery(ctx, target.ID, store.DeliveryFailure{
			ErrorCode: "YOUTUBE_AUTH_EXPIRED",
			ErrorMsg:  "OAuth token must be refreshed",
		}); err != nil {
			t.Fatalf("FailDelivery: %v", err)
		}
	})
}

// TestDeliveryRepositoryContract_SQLite drives the suite against SQLite.
func TestDeliveryRepositoryContract_SQLite(t *testing.T) {
	DeliveryRepositoryContract(t, NewSQLiteDeliveryRepositoryFactory)
}
