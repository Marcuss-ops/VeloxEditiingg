package store

import (
	"context"
	"database/sql"
	"testing"
)

func newScorecardTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE worker_capacity_scorecards (
		worker_id           TEXT PRIMARY KEY,
		render_slots        INTEGER NOT NULL DEFAULT 0,
		prefetch_slots      INTEGER NOT NULL DEFAULT 0,
		publisher_slots     INTEGER NOT NULL DEFAULT 0,
		ram_slots           INTEGER NOT NULL DEFAULT 0,
		cpu_slots           INTEGER NOT NULL DEFAULT 0,
		disk_slots          INTEGER NOT NULL DEFAULT 0,
		network_slots       INTEGER NOT NULL DEFAULT 0,
		limiting_resource   TEXT    NOT NULL DEFAULT '',
		total_ram_bytes     INTEGER NOT NULL DEFAULT 0,
		available_ram_bytes INTEGER NOT NULL DEFAULT 0,
		effective_cpu_cores INTEGER NOT NULL DEFAULT 0,
		disk_read_mbps      REAL    NOT NULL DEFAULT 0,
		disk_write_mbps     REAL    NOT NULL DEFAULT 0,
		download_mbps       REAL    NOT NULL DEFAULT 0,
		upload_mbps         REAL    NOT NULL DEFAULT 0,
		ram_per_job_bytes   INTEGER NOT NULL DEFAULT 0,
		cpu_cores_per_job   REAL    NOT NULL DEFAULT 0,
		disk_mbps_per_job   REAL    NOT NULL DEFAULT 0,
		network_mbps_per_job REAL   NOT NULL DEFAULT 0,
		render_wall_ms_per_job   INTEGER NOT NULL DEFAULT 0,
		prefetch_wall_ms_per_job INTEGER NOT NULL DEFAULT 0,
		publish_wall_ms_per_job  INTEGER NOT NULL DEFAULT 0,
		computed_at         TEXT    NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	return &SQLiteStore{db: db}
}

func TestUpsertAndGetScorecard(t *testing.T) {
	s := newScorecardTestDB(t)
	ctx := context.Background()

	// Insert a scorecard
	row := ScorecardRow{
		WorkerID:          "w-test-1",
		RenderSlots:       3,
		PrefetchSlots:     4,
		PublisherSlots:    2,
		RAMSlots:          6,
		CPUSlots:          8,
		DiskSlots:         5,
		NetworkSlots:      2,
		LimitingResource:  "NETWORK",
		TotalRAMBytes:     16 * 1024 * 1024 * 1024,
		AvailableRAMBytes: 10 * 1024 * 1024 * 1024,
		EffectiveCPUCores: 8,
		DiskReadMbps:      1400,
		DiskWriteMbps:     1200,
		DownloadMbps:      92,
		UploadMbps:        81,
		RAMPerJobBytes:    512 * 1024 * 1024,
		CPUCoresPerJob:    1.0,
		DiskMBpsPerJob:    100,
		NetworkMbpsPerJob: 50,
		ComputedAt:        "2026-08-27T12:00:00Z",
	}
	if err := s.UpsertScorecard(ctx, row); err != nil {
		t.Fatalf("UpsertScorecard: %v", err)
	}

	// Read it back
	got, err := s.GetScorecard(ctx, "w-test-1")
	if err != nil {
		t.Fatalf("GetScorecard: %v", err)
	}
	if got == nil {
		t.Fatal("GetScorecard returned nil")
	}
	if got.RenderSlots != 3 || got.PrefetchSlots != 4 || got.PublisherSlots != 2 {
		t.Fatalf("slots = %d/%d/%d, want 3/4/2", got.RenderSlots, got.PrefetchSlots, got.PublisherSlots)
	}
	if got.LimitingResource != "NETWORK" {
		t.Fatalf("limiting_resource = %q, want NETWORK", got.LimitingResource)
	}
	if got.TotalRAMBytes != 16*1024*1024*1024 {
		t.Fatalf("total_ram_bytes = %d, want %d", got.TotalRAMBytes, 16*1024*1024*1024)
	}
}

func TestUpsertScorecardIsIdempotent(t *testing.T) {
	s := newScorecardTestDB(t)
	ctx := context.Background()

	// Insert twice — second should update
	for i := 0; i < 2; i++ {
		if err := s.UpsertScorecard(ctx, ScorecardRow{
			WorkerID:    "w-idem-1",
			RenderSlots: 2 + i,
			ComputedAt:  "2026-08-27T12:00:00Z",
		}); err != nil {
			t.Fatalf("UpsertScorecard attempt %d: %v", i, err)
		}
	}

	got, err := s.GetScorecard(ctx, "w-idem-1")
	if err != nil {
		t.Fatalf("GetScorecard: %v", err)
	}
	if got.RenderSlots != 3 {
		t.Fatalf("RenderSlots = %d, want 3 (last write wins)", got.RenderSlots)
	}
}

func TestGetScorecardNotFound(t *testing.T) {
	s := newScorecardTestDB(t)
	ctx := context.Background()

	got, err := s.GetScorecard(ctx, "w-nonexistent")
	if err != nil {
		t.Fatalf("GetScorecard: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for nonexistent worker, got %+v", got)
	}
}

func TestGetScorecardsBulk(t *testing.T) {
	s := newScorecardTestDB(t)
	ctx := context.Background()

	// Insert two scorecards
	for _, id := range []string{"w-bulk-1", "w-bulk-2"} {
		if err := s.UpsertScorecard(ctx, ScorecardRow{
			WorkerID:    id,
			RenderSlots: 3,
			ComputedAt:  "2026-08-27T12:00:00Z",
		}); err != nil {
			t.Fatalf("UpsertScorecard %s: %v", id, err)
		}
	}

	got, err := s.GetScorecardsBulk(ctx, []string{"w-bulk-1", "w-bulk-2", "w-bulk-missing"})
	if err != nil {
		t.Fatalf("GetScorecardsBulk: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d scorecards, want 2", len(got))
	}
	if got["w-bulk-1"] == nil || got["w-bulk-1"].RenderSlots != 3 {
		t.Fatalf("w-bulk-1 wrong: %+v", got["w-bulk-1"])
	}
	if got["w-bulk-missing"] != nil {
		t.Fatal("w-bulk-missing should not be in result")
	}
}
