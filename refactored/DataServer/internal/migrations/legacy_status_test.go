package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openLegacyDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("pragma foreign_keys: %v", err)
	}
	return db
}

// TestCountLegacyStatus_EmptyDB: brand-new DB with no jobs table returns
// all zeros and EraseSafe=true (since nothing is blocking).
func TestCountLegacyStatus_EmptyDB(t *testing.T) {
	db := openLegacyDB(t)
	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}
	if !got.EraseSafe() {
		t.Errorf("empty DB should be EraseSafe, got reasons: %v", got.BlockingReasons())
	}
	if got.JobsProcessing != 0 || got.JobsCompleted != 0 {
		t.Errorf("jobs.status counts should be 0 on empty DB, got %+v", got)
	}
	if got.JobsHasLegacyColumns {
		t.Errorf("empty DB has no jobs table, JobsHasLegacyColumns should be false")
	}
}

// TestCountLegacyStatus_LegacyStateRows seeds the schema with a few
// jobs in legacy states and verifies each is counted.
func TestCountLegacyStatus_LegacyStateRows(t *testing.T) {
	db := openLegacyDB(t)
	createJobsTable(t, db, []string{"status"})

	if _, err := db.Exec(`INSERT INTO jobs (job_id, status) VALUES
		('j1', 'PROCESSING'),
		('j2', 'PROCESSING'),
		('j3', 'COMPLETED'),
		('j4', 'AWAITING_ARTIFACT'),
		('j5', 'RENDER_FINISHED'),
		('j6', 'RUNNING')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}

	if got.JobsProcessing != 2 {
		t.Errorf("JobsProcessing: got %d, want 2", got.JobsProcessing)
	}
	if got.JobsCompleted != 1 {
		t.Errorf("JobsCompleted: got %d, want 1", got.JobsCompleted)
	}
	if got.JobsAwaitingArt != 1 {
		t.Errorf("JobsAwaitingArt: got %d, want 1", got.JobsAwaitingArt)
	}
	if got.JobsRenderFin != 1 {
		t.Errorf("JobsRenderFin: got %d, want 1", got.JobsRenderFin)
	}
	if got.EraseSafe() {
		t.Errorf("EraseSafe should be false with legacy state rows; reasons: %v", got.BlockingReasons())
	}
}

// TestCountLegacyStatus_OnlyCanonical: jobs table populated with all
// canonical-status rows should be EraseSafe (after columns are dropped).
// Since legacy columns remain in this fixture, we expect JobsHasLegacyColumns=true.
func TestCountLegacyStatus_OnlyCanonicalRows(t *testing.T) {
	db := openLegacyDB(t)
	createJobsTable(t, db, []string{"status"})

	if _, err := db.Exec(`INSERT INTO jobs (job_id, status) VALUES
		('k1', 'PENDING'),
		('k2', 'LEASED'),
		('k3', 'RUNNING'),
		('k4', 'SUCCEEDED'),
		('k5', 'FAILED'),
		('k6', 'RETRY_WAIT'),
		('k7', 'CANCELLED')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}

	if got.JobsProcessing != 0 || got.JobsCompleted != 0 ||
		got.JobsAwaitingArt != 0 || got.JobsRenderFin != 0 {
		t.Errorf("canonical-only rows should have 0 legacy-state counts, got %+v", got)
	}
}

// TestCountLegacyStatus_EmbeddedFlatFields covers legacy columns whose
// values overlap with artifacts / job_deliveries content.
func TestCountLegacyStatus_EmbeddedFlatFields(t *testing.T) {
	db := openLegacyDB(t)
	createJobsTable(t, db, []string{
		"status", "master_video_path", "drive_url", "youtube_url",
		"artifact_id", "output_sha256", "upload_idempotency_key",
		"video_uploaded", "raw_json",
	})

	if _, err := db.Exec(`INSERT INTO jobs
		(job_id, status, master_video_path, drive_url, youtube_url,
		 artifact_id, output_sha256, upload_idempotency_key, video_uploaded)
		VALUES
		('p1', 'PENDING', '/tmp/m.mp4', 'https://drive/U1', 'https://youtu.be/v1',
		 'art1', 'sha1', 'idem1', 1),
		('p2', 'PENDING', '',                '',                  '',
		 '', '', '',  0),
		('p3', 'PENDING', '/tmp/m2.mp4', '', '',
		 'art2', '', '', 1)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := db.Exec(`UPDATE jobs SET raw_json = '{"stale":true}' WHERE job_id = 'p1'`); err != nil {
		t.Fatalf("update raw_json: %v", err)
	}

	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}

	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"JobsWithMasterPathEmbed", got.JobsWithMasterPathEmbed, 2},
		{"JobsWithDriveURLEmbed", got.JobsWithDriveURLEmbed, 1},
		{"JobsWithYouTubeEmbed", got.JobsWithYouTubeEmbed, 1},
		{"JobsWithArtifactIDEmbed", got.JobsWithArtifactIDEmbed, 2},
		{"JobsWithOutputSHA256Embed", got.JobsWithOutputSHA256Embed, 1},
		{"JobsWithIdempotencyEmbed", got.JobsWithIdempotencyEmbed, 1},
		{"JobsWithVideoUploadedCol", got.JobsWithVideoUploadedCol, 2},
		{"JobsWithRawJSONNonEmpty", got.JobsWithRawJSONNonEmpty, 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, c.got, c.want)
		}
	}
	if got.EraseSafe() {
		t.Errorf("EraseSafe should be false with embedded values; reasons: %v", got.BlockingReasons())
	}
	if !got.JobsHasLegacyColumns {
		t.Errorf("JobsHasLegacyColumns should be true in this schema")
	}
}

// TestCountLegacyStatus_DroppedColumns: legacy columns gone but a stale
// row in PROCESSING state still keeps EraseSafe == false.
// Mirrors post-rebuild-migration state on an installation that suffered
// a partial purge.
func TestCountLegacyStatus_DroppedColumns_ButStaleRows(t *testing.T) {
	db := openLegacyDB(t)
	// Create jobs table WITHOUT legacy columns to simulate 028+ rebuild
	createJobsTable(t, db, []string{"status"})

	if _, err := db.Exec(`INSERT INTO jobs (job_id, status) VALUES ('q1', 'PROCESSING')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}
	if got.JobsHasLegacyColumns {
		t.Errorf("JobsHasLegacyColumns should be false without legacy cols, got %v", got.JobsLegacyColumnsPresent)
	}
	if got.JobsProcessing != 1 {
		t.Errorf("JobsProcessing: got %d, want 1", got.JobsProcessing)
	}
	if got.EraseSafe() {
		t.Errorf("EraseSafe should be false while PROCESSING rows exist")
	}
}

// TestCountLegacyStatus_FullyEraseSafe: an emptied schema (no legacy
// columns, no legacy-state rows, no embedded flat values) reports
// EraseSafe=true.
func TestCountLegacyStatus_FullyEraseSafe(t *testing.T) {
	db := openLegacyDB(t)
	createJobsTable(t, db, []string{"status"})

	if _, err := db.Exec(`INSERT INTO jobs (job_id, status) VALUES ('s1', 'SUCCEEDED')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}
	if !got.EraseSafe() {
		t.Errorf("expected EraseSafe=true, blocking reasons: %v", got.BlockingReasons())
	}
	if len(got.BlockingReasons()) != 0 {
		t.Errorf("BlockingReasons should be empty when EraseSafe, got %v", got.BlockingReasons())
	}
}

// TestCountLegacyStatus_NilDB returns a clean error rather than panicking.
func TestCountLegacyStatus_NilDB(t *testing.T) {
	_, err := CountLegacyStatus(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil db, got nil")
	}
}

// TestCountLegacyStatus_PartialSchema: tables from newer migrations
// not yet applied should not cause the function to blow up.
func TestCountLegacyStatus_PartialSchema(t *testing.T) {
	db := openLegacyDB(t)
	createJobsTable(t, db, []string{"status"})

	// workflow_runs and job_deliveries intentionally absent.
	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus should tolerate missing tables: %v", err)
	}
	if got.WorkflowRunsCount != 0 {
		t.Errorf("WorkflowRunsCount: got %d, want 0", got.WorkflowRunsCount)
	}
	if got.JobDeliveriesLegacyStatus != 0 {
		t.Errorf("JobDeliveriesLegacyStatus: got %d, want 0", got.JobDeliveriesLegacyStatus)
	}
}

// TestCountLegacyStatus_JobDeliveriesLegacyStatus counts only legacy
// strings on job_deliveries; canonical statuses must be ignored.
func TestCountLegacyStatus_JobDeliveriesLegacyStatus(t *testing.T) {
	db := openLegacyDB(t)
	createTasksTable(t, db, "job_deliveries", []string{"status"})

	if _, err := db.Exec(`INSERT INTO job_deliveries (delivery_id, status) VALUES
		('d1', 'PROCESSING'),
		('d2', 'COMPLETED'),
		('d3', 'PENDING'),
		('d4', 'RUNNING'),
		('d5', 'SUCCEEDED')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := CountLegacyStatus(context.Background(), db)
	if err != nil {
		t.Fatalf("CountLegacyStatus: %v", err)
	}
	if got.JobDeliveriesLegacyStatus != 2 {
		t.Errorf("JobDeliveriesLegacyStatus: got %d, want 2 (PROCESSING + COMPLETED)", got.JobDeliveriesLegacyStatus)
	}
}

// TestBlockingReasons_NonEmpty covers the human-readable diagnostic.
func TestBlockingReasons_NonEmpty(t *testing.T) {
	c := LegacyStatusCounts{
		JobsProcessing: 1,
		JobsCompleted:  2,
	}
	reasons := c.BlockingReasons()
	if len(reasons) != 2 {
		t.Fatalf("expected 2 reasons, got %d: %v", len(reasons), reasons)
	}
	for _, r := range reasons {
		if r == "" {
			t.Errorf("empty reason returned")
		}
	}
}

// TestBlockingReasons_FullCoverage fills every counter on a fixture
// struct and asserts the message list contains a line for each. This
// catches drift if a future field is added to LegacyStatusCounts without
// a matching line in BlockingReasons.
func TestBlockingReasons_FullCoverage(t *testing.T) {
	c := LegacyStatusCounts{
		JobsProcessing:            1,
		JobsCompleted:             1,
		JobsAwaitingArt:           1,
		JobsRenderFin:             1,
		JobsWithMasterPathEmbed:   1,
		JobsWithDriveURLEmbed:     1,
		JobsWithYouTubeEmbed:      1,
		JobsWithArtifactIDEmbed:   1,
		JobsWithOutputSHA256Embed: 1,
		JobsWithIdempotencyEmbed:  1,
		JobsWithVideoUploadedCol:  1,
		JobsWithRawJSONNonEmpty:   1,
		JobDeliveriesLegacyStatus: 1,
		JobsHasLegacyColumns:      true,
		JobsLegacyColumnsPresent:  []string{"master_video_path"},
	}
	if c.EraseSafe() {
		t.Fatal("fixture must not be EraseSafe")
	}
	reasons := c.BlockingReasons()
	if len(reasons) != 14 {
		t.Fatalf("expected 14 reasons (one per blocking counter), got %d: %v", len(reasons), reasons)
	}
}

// ============================================================
// helpers
// ============================================================

func createJobsTable(t *testing.T, db *sql.DB, extraCols []string) {
	t.Helper()
	cols := "job_id TEXT PRIMARY KEY, status TEXT"
	for _, c := range extraCols {
		cols += ", " + c + " TEXT"
	}
	if _, err := db.Exec(`CREATE TABLE jobs (` + cols + `)`); err != nil {
		t.Fatalf("create jobs: %v", err)
	}
}

func createTasksTable(t *testing.T, db *sql.DB, name string, extraCols []string) {
	t.Helper()
	cols := "delivery_id TEXT PRIMARY KEY, status TEXT"
	if name == "workflow_runs" {
		cols = "run_id TEXT PRIMARY KEY, status TEXT, raw_json TEXT"
	}
	if name == "jobs" {
		cols = "job_id TEXT PRIMARY KEY, status TEXT"
	}
	for _, c := range extraCols {
		if c == "raw_json" && name == "workflow_runs" {
			continue
		}
		cols += ", " + c + " TEXT"
	}
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (%s)`, name, cols)); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}
