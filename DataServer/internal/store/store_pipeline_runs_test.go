package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/pipelineruns"
)

func newTestPipelineRunStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "pipeline_runs.sqlite")
	db, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestInsertPipelineRun_CreatesAndReturnsRow(t *testing.T) {
	db := newTestPipelineRunStore(t)
	ctx := context.Background()

	pr := &pipelineruns.PipelineRun{
		ID:             "run_123",
		RequestID:      "req_123",
		IdempotencyKey: "idem-123",
		Status:         pipelineruns.StatusAccepted,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	res, err := db.InsertPipelineRun(ctx, pr)
	if err != nil {
		t.Fatalf("insert pipeline run: %v", err)
	}
	if !res.Created {
		t.Fatalf("expected Created=true on first insert")
	}
	if res.Run == nil || res.Run.ID != "run_123" {
		t.Fatalf("expected returned run to have id run_123, got %+v", res.Run)
	}

	got, err := db.GetPipelineRun(ctx, "run_123")
	if err != nil {
		t.Fatalf("get pipeline run: %v", err)
	}
	if got.IdempotencyKey != "idem-123" {
		t.Fatalf("idempotency key mismatch: got %q", got.IdempotencyKey)
	}
}

func TestInsertPipelineRun_IdempotentOnDuplicateKey(t *testing.T) {
	db := newTestPipelineRunStore(t)
	ctx := context.Background()

	pr := &pipelineruns.PipelineRun{
		ID:             "run_123",
		RequestID:      "req_123",
		IdempotencyKey: "idem-duplicate",
		Status:         pipelineruns.StatusAccepted,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	first, err := db.InsertPipelineRun(ctx, pr)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !first.Created {
		t.Fatalf("expected first insert to create row")
	}

	// Second insert with same idempotency key but different id/request_id.
	pr2 := &pipelineruns.PipelineRun{
		ID:             "run_456",
		RequestID:      "req_456",
		IdempotencyKey: "idem-duplicate",
		Status:         pipelineruns.StatusAccepted,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	second, err := db.InsertPipelineRun(ctx, pr2)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if second.Created {
		t.Fatalf("expected second insert to be a duplicate")
	}
	if second.Run.ID != first.Run.ID {
		t.Fatalf("expected duplicate to return same id: got %q want %q", second.Run.ID, first.Run.ID)
	}

	// Ensure only one row exists.
	var count int
	if err := db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM pipeline_runs WHERE idempotency_key = ?", "idem-duplicate").Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one row, got %d", count)
	}
}

func TestGetPipelineRunByRequestID(t *testing.T) {
	db := newTestPipelineRunStore(t)
	ctx := context.Background()

	pr := &pipelineruns.PipelineRun{
		ID:             "run_789",
		RequestID:      "req_789",
		IdempotencyKey: "idem-789",
		Status:         pipelineruns.StatusAccepted,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if _, err := db.InsertPipelineRun(ctx, pr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := db.GetPipelineRunByRequestID(ctx, "req_789")
	if err != nil {
		t.Fatalf("get by request id: %v", err)
	}
	if got.ID != "run_789" {
		t.Fatalf("expected run_789, got %q", got.ID)
	}
}

func TestUpdatePipelineRunStatus_SetsCompletedAtForTerminal(t *testing.T) {
	db := newTestPipelineRunStore(t)
	ctx := context.Background()

	pr := &pipelineruns.PipelineRun{
		ID:             "run_status",
		RequestID:      "req_status",
		IdempotencyKey: "idem-status",
		Status:         pipelineruns.StatusAccepted,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if _, err := db.InsertPipelineRun(ctx, pr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.UpdatePipelineRunStatus(ctx, "run_status", pipelineruns.StatusFailed, "remote"); err != nil {
		t.Fatalf("update status: %v", err)
	}

	got, err := db.GetPipelineRun(ctx, "run_status")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.Status != pipelineruns.StatusFailed {
		t.Fatalf("expected FAILED, got %q", got.Status)
	}
	if got.CompletedAt.IsZero() {
		t.Fatalf("expected completed_at to be set for terminal status")
	}
}

func TestUpdatePipelineRunError_MarksFailed(t *testing.T) {
	db := newTestPipelineRunStore(t)
	ctx := context.Background()

	pr := &pipelineruns.PipelineRun{
		ID:             "run_err",
		RequestID:      "req_err",
		IdempotencyKey: "idem-err",
		Status:         pipelineruns.StatusAccepted,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if _, err := db.InsertPipelineRun(ctx, pr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.UpdatePipelineRunError(ctx, "run_err", "REMOTE_CALL_FAILED", "remote engine timeout", "REMOTE_SUBMITTING"); err != nil {
		t.Fatalf("update error: %v", err)
	}

	got, err := db.GetPipelineRun(ctx, "run_err")
	if err != nil {
		t.Fatalf("get after error: %v", err)
	}
	if got.Status != pipelineruns.StatusFailed {
		t.Fatalf("expected FAILED, got %q", got.Status)
	}
	if got.ErrorCode != "REMOTE_CALL_FAILED" {
		t.Fatalf("expected error code REMOTE_CALL_FAILED, got %q", got.ErrorCode)
	}
	if got.FailedStage != "REMOTE_SUBMITTING" {
		t.Fatalf("expected failed stage REMOTE_SUBMITTING, got %q", got.FailedStage)
	}
}

func TestClearPipelineRunError_ResetsErrorFields(t *testing.T) {
	db := newTestPipelineRunStore(t)
	ctx := context.Background()

	pr := &pipelineruns.PipelineRun{
		ID:             "run_clear",
		RequestID:      "req_clear",
		IdempotencyKey: "idem-clear",
		Status:         pipelineruns.StatusFailed,
		ErrorCode:      "OLD_ERROR",
		ErrorMessage:   "old message",
		FailedStage:    "REMOTE",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if _, err := db.InsertPipelineRun(ctx, pr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.ClearPipelineRunError(ctx, "run_clear"); err != nil {
		t.Fatalf("clear error: %v", err)
	}

	got, err := db.GetPipelineRun(ctx, "run_clear")
	if err != nil {
		t.Fatalf("get after clear: %v", err)
	}
	if got.ErrorCode != "" || got.ErrorMessage != "" || got.FailedStage != "" {
		t.Fatalf("expected error fields cleared, got %+v", got)
	}
}
