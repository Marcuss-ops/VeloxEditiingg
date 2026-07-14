package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/performance"
)

func TestSQLitePerformanceRepository_UpsertAndGet(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	repo := NewSQLitePerformanceRepository(store)
	baseline := &performance.Baseline{
		WorkloadKey:     "pipeline=composite|res=1080p|fps=30|duration=120s",
		GitSHA:          "abc1234",
		ConfigHash:      "sha256:def5678",
		WorkerClass:     "worker-rome-02",
		SampleCount:     42,
		P50WallMs:       1234.5,
		P95WallMs:       2345.6,
		P50RenderFactor: 0.84,
		P95RenderFactor: 1.12,
		ErrorRate:       0.01,
		CreatedAt:       time.Now().UTC(),
	}

	if err := repo.Upsert(ctx, baseline); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if baseline.BaselineID == "" {
		t.Fatal("BaselineID should be assigned")
	}

	got, err := repo.Get(ctx, baseline.WorkloadKey, baseline.GitSHA, baseline.ConfigHash, baseline.WorkerClass)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if got.SampleCount != 42 {
		t.Errorf("SampleCount = %d; want 42", got.SampleCount)
	}
	if got.P50WallMs != 1234.5 {
		t.Errorf("P50WallMs = %f; want 1234.5", got.P50WallMs)
	}
	if got.P95RenderFactor != 1.12 {
		t.Errorf("P95RenderFactor = %f; want 1.12", got.P95RenderFactor)
	}

	// Upsert with same tuple should replace the row while preserving baseline_id.
	originalID := baseline.BaselineID
	baseline.SampleCount = 100
	baseline.P50WallMs = 999.0
	if err := repo.Upsert(ctx, baseline); err != nil {
		t.Fatalf("Upsert replace: %v", err)
	}
	got, err = repo.Get(ctx, baseline.WorkloadKey, baseline.GitSHA, baseline.ConfigHash, baseline.WorkerClass)
	if err != nil {
		t.Fatalf("Get after replace: %v", err)
	}
	if got.BaselineID != originalID {
		t.Errorf("BaselineID after replace = %s; want %s", got.BaselineID, originalID)
	}
	if got.SampleCount != 100 {
		t.Errorf("SampleCount after replace = %d; want 100", got.SampleCount)
	}
	if got.P50WallMs != 999.0 {
		t.Errorf("P50WallMs after replace = %f; want 999.0", got.P50WallMs)
	}
}

func TestSQLitePerformanceRepository_ListByWorkloadKey(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	repo := NewSQLitePerformanceRepository(store)
	baseTime := time.Now().UTC()
	for i := 0; i < 3; i++ {
		b := &performance.Baseline{
			WorkloadKey:     "workload-A",
			GitSHA:          "sha-" + string(rune('a'+i)),
			ConfigHash:      "cfg-1",
			WorkerClass:     "class-" + string(rune('x'+i)),
			SampleCount:     i + 1,
			P50WallMs:       float64(i),
			P95WallMs:       float64(i),
			P50RenderFactor: float64(i),
			P95RenderFactor: float64(i),
			ErrorRate:       float64(i) * 0.01,
			CreatedAt:       baseTime.Add(time.Duration(-i) * time.Hour),
		}
		if err := repo.Upsert(ctx, b); err != nil {
			t.Fatalf("Upsert %d: %v", i, err)
		}
	}

	results, err := repo.ListByWorkloadKey(ctx, "workload-A")
	if err != nil {
		t.Fatalf("ListByWorkloadKey: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d; want 3", len(results))
	}
	for i := 1; i < len(results); i++ {
		if results[i-1].CreatedAt.Before(results[i].CreatedAt) {
			t.Fatalf("results not ordered by created_at DESC at index %d", i)
		}
	}
}

func TestSQLitePerformanceRepository_ListByGitSHA(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	repo := NewSQLitePerformanceRepository(store)
	baseTime := time.Now().UTC()
	for i := 0; i < 2; i++ {
		b := &performance.Baseline{
			WorkloadKey:     "workload-" + string(rune('a'+i)),
			GitSHA:          "sha-common",
			ConfigHash:      "cfg-" + string(rune('1'+i)),
			WorkerClass:     "class-x",
			SampleCount:     1,
			P50WallMs:       100,
			P95WallMs:       200,
			P50RenderFactor: 0.5,
			P95RenderFactor: 1.0,
			ErrorRate:       0,
			CreatedAt:       baseTime.Add(time.Duration(-i) * time.Hour),
		}
		if err := repo.Upsert(ctx, b); err != nil {
			t.Fatalf("Upsert %d: %v", i, err)
		}
	}

	results, err := repo.ListByGitSHA(ctx, "sha-common")
	if err != nil {
		t.Fatalf("ListByGitSHA: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d; want 2", len(results))
	}
	for i := 1; i < len(results); i++ {
		if results[i-1].CreatedAt.Before(results[i].CreatedAt) {
			t.Fatalf("results not ordered by created_at DESC at index %d", i)
		}
	}
}

func TestSQLitePerformanceRepository_GetMissing(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	repo := NewSQLitePerformanceRepository(store)
	got, err := repo.Get(ctx, "missing", "missing", "missing", "missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Fatalf("Get returned non-nil for missing baseline: %+v", got)
	}
}
