package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/performance"
)

func benchmarkRunFixture() *performance.BenchmarkRun {
	return &performance.BenchmarkRun{
		RunID:             "gervais-final-v1:cold_cache:attempt-1",
		BenchmarkCaseID:   performance.BenchmarkCaseGervaisFinalV1,
		JobID:             "job-1",
		TaskID:            "task-1",
		AttemptID:         "attempt-1",
		WorkerID:          "worker-1",
		WorkerSnapshotID:  "snapshot-1",
		CacheMode:         performance.BenchmarkCacheCold,
		GitSHA:            "git-1",
		EngineVersion:     "engine-1",
		FFmpegVersion:     "ffmpeg-1",
		ConfigHash:        "config-1",
		DockerImageDigest: "sha256:image-1",
		Status:            "SUCCEEDED",
		RenderFactor:      0.5,
		WallMS:            3000,
		OutputDurationMS:  6000,
		OutputSHA256:      "output-hash-1",
	}
}

func TestSQLitePerformanceRepository_BenchmarkRunReplayAndConflict(t *testing.T) {
	st := openTestDB(t)
	defer st.Close()
	repo := NewSQLitePerformanceRepository(st)
	ctx := context.Background()

	run := benchmarkRunFixture()
	if err := repo.RecordBenchmarkRun(ctx, run); err != nil {
		t.Fatalf("first record: %v", err)
	}
	first, err := repo.GetBenchmarkRun(ctx, run.RunID)
	if err != nil {
		t.Fatalf("get first record: %v", err)
	}
	if first == nil || first.PayloadHash == "" {
		t.Fatalf("first record missing payload hash: %+v", first)
	}

	// The same payload is accepted and does not create a second row.
	replay := *run
	replay.CreatedAt = first.CreatedAt.Add(24 * time.Hour)
	if err := repo.RecordBenchmarkRun(ctx, &replay); err != nil {
		t.Fatalf("identical replay: %v", err)
	}
	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM performance_benchmark_runs WHERE run_id = ?`, run.RunID).Scan(&count); err != nil {
		t.Fatalf("count replay rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("replay row count=%d, want 1", count)
	}

	// A changed metric with the same identity is a hard conflict and
	// leaves the original evidence untouched.
	conflict := *run
	conflict.WallMS = 9000
	err = repo.RecordBenchmarkRun(ctx, &conflict)
	if !performance.IsBenchmarkRunConflict(err) {
		t.Fatalf("divergent replay error=%v, want benchmark conflict", err)
	}
	got, err := repo.GetBenchmarkRun(ctx, run.RunID)
	if err != nil {
		t.Fatalf("get after conflict: %v", err)
	}
	if got.WallMS != 3000 || got.PayloadHash != first.PayloadHash {
		t.Fatalf("conflict modified original row: %+v", got)
	}
}

func TestSQLitePerformanceRepository_BenchmarkRunCacheModesAndValidation(t *testing.T) {
	st := openTestDB(t)
	defer st.Close()
	repo := NewSQLitePerformanceRepository(st)
	ctx := context.Background()

	cold := benchmarkRunFixture()
	warm := benchmarkRunFixture()
	warm.RunID = "gervais-final-v1:warm_cache:attempt-1"
	warm.CacheMode = performance.BenchmarkCacheWarm
	if err := repo.RecordBenchmarkRun(ctx, cold); err != nil {
		t.Fatalf("cold record: %v", err)
	}
	if err := repo.RecordBenchmarkRun(ctx, warm); err != nil {
		t.Fatalf("warm record: %v", err)
	}
	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM performance_benchmark_runs WHERE benchmark_case_id = ?`, performance.BenchmarkCaseGervaisFinalV1).Scan(&count); err != nil {
		t.Fatalf("count benchmark rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("benchmark rows=%d, want 2", count)
	}

	invalid := benchmarkRunFixture()
	invalid.RunID = "invalid"
	invalid.CacheMode = "warm"
	if err := repo.RecordBenchmarkRun(ctx, invalid); err == nil {
		t.Fatal("invalid cache mode should fail validation")
	}
}
