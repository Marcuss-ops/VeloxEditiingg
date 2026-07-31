package metrics

import (
	"context"
	"math"
	"testing"

	"velox-server/internal/store"
)

func TestBuildRenderCohortKeys_EquivalentWorkloads(t *testing.T) {
	base := RenderCohortInput{
		ExecutorID: "scene.composite", ExecutorVersion: 1, WorkerClass: "GPU",
		ResolutionWidth: 1920, ResolutionHeight: 1080, FPS: 30,
		OutputDuration: 42, SceneCount: 3, SegmentCount: 8,
		AudioTracks: 2, SubtitleCount: 1, Codec: "H264", Preset: "Fast",
		CacheMode: "warm", TemplateID: "Gervais", ConfigHash: "CFG-1",
	}
	v1, baseKey1 := BuildRenderCohortKeys(base)
	base.ExecutorVersion = 2
	v2, baseKey2 := BuildRenderCohortKeys(base)
	if baseKey1 != baseKey2 {
		t.Fatalf("equivalent workloads must share base cohort: %q != %q", baseKey1, baseKey2)
	}
	if v1 == v2 {
		t.Fatalf("executor versions must produce distinct cohort keys: %q", v1)
	}
	if got, want := v1, baseKey1+"|executor_version=1"; got != want {
		t.Fatalf("v1 cohort=%q, want %q", got, want)
	}
}

func TestRenderPerformanceDaily_VersionRegressionAndRecoverableTime(t *testing.T) {
	s, err := store.NewSQLiteStore(t.TempDir() + "/render-performance.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	resolver := NewSQLiteLabelResolver(s.DB())
	ctx := context.Background()

	baseDay := "2026-07-30"
	currentDay := "2026-07-31"
	seed := func(id, day, status string, executorVersion int, phaseMS float64) {
		t.Helper()
		completed := day + "T12:00:00Z"
		if _, err := s.DB().Exec(`INSERT INTO tasks
			(task_id, job_id, executor_id, executor_version, created_at, updated_at)
			VALUES (?, ?, 'scene.composite', ?, ?, ?)`, id+"-task", id+"-job", executorVersion, completed, completed); err != nil {
			t.Fatalf("seed task %s: %v", id, err)
		}
		if _, err := s.DB().Exec(`INSERT OR IGNORE INTO workers
			(worker_id, worker_name, status, raw_json, migrated_at, worker_class)
			VALUES ('worker-gpu', 'worker-gpu', 'idle', '{}', ?, 'gpu')`, completed); err != nil {
			t.Fatalf("seed worker %s: %v", id, err)
		}
		if _, err := s.DB().Exec(`INSERT INTO task_attempts
			(id, task_id, attempt_number, worker_id, status, completed_at, created_at, updated_at,
			 git_sha, engine_version, ffmpeg_version, config_hash, docker_image_digest)
			VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 'n7', 'cfg', 'sha256:image')`,
			id, id+"-task", "worker-gpu", status, completed, completed, completed,
			"git-"+string(rune('0'+executorVersion)), "engine-"+string(rune('0'+executorVersion))); err != nil {
			t.Fatalf("seed attempt %s: %v", id, err)
		}
		if _, err := s.DB().Exec(`INSERT INTO task_attempt_metrics
			(attempt_id, resolution_width, resolution_height, fps, media_duration_seconds,
			 scene_count, segment_count, audio_track_count, subtitle_count, template_id,
			 output_bytes, wall_clock_seconds, cpu_time_ms)
			VALUES (?, 1920, 1080, 30, 42, 3, 8, 2, 1, 'gervais', 1000, ?, 10)`, id, phaseMS/1000); err != nil {
			t.Fatalf("seed metrics %s: %v", id, err)
		}
		if _, err := s.DB().Exec(`INSERT INTO task_phase_timings
			(attempt_id, phase, duration_ms, wall_start, wall_end, phase_order, component, action, status)
			VALUES (?, 'engine.encode', ?, ?, ?, 1, 'engine', 'encode', 'ok')`,
			id, phaseMS, completed, completed); err != nil {
			t.Fatalf("seed phase %s: %v", id, err)
		}
	}

	// Historical healthy baseline: p25(engine.encode) = 100ms.
	seed("baseline", baseDay, "SUCCEEDED", 1, 100)
	// Current version 2 has one 200ms regression and one 50ms improvement.
	seed("regressed", currentDay, "SUCCEEDED", 2, 200)
	seed("improved", currentDay, "SUCCEEDED", 2, 50)
	// A failed current attempt contributes to attempts/failed but not baseline history.
	seed("failed", currentDay, "FAILED", 2, 300)

	if err := resolver.ComputeRenderPerformanceDailyRollup(ctx, currentDay); err != nil {
		t.Fatalf("ComputeRenderPerformanceDailyRollup: %v", err)
	}
	// Idempotency: the same day can be recalculated without duplicate rows.
	if err := resolver.ComputeRenderPerformanceDailyRollup(ctx, currentDay); err != nil {
		t.Fatalf("second ComputeRenderPerformanceDailyRollup: %v", err)
	}

	rows, err := resolver.ListRenderPerformanceDaily(ctx, currentDay, "", "engine.encode")
	if err != nil {
		t.Fatal(err)
	}
	// Rows are per (day, cohort, phase, worker). All three current attempts
	// share the same worker, so they aggregate into a single row whose
	// attempts/succeeded/failed and recoverable totals cover the whole day.
	if len(rows) != 1 {
		t.Fatalf("current rollup rows=%d, want 1 for the single worker cohort", len(rows))
	}
	row := rows[0]
	if row.WorkerID != "worker-gpu" {
		t.Fatalf("rollup worker_id=%q, want worker-gpu", row.WorkerID)
	}
	if row.ExecutorVersion != 2 || row.Attempts != 3 || row.Succeeded != 2 || row.Failed != 1 {
		t.Fatalf("unexpected version/status aggregate: %+v", row)
	}
	if math.Abs(row.BaselineP25MS-100) > 0.001 {
		t.Fatalf("baseline p25=%v, want 100", row.BaselineP25MS)
	}
	// max(0, 200-100) + max(0, 50-100) + max(0, 300-100) = 300.
	if math.Abs(row.RecoverableMSTotal-300) > 0.001 {
		t.Fatalf("recoverable_ms_total=%v, want 300", row.RecoverableMSTotal)
	}
	if row.EncodeMSTotal != 200+50+300 {
		t.Fatalf("encode_ms_total=%v, want 550", row.EncodeMSTotal)
	}

	_, baseKey := BuildRenderCohortKeys(RenderCohortInput{
		ExecutorID: "scene.composite", ExecutorVersion: 2, WorkerClass: "gpu",
		ResolutionWidth: 1920, ResolutionHeight: 1080, FPS: 30, OutputDuration: 42,
		SceneCount: 3, SegmentCount: 8, AudioTracks: 2, SubtitleCount: 1,
		TemplateID: "gervais", ConfigHash: "cfg",
	})
	regressions, err := resolver.CompareRenderPerformanceVersions(ctx, baseKey, "engine.encode")
	if err != nil {
		t.Fatal(err)
	}
	if len(regressions) != 1 || regressions[0].ExecutorVersion != 2 {
		t.Fatalf("version regression query=%+v, want version 2", regressions)
	}
	if regressions[0].WorkerID != "worker-gpu" {
		t.Fatalf("regression query lost worker identity: %+v", regressions)
	}
}
