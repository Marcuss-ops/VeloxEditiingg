package observability

import (
	"context"
	"math"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	sharedtelemetry "velox-shared/telemetry"
)

func TestApplyLiveAttemptOverlayPreservesDurableFields(t *testing.T) {
	target := AttemptSummary{
		AttemptID: "attempt-durable", AttemptNumber: 2,
		Status: taskattempts.AttemptStatusFailed, WorkerID: "worker-durable",
		ErrorCode: "FINAL_ERROR", ErrorMessage: "durable failure",
		StartedAt: "2026-08-10T10:00:00Z", CompletedAt: "2026-08-10T10:04:00Z",
		Metrics: &taskattempts.AttemptMetrics{AttemptID: "attempt-durable", FramesEncoded: 12},
	}
	applyLiveAttemptOverlay(&target, &LiveAttempt{
		AttemptID: "attempt-live", WorkerID: "worker-live", RuntimeStatus: "RUNNING",
		ProgressPhase: "render", ProgressPercent: 80, FramesEncoded: 999,
		StartedAt: "2026-08-10T09:00:00Z", LastProgressAt: "2026-08-10T10:03:00Z",
	})
	if !target.Live || target.Status != taskattempts.AttemptStatusFailed || target.WorkerID != "worker-durable" || target.ErrorCode != "FINAL_ERROR" || target.ErrorMessage != "durable failure" {
		t.Fatalf("overlay replaced durable identity/status/error: %#v", target)
	}
	if target.StartedAt != "2026-08-10T10:00:00Z" || target.CompletedAt != "2026-08-10T10:04:00Z" || target.Metrics == nil || target.Metrics.FramesEncoded != 12 {
		t.Fatalf("overlay replaced durable timestamps/metrics: %#v", target)
	}
	if target.Phase != "render" || target.ProgressPercent != 80 || target.FramesEncoded != 999 {
		t.Fatalf("overlay did not apply volatile progress: %#v", target)
	}
}

// TestService_SummarizeTaskLiveOverlayIncludesAttemptMilestones locks STEP A
// end-to-end on the read side: the live worker_task_runtime overlay must
// surface the worker's milestone timeline (attempt_milestones) inside the
// AttemptSummary consumed by fleetctl job inspect while the job is RUNNING.
func TestService_SummarizeTaskLiveOverlayIncludesAttemptMilestones(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-milestones"] = &taskgraph.Task{ID: "T-milestones", JobID: "J-milestones", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-milestones", JobID: "J-milestones", AttemptID: "A-milestones", AttemptNumber: 1,
		WorkerID: "worker-milestones", RuntimeStatus: "RUNNING", ProgressPercent: 40,
		AttemptMilestones: []sharedtelemetry.AttemptMilestoneSample{
			{Name: sharedtelemetry.MilestoneExecutionStarted, Sequence: 1, ElapsedMS: 0, OccurredAt: "2026-08-26T12:00:00Z"},
			{Name: sharedtelemetry.MilestoneAssetsRequested, Sequence: 2, ElapsedMS: 211, OccurredAt: "2026-08-26T12:00:00Z"},
			{Name: sharedtelemetry.MilestoneAllAssetsReady, Sequence: 3, ElapsedMS: 298421, OccurredAt: "2026-08-26T12:04:58Z"},
		},
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-milestones")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Live {
		t.Fatalf("live attempts = %#v", result.Attempts)
	}
	live := result.Attempts[0]
	if len(live.AttemptMilestones) != 3 {
		t.Fatalf("attempt_milestones = %+v, want 3 samples", live.AttemptMilestones)
	}
	if live.AttemptMilestones[2].Name != sharedtelemetry.MilestoneAllAssetsReady || live.AttemptMilestones[2].ElapsedMS != 298421 {
		t.Fatalf("milestone[2] = %+v, want assets.all_ready @ 298421ms", live.AttemptMilestones[2])
	}
}

func TestService_SummarizeTaskIncludesLiveAttemptProgress(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-live"] = &taskgraph.Task{ID: "T-live", JobID: "J-live", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-live", JobID: "J-live", AttemptID: "A-live", AttemptNumber: 1,
		WorkerID: "worker-live", RuntimeStatus: "RUNNING", ProgressPercent: 46,
		ProgressPhase: "building_segments", CurrentScene: 7, TotalScenes: 13,
		CurrentSegment: 12, TotalSegments: 26, FramesEncoded: 18432,
		FFmpegSpeedX: 2.37, ElapsedMS: 183421, LastProgressAt: "2026-08-10T10:03:42Z",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-live")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Live || result.Attempts[0].AttemptID != "A-live" {
		t.Fatalf("live attempts = %#v", result.Attempts)
	}
	live := result.Attempts[0]
	if live.Phase != "building_segments" || live.CurrentScene != 7 || live.CurrentSegment != 12 || live.FramesEncoded != 18432 || live.LastProgressAt == "" {
		t.Fatalf("live attempt projection = %#v", live)
	}
	if live.Metrics == nil || live.Metrics.AttemptID != "A-live" || live.Metrics.FramesEncoded != 18432 || live.Metrics.FramesDecoded != 0 {
		t.Fatalf("live metrics projection = %#v; expected the same typed AttemptMetrics shape used by final ingestion", live.Metrics)
	}
	if result.AttemptID != "A-live" || result.WorkerID != "worker-live" || result.Phase != "building_segments" || result.LastProgressAt != "2026-08-10T10:03:42Z" {
		t.Fatalf("top-level live execution identity = attempt=%q worker=%q phase=%q last_progress_at=%q; want the same canonical Attempt projection",
			result.AttemptID, result.WorkerID, result.Phase, result.LastProgressAt)
	}
	if result.Progress == nil || result.Progress.Percent != 46 || result.Progress.Scene != 7 || result.Progress.ScenesTotal != 13 || result.Progress.Segment != 12 || result.Progress.SegmentsTotal != 26 {
		t.Fatalf("top-level live progress = %#v; want canonical scene/segment projection", result.Progress)
	}
	if result.LiveMetrics == nil || result.LiveMetrics.ElapsedMS != 183421 || result.LiveMetrics.FramesEncoded != 18432 || result.LiveMetrics.FFmpegSpeedX != 2.37 {
		t.Fatalf("top-level live metrics = %#v; want canonical cumulative metrics projection", result.LiveMetrics)
	}
}
func TestService_SummarizeTaskOmitsLiveExecutionFieldsForLegacyJob(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-legacy"] = &taskgraph.Task{ID: "T-legacy", JobID: "J-legacy", Status: taskgraph.StatusSucceeded, AttemptCount: 1}

	result, err := svc.SummarizeTask(context.Background(), "T-legacy")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Phase != "" || result.Progress != nil || result.LiveMetrics != nil || result.LastProgressAt != "" {
		t.Fatalf("legacy execution unexpectedly contains live fields: %#v", result)
	}
}
func TestService_SummarizeTaskTerminalAttemptWinsOverStaleLiveProjection(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	now := time.Date(2026, 8, 10, 10, 5, 0, 0, time.UTC)
	tasks.tasks["T-converged"] = &taskgraph.Task{
		ID: "T-converged", JobID: "J-converged", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	attempts.attempts["T-converged"] = []taskattempts.TaskAttempt{{
		ID: "A-converged", TaskID: "T-converged", JobID: "J-converged", AttemptNumber: 1,
		WorkerID: "worker-final", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &now,
	}}
	attempts.metrics["A-converged"] = &taskattempts.AttemptMetrics{
		AttemptID: "A-converged", InputBytes: 100, OutputBytes: 80, FramesEncoded: 10,
	}
	// Simulate a heartbeat/runtime cleanup race: the volatile row still has
	// the same Attempt identity, but its progress is an older RUNNING view.
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-converged", JobID: "J-converged", AttemptID: "A-converged", AttemptNumber: 1,
		WorkerID: "worker-stale", RuntimeStatus: "RUNNING", ProgressPercent: 46,
		ProgressPhase: "building_segments", FramesEncoded: 999, ElapsedMS: 9999,
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-converged")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one canonical attempt", result.Attempts)
	}
	got := result.Attempts[0]
	if got.Live {
		t.Fatalf("terminal attempt incorrectly marked live: %#v", got)
	}
	if got.Status != taskattempts.AttemptStatusSucceeded || got.WorkerID != "worker-final" {
		t.Fatalf("terminal durable identity/status overwritten by stale live row: %#v", got)
	}
	if got.Metrics == nil || got.Metrics.FramesEncoded != 10 || result.TotalOutputBytes != 80 {
		t.Fatalf("final durable metrics did not converge: attempt=%#v summary=%#v", got, result)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Phase != "" || result.Progress != nil || result.LiveMetrics != nil || result.LastProgressAt != "" {
		t.Fatalf("stale live execution leaked into top-level summary: %#v", result)
	}
}
func TestService_SummarizeTaskUsesAttemptLifecycleForWallClock(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	started := time.Date(2026, 8, 16, 16, 42, 19, 0, time.UTC)
	completed := started.Add(3*time.Minute + 14*time.Second)
	tasks.tasks["T-telemetry-clock"] = &taskgraph.Task{
		ID: "T-telemetry-clock", JobID: "J-telemetry-clock",
		Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	attempts.attempts["T-telemetry-clock"] = []taskattempts.TaskAttempt{{
		ID: "A-telemetry-clock", TaskID: "T-telemetry-clock", JobID: "J-telemetry-clock",
		AttemptNumber: 1, WorkerID: "worker-clock", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &started, CompletedAt: &completed,
	}}
	attempts.phaseTimings["A-telemetry-clock"] = []taskattempts.PhaseTiming{
		{AttemptID: "A-telemetry-clock", Phase: "render", DurationMS: 190000,
			WallStart: started, WallEnd: started.Add(190 * time.Second)},
		// Regression fixture: a 1ms finalize event whose wall end was
		// accidentally stamped five minutes late.
		{AttemptID: "A-telemetry-clock", Phase: "finalize", DurationMS: 1,
			WallStart: completed, WallEnd: completed.Add(5 * time.Minute)},
	}
	attempts.metrics["A-telemetry-clock"] = &taskattempts.AttemptMetrics{
		AttemptID: "A-telemetry-clock", WallClockSeconds: 486.072,
	}

	result, err := svc.SummarizeTask(context.Background(), "T-telemetry-clock")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one attempt", result.Attempts)
	}
	got := result.Attempts[0]
	wantMS := int64((3*time.Minute + 14*time.Second) / time.Millisecond)
	if got.DurationMS != wantMS {
		t.Fatalf("attempt duration = %dms, want lifecycle duration %dms", got.DurationMS, wantMS)
	}
	if result.TotalWallTimeMS != wantMS {
		t.Fatalf("total wall time = %dms, want lifecycle duration %dms", result.TotalWallTimeMS, wantMS)
	}
	if got.Metrics == nil || math.Abs(got.Metrics.WallClockSeconds-float64(wantMS)/1000) > 1e-9 {
		t.Fatalf("wall_clock_seconds = %#v, want lifecycle duration %.3f", got.Metrics, float64(wantMS)/1000)
	}
}
func TestService_SummarizeJobLiveAttemptIdentityIsImmediateAndUnique(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-live-admin"] = &taskgraph.Task{ID: "T-live-admin", JobID: "J-live-admin", Status: taskgraph.StatusRunning, AttemptCount: 1}
	attempts.attempts["T-live-admin"] = []taskattempts.TaskAttempt{{
		ID: "A-live-admin", TaskID: "T-live-admin", JobID: "J-live-admin", AttemptNumber: 1,
		WorkerID: "worker-admin", Status: taskattempts.AttemptStatusRunning,
	}}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-live-admin", JobID: "J-live-admin", AttemptID: "A-live-admin", AttemptNumber: 1,
		WorkerID: "worker-admin", RuntimeStatus: "RUNNING", StartedAt: "2026-08-10T10:00:00Z",
	}})

	result, err := svc.SummarizeJob(context.Background(), "J-live-admin")
	if err != nil {
		t.Fatalf("SummarizeJob() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one canonical live Attempt", result.Attempts)
	}
	live := result.Attempts[0]
	if !live.Live || live.WorkerID != "worker-admin" || live.AttemptID != "A-live-admin" || live.StartedAt == "" {
		t.Fatalf("canonical live Attempt = %#v; worker_id, attempt_id and started_at must be immediate and non-empty", live)
	}
}

// TestService_SummarizeTaskExposesMasterReportTimestamps locks the
// Master-received_at/committed_at followup: the summary must expose the
// Master-local report timestamps (task_attempt_reports received_at +
// persisted_at) so the result_ingest diagnostic separates transport/heartbeat
// delay from worker runtime. Both stamps come from the Master clock; the
// worker's UTC clock is never subtracted from them.
func TestService_SummarizeTaskExposesMasterReportTimestamps(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	started := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	completed := started.Add(328*time.Second + 41*time.Millisecond)
	tasks.tasks["T-master-ts"] = &taskgraph.Task{
		ID: "T-master-ts", JobID: "J-master-ts", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	attempts.attempts["T-master-ts"] = []taskattempts.TaskAttempt{{
		ID: "A-master-ts", TaskID: "T-master-ts", JobID: "J-master-ts", AttemptNumber: 1,
		WorkerID: "worker-ts", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &started, CompletedAt: &completed,
	}}
	attempts.rawReports = map[string]string{"A-master-ts": realisticAttemptReportJSON}
	attempts.reportTimes = map[string]struct{ received, committed time.Time }{
		"A-master-ts": {
			received:  started.Add(328*time.Second + 500*time.Millisecond),
			committed: started.Add(328*time.Second + 700*time.Millisecond),
		},
	}

	result, err := svc.SummarizeTask(context.Background(), "T-master-ts")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one", result.Attempts)
	}
	got := result.Attempts[0]
	if got.MasterReceivedAt == "" || got.MasterCommittedAt == "" {
		t.Fatalf("master timestamps not exposed: received=%q committed=%q", got.MasterReceivedAt, got.MasterCommittedAt)
	}
	received, err := time.Parse(time.RFC3339Nano, got.MasterReceivedAt)
	if err != nil {
		t.Fatalf("master_received_at parse: %v", err)
	}
	committed, err := time.Parse(time.RFC3339Nano, got.MasterCommittedAt)
	if err != nil {
		t.Fatalf("master_committed_at parse: %v", err)
	}
	// The receive→commit window is Master-local (both stamps from the Master
	// clock), so the 200ms lag is safe to compute; the worker clock is never
	// involved in the subtraction.
	lag := committed.Sub(received)
	if lag != 200*time.Millisecond {
		t.Fatalf("receive→commit lag = %v, want 200ms", lag)
	}
}

func TestService_SummarizeTaskDropsOlderLiveAttemptAfterRetry(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-retry-live"] = &taskgraph.Task{
		ID: "T-retry-live", JobID: "J-retry-live", Status: taskgraph.StatusRunning, AttemptCount: 2,
	}
	attempts.attempts["T-retry-live"] = []taskattempts.TaskAttempt{{
		ID: "A-retry-new", TaskID: "T-retry-live", JobID: "J-retry-live", AttemptNumber: 2,
		WorkerID: "worker-new", Status: taskattempts.AttemptStatusRunning,
	}}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-retry-live", JobID: "J-retry-live", AttemptID: "A-retry-old", AttemptNumber: 1,
		WorkerID: "worker-old", RuntimeStatus: "RUNNING", ProgressPercent: 82,
		ProgressPhase: "building_segments", LastProgressAt: "2026-08-10T10:03:42Z",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-retry-live")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].AttemptID != "A-retry-new" {
		t.Fatalf("older live attempt was appended after retry: %#v", result.Attempts)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil || result.LastProgressAt != "" {
		t.Fatalf("older live attempt shadowed the retry at top level: %#v", result)
	}
}
func TestService_SummarizeTaskDropsDisconnectedLiveAttempt(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-disconnected-live"] = &taskgraph.Task{
		ID: "T-disconnected-live", JobID: "J-disconnected-live", Status: taskgraph.StatusRunning, AttemptCount: 1,
	}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-disconnected-live", JobID: "J-disconnected-live", AttemptID: "A-disconnected", AttemptNumber: 1,
		WorkerID: "worker-disconnected", RuntimeStatus: "PARTITIONED_SUSPECTED", ProgressPercent: 74,
		ProgressPhase: "building_segments", LastProgressAt: "2026-08-10T10:03:42Z",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-disconnected-live")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 {
		t.Fatalf("partitioned runtime was exposed as a live attempt: %#v", result.Attempts)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil || result.LiveMetrics != nil || result.LastProgressAt != "" {
		t.Fatalf("partitioned runtime leaked into top-level live projection: %#v", result)
	}
}
func TestService_SummarizeTaskDropsLiveRuntimeFromAnotherTask(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-current"] = &taskgraph.Task{ID: "T-current", JobID: "J-multi", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-other", JobID: "J-multi", AttemptID: "A-other", AttemptNumber: 1,
		WorkerID: "worker-other", RuntimeStatus: "RUNNING", ProgressPercent: 90,
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-current")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 || result.AttemptID != "" || result.WorkerID != "" {
		t.Fatalf("runtime from another task was exposed: %#v", result)
	}
}
func TestService_LiveRuntimeStatusesMapToRunningAttemptStatus(t *testing.T) {
	for _, runtimeStatus := range []string{"ACCEPTED", "STARTING", "RUNNING", "CANCELLING", "UPLOADING", "FINALIZING"} {
		if got := liveAttemptStatus(&LiveAttempt{RuntimeStatus: runtimeStatus}); got != taskattempts.AttemptStatusRunning {
			t.Fatalf("runtime status %q mapped to %q; want RUNNING", runtimeStatus, got)
		}
	}
}
func TestService_SummarizeTaskDropsRuntimeForPartitionedWorker(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-worker-partitioned"] = &taskgraph.Task{
		ID: "T-worker-partitioned", JobID: "J-worker-partitioned", Status: taskgraph.StatusRunning, AttemptCount: 1,
	}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-worker-partitioned", JobID: "J-worker-partitioned", AttemptID: "A-worker-partitioned", AttemptNumber: 1,
		WorkerID: "worker-partitioned", RuntimeStatus: "RUNNING", WorkerConnectionState: "PARTITIONED",
		ProgressPercent: 50, ProgressPhase: "building_segments",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-worker-partitioned")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 || result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil {
		t.Fatalf("runtime from partitioned worker was exposed as live: %#v", result)
	}
}
func TestService_SummarizeTaskKeepsMissingDurableAttemptAsTemporaryOverlay(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-missing-durable"] = &taskgraph.Task{ID: "T-missing-durable", JobID: "J-missing-durable", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-missing-durable", JobID: "J-missing-durable", AttemptID: "A-missing-durable", AttemptNumber: 1,
		WorkerID: "worker-live", RuntimeStatus: "RUNNING", ProgressPercent: 50,
	}})
	result, err := svc.SummarizeTask(context.Background(), "T-missing-durable")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Live || result.Attempts[0].AttemptID != "A-missing-durable" {
		t.Fatalf("missing durable row should remain a temporary live overlay: %#v", result)
	}
	if result.AttemptID != "A-missing-durable" || result.Progress == nil || result.Progress.Percent != 50 {
		t.Fatalf("temporary overlay was not projected consistently: %#v", result)
	}
}
func TestService_SummarizeTaskDropsUnmatchedLiveAttemptForTerminalTask(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-terminal-ghost"] = &taskgraph.Task{
		ID: "T-terminal-ghost", JobID: "J-terminal-ghost", Status: taskgraph.StatusFailed, AttemptCount: 1,
	}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-terminal-ghost", JobID: "J-terminal-ghost", AttemptID: "A-terminal-ghost", AttemptNumber: 1,
		WorkerID: "worker-ghost", RuntimeStatus: "RUNNING", ProgressPercent: 50,
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-terminal-ghost")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 {
		t.Fatalf("unmatched live attempt survived terminal task: %#v", result.Attempts)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil || result.LiveMetrics != nil {
		t.Fatalf("terminal ghost leaked into top-level projection: %#v", result)
	}
}
