package grpcserver

import (
	"testing"

	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"

	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
)

// =====================================================================
// (C) F1 — Scorecard v1 typed metrics ingest. Drives handleTaskResult
// with a fully-populated pb.TaskExecutionMetrics payload + an artifact
// declaration; asserts that the typed Go structs flow through
// IngestTaskResult → persistMetrics/PersistCacheStats/PersistCostBasis.
// =====================================================================

func TestHandleTaskResult_PersistTypedMetrics_F1(t *testing.T) {
	// outputArts dropped from destructure: fix/atomic-ingestion moved the
	// artifact-register side-effect into taskRepo.IngestTaskResultAtomic,
	// so F1's cross-check no longer references the outputArts stub.
	handler, taskRepo, jobsRepo, _ := buildSpoofHandler(t)
	fx := newSpoofFixture()

	// A non-zero typed execution metrics envelope, every writable
	// int32/int64/float64/bool populated. The derived CostBasis only
	// depends on CpuTimeMs + the 3 price fields today (TempBytesWritten
	// is NOT yet on the typed proto — derives to 0 in the cost row).
	em := &pb.TaskExecutionMetrics{
		InputBytes:            1048576,
		OutputBytes:           524288,
		BytesFromDrive:        262144,
		BytesFromBlobstore:    262144,
		BytesFromLocalCache:   524288,
		CpuTimeMs:             12345,
		PeakRssBytes:          536870912,
		FramesDecoded:         1800,
		FramesComposited:      1800,
		FramesEncoded:         1800,
		FfmpegSpeedRatio:      1.42,
		EncodePasses:          1,
		FinalConcatStreamCopy: true,
		ConcatMode:            "stream_copy",
		CpuPricePerSecond:     0.000005,
		StoragePricePerGb:     0.00012,
		NetworkPricePerGb:     0.01,
	}

	artItem, _ := structpb.NewStruct(map[string]interface{}{
		"artifact_id":   "art-1",
		"artifact_type": "video",
		"size_bytes":    524288,
	})
	tr := &pb.TaskResult{
		TaskId:           fx.taskID,
		AttemptId:        fx.wireAttemptID,
		AttemptNumber:    1,
		LeaseId:          fx.canonicalLease,
		JobId:            fx.wireJobID,
		Status:           "succeeded",
		ExecutionMetrics: em,
		OutputArtifacts:  []*structpb.Struct{artItem},
	}

	handler.handleTaskResult(fx.workerID, tr, nil)

	if taskRepo.persistMetricsCalls != 1 {
		t.Errorf("IngestTaskResultAtomic metrics-fanout calls = %d; want 1 (F1 typed-wiring)", taskRepo.persistMetricsCalls)
	}
	if taskRepo.persistCacheCalls != 1 {
		t.Errorf("IngestTaskResultAtomic cache-fanout calls = %d; want 1 (F1 typed-wiring)", taskRepo.persistCacheCalls)
	}
	if taskRepo.persistCostCalls != 1 {
		t.Errorf("IngestTaskResultAtomic cost-fanout calls = %d; want 1 (F1 typed-wiring)", taskRepo.persistCostCalls)
	}

	got := taskRepo.lastMetrics
	if got.AttemptID != fx.wireAttemptID {
		t.Errorf("AttemptMetrics.AttemptID = %q; want %q (handler must bind to wire attempt)", got.AttemptID, fx.wireAttemptID)
	}
	if got.InputBytes != em.GetInputBytes() || got.OutputBytes != em.GetOutputBytes() {
		t.Errorf("AttemptMetrics bytes mismatch: got input=%d output=%d want input=%d output=%d", got.InputBytes, got.OutputBytes, em.GetInputBytes(), em.GetOutputBytes())
	}
	if got.FramesEncoded != em.GetFramesEncoded() || got.FramesDecoded != em.GetFramesDecoded() {
		t.Errorf("AttemptMetrics frame counters mismatch: got decoded=%d encoded=%d want decoded=%d encoded=%d", got.FramesDecoded, got.FramesEncoded, em.GetFramesDecoded(), em.GetFramesEncoded())
	}
	if got.FFmpegSpeedRatio != em.GetFfmpegSpeedRatio() {
		t.Errorf("AttemptMetrics ffmpeg_speed_ratio = %v; want %v", got.FFmpegSpeedRatio, em.GetFfmpegSpeedRatio())
	}
	if got.FinalConcatStreamCopy != em.GetFinalConcatStreamCopy() || got.ConcatMode != em.GetConcatMode() {
		t.Errorf("AttemptMetrics concat fields wrong: sc=%v mode=%q want sc=%v mode=%q", got.FinalConcatStreamCopy, got.ConcatMode, em.GetFinalConcatStreamCopy(), em.GetConcatMode())
	}
	if got.TempBytesWritten != 0 {
		t.Errorf("AttemptMetrics TempBytesWritten = %d; want 0 (not yet on typed proto, derives to 0)", got.TempBytesWritten)
	}

	gotCache := taskRepo.lastCacheStats
	if gotCache.AttemptID != fx.wireAttemptID {
		t.Errorf("AttemptCacheStats.AttemptID = %q; want %q", gotCache.AttemptID, fx.wireAttemptID)
	}
	if gotCache.CacheBytesUsed != em.GetBytesFromLocalCache() {
		t.Errorf("AttemptCacheStats.CacheBytesUsed = %d; want BytesFromLocalCache=%d", gotCache.CacheBytesUsed, em.GetBytesFromLocalCache())
	}
	if gotCache.CacheHits != 0 || gotCache.CacheMisses != 0 || gotCache.CacheEvictions != 0 || gotCache.CacheCorruptions != 0 {
		t.Errorf("AttemptCacheStats must report 0 for un-derivable counters; got H=%d M=%d E=%d C=%d", gotCache.CacheHits, gotCache.CacheMisses, gotCache.CacheEvictions, gotCache.CacheCorruptions)
	}

	gotCost := taskRepo.lastCostBasis
	if gotCost.CPUTimeSecondsTotal != float64(em.GetCpuTimeMs())/1000.0 {
		t.Errorf("CostBasis.CPUTimeSecondsTotal = %v; want %v", gotCost.CPUTimeSecondsTotal, float64(em.GetCpuTimeMs())/1000.0)
	}
	if gotCost.StorageGBWritten != 0 {
		t.Errorf("CostBasis.StorageGBWritten = %v; want 0 (TempBytesWritten not on typed proto yet)", gotCost.StorageGBWritten)
	}
	if gotCost.NetworkGBEgressed != 0 {
		t.Errorf("CostBasis.NetworkGBEgressed = %v; want 0 (TODO PR-3)", gotCost.NetworkGBEgressed)
	}
	if gotCost.CPUPricePerSecond != em.GetCpuPricePerSecond() || gotCost.StoragePricePerGB != em.GetStoragePricePerGb() {
		t.Errorf("CostBasis prices mismatch: cpu=%v storage=%v want cpu=%v storage=%v", gotCost.CPUPricePerSecond, gotCost.StoragePricePerGB, em.GetCpuPricePerSecond(), em.GetStoragePricePerGb())
	}
	if gotCost.CostPerOutputMinute != 0 {
		t.Errorf("CostBasis.CostPerOutputMinute = %v; want 0 (OutputMinutesTotal is 0 today)", gotCost.CostPerOutputMinute)
	}

	if got := taskRepo.transitionCalls; got != 1 {
		t.Errorf("taskRepo.IngestTaskResultAtomic calls = %d; want 1 (happy path)", got)
	}
	if got := jobsRepo.setStatusCalls; got != 1 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 1 (Job roll-up must fire)", got)
	}
	if got := taskRepo.registerCalls; got != 1 {
		t.Errorf("IngestTaskResultAtomic artifact-fanout calls = %d; want 1 (artifact declare must fire)", got)
	}
}

func TestHandleTaskResult_PersistTypedMetrics_NilExecutionMetrics(t *testing.T) {
	handler, taskRepo, jobsRepo, _ := buildSpoofHandler(t)
	fx := newSpoofFixture()

	artItem, _ := structpb.NewStruct(map[string]interface{}{
		"artifact_id":   "art-1",
		"artifact_type": "video",
	})
	tr := &pb.TaskResult{
		TaskId:           fx.taskID,
		AttemptId:        fx.wireAttemptID,
		AttemptNumber:    1,
		LeaseId:          fx.canonicalLease,
		JobId:            fx.wireJobID,
		Status:           "succeeded",
		ExecutionMetrics: nil,
		OutputArtifacts:  []*structpb.Struct{artItem},
	}

	handler.handleTaskResult(fx.workerID, tr, nil)

	if taskRepo.persistMetricsCalls != 1 {
		t.Errorf("IngestTaskResultAtomic metrics-fanout calls = %d; want 1 (legacy-em must persist zero-row)", taskRepo.persistMetricsCalls)
	}
	if taskRepo.persistCacheCalls != 1 {
		t.Errorf("IngestTaskResultAtomic cache-fanout calls = %d; want 1 (legacy-em must persist zero-row)", taskRepo.persistCacheCalls)
	}
	if taskRepo.persistCostCalls != 1 {
		t.Errorf("IngestTaskResultAtomic cost-fanout calls = %d; want 1 (legacy-em must persist zero-row)", taskRepo.persistCostCalls)
	}
	if taskRepo.lastMetrics.AttemptID != fx.wireAttemptID {
		t.Errorf("nil-em AttemptMetrics.AttemptID = %q; want %q (handler must still bind wire attempt)", taskRepo.lastMetrics.AttemptID, fx.wireAttemptID)
	}
	if taskRepo.lastCacheStats.AttemptID != fx.wireAttemptID {
		t.Errorf("nil-em AttemptCacheStats.AttemptID = %q; want %q", taskRepo.lastCacheStats.AttemptID, fx.wireAttemptID)
	}
	if got := taskRepo.transitionCalls; got != 1 {
		t.Errorf("taskRepo.TransitionTaskToTerminalAtomic calls = %d; want 1 (legacy happy path)", got)
	}
	if got := jobsRepo.setStatusCalls; got != 1 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 1 (legacy Job roll-up)", got)
	}
	if got := taskRepo.registerCalls; got != 1 {
		t.Errorf("IngestTaskResultAtomic artifact-fanout calls = %d; want 1 (legacy artifact declare)", got)
	}
}

func TestHandleTaskResult_PersistTypedMetrics_StaleReplaySkipsMetrics(t *testing.T) {
	fx := newSpoofFixture()
	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)
	taskRepo := &spoofStubTaskRepo{
		transitionErr: store.ErrTransitionConflict,
		listTasks:     []taskgraph.Task{{ID: fx.taskID, JobID: fx.wireJobID, Status: taskgraph.StatusSucceeded}},
	}
	jobsRepo := &spoofStubJobsRepo{getJob: &jobs.Job{ID: fx.wireJobID, Status: jobs.StatusAwaitingArtifact, Revision: 0}}
	outputArts := newSpoofStubOutputArts()
	svc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, outputArts)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	handler := NewHandler(nil, nil, jobsRepo, taskRepo, attempts, nil, nil, &HandlerConfig{PushMode: true})
	handler.SetIngestionSvc(svc)
	tr := &pb.TaskResult{TaskId: fx.taskID, AttemptId: fx.wireAttemptID, AttemptNumber: 1, LeaseId: fx.canonicalLease, JobId: fx.wireJobID, Status: "succeeded", ExecutionMetrics: &pb.TaskExecutionMetrics{InputBytes: 999}}
	handler.handleTaskResult(fx.workerID, tr, nil)
	if got := taskRepo.persistMetricsCalls; got != 0 {
		t.Errorf("IngestTaskResultAtomic metrics-fanout calls = %d; want 0 (stale replay must skip step 2.5)", got)
	}
	if got := taskRepo.persistCacheCalls; got != 0 {
		t.Errorf("IngestTaskResultAtomic cache-fanout calls = %d; want 0 (stale replay must skip step 2.5)", got)
	}
	if got := taskRepo.persistCostCalls; got != 0 {
		t.Errorf("IngestTaskResultAtomic cost-fanout calls = %d; want 0 (stale replay must skip step 2.5)", got)
	}
	if got := taskRepo.transitionCalls; got != 1 {
		t.Errorf("taskRepo.IngestTaskResultAtomic calls = %d; want 1 (CAS attempted)", got)
	}
	if got := jobsRepo.setStatusCalls; got != 0 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 0 (idempotent Job roll-up skip)", got)
	}
}
