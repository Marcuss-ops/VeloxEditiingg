package taskrunner

import (
	"context"
	"testing"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

func TestMergeStatsInto_StampsFFmpegAggregate(t *testing.T) {
	agg := ffmpegrunner.NewAggregator()
	agg.Add(ffmpegrunner.FFmpegResult{
		Operation:      ffmpegrunner.OperationCompose,
		ProcessSpawnMS: 10, FirstOutputMS: 40, ProcessingMS: 500,
		ExitWaitMS: 2, ProcessWallMS: 545,
	})
	report := &TaskExecutionReport{FFmpegProfiles: agg, Metrics: map[string]interface{}{}}
	(&TaskRunner{}).mergeStatsInto(report)

	value, ok := report.Metrics["ffmpeg.aggregate"].(ffmpegrunner.ProfileAggregate)
	if !ok {
		t.Fatalf("metrics[ffmpeg.aggregate] missing or wrong type: %#v", report.Metrics["ffmpeg.aggregate"])
	}
	if value.ProcessCount != 1 {
		t.Errorf("aggregate ProcessCount = %d, want 1", value.ProcessCount)
	}
	if value.TotalSpawnMS != 10 || value.TotalProcessingMS != 500 || value.TotalWallMS != 545 {
		t.Errorf("aggregate totals = spawn %d processing %d wall %d, want 10/500/545", value.TotalSpawnMS, value.TotalProcessingMS, value.TotalWallMS)
	}
	if _, ok := value.Operations[string(ffmpegrunner.OperationCompose)]; !ok {
		t.Errorf("aggregate operations missing compose bucket: %+v", value.Operations)
	}
}

func TestMergeStatsInto_SkipsEmptyAggregate(t *testing.T) {
	report := &TaskExecutionReport{FFmpegProfiles: ffmpegrunner.NewAggregator(), Metrics: map[string]interface{}{}}
	(&TaskRunner{}).mergeStatsInto(report)
	if _, ok := report.Metrics["ffmpeg.aggregate"]; ok {
		t.Errorf("ffmpeg.aggregate stamped for an empty aggregator: %v", report.Metrics["ffmpeg.aggregate"])
	}
}

func TestMergeStatsInto_NoAggregatorIsANoOp(t *testing.T) {
	report := &TaskExecutionReport{Metrics: map[string]interface{}{}}
	(&TaskRunner{}).mergeStatsInto(report)
	if _, ok := report.Metrics["ffmpeg.aggregate"]; ok {
		t.Errorf("ffmpeg.aggregate stamped without an aggregator: %v", report.Metrics["ffmpeg.aggregate"])
	}
}

// TestTypedMetricsFromMap_PreservesAggregateInLegacyMap ensures the typed
// mirror conversion does not drop the already-stamped aggregate (the
// worker recomputes report.TypedMetrics from the metrics map at the
// attempt boundary).
func TestTypedMetricsFromMap_PreservesAggregateInLegacyMap(t *testing.T) {
	agg := ffmpegrunner.NewAggregator()
	agg.Add(ffmpegrunner.FFmpegResult{Operation: ffmpegrunner.OperationEncode, ProcessWallMS: 970})
	report := &TaskExecutionReport{FFmpegProfiles: agg, Metrics: map[string]interface{}{}}
	(&TaskRunner{}).mergeStatsInto(report)

	typed := TypedMetricsFromMap(report.Metrics)
	if typed == nil {
		t.Fatal("typed metrics are nil")
	}
	// The aggregate must survive inside the legacy map used for the
	// typed projection (it is not a typed field itself).
	value, ok := report.Metrics["ffmpeg.aggregate"].(ffmpegrunner.ProfileAggregate)
	if !ok || value.ProcessCount != 1 {
		t.Errorf("aggregate lost after TypedMetricsFromMap round-trip: %#v", report.Metrics["ffmpeg.aggregate"])
	}
}

// ffmpegSinkExec is a fake executor that pushes profiles into the attempt
// sink exactly like runCommandExecutor does (optional interface on the
// ExecutionContext).
type ffmpegSinkExec struct {
	desc    executor.Descriptor
	failure bool
}

func (f *ffmpegSinkExec) Descriptor() executor.Descriptor    { return f.desc }
func (f *ffmpegSinkExec) Validate(_ executor.TaskSpec) error { return nil }

// newRunnerWithExec builds a TaskRunner from any Executor implementation
// (the shared newTestRunner helper only accepts *fakeExec).
func newRunnerWithExec(exec executor.Executor) *TaskRunner {
	reg := executor.NewRegistry()
	reg.MustRegister(exec)
	return NewTaskRunner(reg, nil)
}

func (f *ffmpegSinkExec) Execute(_ context.Context, ec executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	sink, ok := ec.(interface {
		FFmpegProfiles() *ffmpegrunner.Aggregator
	})
	if ok && sink.FFmpegProfiles() != nil {
		sink.FFmpegProfiles().Add(ffmpegrunner.FFmpegResult{
			Operation:      ffmpegrunner.OperationCompose,
			ProcessSpawnMS: 10, FirstOutputMS: 40, ProcessingMS: 500,
			ExitWaitMS: 2, ProcessWallMS: 545,
		})
	}
	started := time.Now().UTC()
	if f.failure {
		return executor.ExecutionResult{Status: "failed", ErrorCode: "command_failed", ErrorDetail: "boom", StartedAt: started, CompletedAt: time.Now().UTC()}, nil
	}
	return executor.ExecutionResult{Status: "succeeded", StartedAt: started, CompletedAt: time.Now().UTC()}, nil
}

// TestRunner_FFmpegAggregateStampedEndToEnd closes the B2 seam: Run creates
// the per-attempt aggregator → runnerContext exposes it → the executor pushes
// → mergeStatsInto stamps report.Metrics["ffmpeg.aggregate"].
func TestRunner_FFmpegAggregateStampedEndToEnd(t *testing.T) {
	exec := &ffmpegSinkExec{desc: makeDesc("ffmpeg.ok.v1", 1)}
	rep, err := newRunnerWithExec(exec).Run(context.Background(), goodSpec("ffmpeg.ok.v1"))
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if !rep.Succeeded() {
		t.Fatalf("Status = %q, want succeeded", rep.Status)
	}
	value, ok := rep.Metrics["ffmpeg.aggregate"].(ffmpegrunner.ProfileAggregate)
	if !ok {
		t.Fatalf("Metrics[ffmpeg.aggregate] missing or wrong type: %#v", rep.Metrics["ffmpeg.aggregate"])
	}
	if value.ProcessCount != 1 || value.TotalSpawnMS != 10 || value.TotalProcessingMS != 500 {
		t.Errorf("aggregate = %+v, want count 1 / spawn 10 / processing 500", value)
	}
}

// TestRunner_FFmpegAggregateStampedOnFailure: the failure path (completeError)
// must stamp the aggregate too — partial ffmpeg activity explains failures.
func TestRunner_FFmpegAggregateStampedOnFailure(t *testing.T) {
	exec := &ffmpegSinkExec{desc: makeDesc("ffmpeg.fail.v1", 1), failure: true}
	rep, err := newRunnerWithExec(exec).Run(context.Background(), goodSpec("ffmpeg.fail.v1"))
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if rep.Succeeded() {
		t.Fatal("expected failed")
	}
	value, ok := rep.Metrics["ffmpeg.aggregate"].(ffmpegrunner.ProfileAggregate)
	if !ok {
		t.Fatalf("Metrics[ffmpeg.aggregate] missing on failure: %#v", rep.Metrics["ffmpeg.aggregate"])
	}
	if value.ProcessCount != 1 {
		t.Errorf("aggregate ProcessCount = %d, want 1 on failure path", value.ProcessCount)
	}
}

// TestRunner_NoFFmpegProfilesLeavesNoAggregate: an executor that never runs
// ffmpeg must not produce an ffmpeg.aggregate key at all.
func TestRunner_NoFFmpegProfilesLeavesNoAggregate(t *testing.T) {
	exec := &fakeExec{desc: makeDesc("plain.v1", 1)}
	rep, err := newTestRunner(exec).Run(context.Background(), goodSpec("plain.v1"))
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if _, ok := rep.Metrics["ffmpeg.aggregate"]; ok {
		t.Errorf("ffmpeg.aggregate present without any ffmpeg profile: %#v", rep.Metrics["ffmpeg.aggregate"])
	}
}
