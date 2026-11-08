package worker

import (
	"context"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/storage"
)

type cacheFirstWindowExecutor struct {
	hit      bool
	executed bool
	probes   int
}

func (e *cacheFirstWindowExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: contract.VideoPrepareWindowExecutorID, Version: 1,
		ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalWindowed,
		Deterministic: true, Cacheable: true,
	}
}

func (e *cacheFirstWindowExecutor) Validate(executor.TaskSpec) error { return nil }

func (e *cacheFirstWindowExecutor) ProbeLocalPreparation(context.Context, executor.TaskSpec, *storage.Resolver) (bool, error) {
	e.probes++
	return e.hit, nil
}

func (e *cacheFirstWindowExecutor) Execute(context.Context, executor.ExecutionContext, executor.TaskSpec) (executor.ExecutionResult, error) {
	e.executed = true
	return executor.ExecutionResult{Status: "succeeded"}, nil
}

func TestDispatchTaskRunner_SkipsSourceResolutionOnPreparedWindowCacheHit(t *testing.T) {
	w, _ := newDispatchTestWorker(t)
	probe := &cacheFirstWindowExecutor{hit: true}
	registry := executor.NewRegistry()
	registry.MustRegister(probe)
	w.executorRegistry = registry
	w.taskRunner = taskrunner.NewTaskRunner(registry, w.logger)

	// This source deliberately cannot be resolved: no Drive locator exists.
	// A cache miss would therefore fail before Execute. A true cache hit must
	// never touch the resolver.
	payload := map[string]interface{}{
		contract.VideoPrepareWindowPayloadKey: map[string]interface{}{
			"version":            contract.VideoPrepareWindowVersion,
			"source":             map[string]interface{}{"asset_id": "drive:missing", "source_kind": "drive", "sha256": strings.Repeat("a", 64), "size_bytes": 1024, "kind": "video"},
			"source_start_frame": 0, "source_frame_count": 24, "source_fps_num": 24, "source_fps_den": 1,
			"output_frame_count": 24, "timeline_revision": 1, "timeline_sha256": strings.Repeat("b", 64),
			"output_profile_id": contract.CanonicalVideoProfileIDV1, "output_profile_revision": "video-window-preparer.v1:" + strings.Repeat("c", 64),
		},
	}
	pte := &PendingTaskExecution{
		TaskID: "task-cache-first", JobID: "job-cache-first", AttemptID: "attempt-cache-first",
		ExecutorID: contract.VideoPrepareWindowExecutorID, ExecutorVersion: 1,
		Spec: executor.TaskSpec{Version: 1, JobID: "job-cache-first", ExecutorID: contract.VideoPrepareWindowExecutorID, Payload: payload},
	}
	if _, err := w.dispatchTaskRunner(context.Background(), pte); err != nil {
		t.Fatalf("cache-first dispatch: %v", err)
	}
	if probe.probes != 1 || !probe.executed {
		t.Fatalf("probe called %d times, executed=%v; want one probe and execution", probe.probes, probe.executed)
	}
}

func TestDispatchTaskRunner_ResolvesSourceOnPreparedWindowCacheMiss(t *testing.T) {
	w, _ := newDispatchTestWorker(t)
	probe := &cacheFirstWindowExecutor{hit: false}
	registry := executor.NewRegistry()
	registry.MustRegister(probe)
	w.executorRegistry = registry
	w.taskRunner = taskrunner.NewTaskRunner(registry, w.logger)

	payload := map[string]interface{}{
		contract.VideoPrepareWindowPayloadKey: map[string]interface{}{
			"version":            contract.VideoPrepareWindowVersion,
			"source":             map[string]interface{}{"asset_id": "drive:missing", "source_kind": "drive", "sha256": strings.Repeat("a", 64), "size_bytes": 1024, "kind": "video"},
			"source_start_frame": 0, "source_frame_count": 24, "source_fps_num": 24, "source_fps_den": 1,
			"output_frame_count": 24, "timeline_revision": 1, "timeline_sha256": strings.Repeat("b", 64),
			"output_profile_id": contract.CanonicalVideoProfileIDV1, "output_profile_revision": "video-window-preparer.v1:" + strings.Repeat("c", 64),
		},
	}
	pte := &PendingTaskExecution{
		TaskID: "task-cache-miss", JobID: "job-cache-miss", AttemptID: "attempt-cache-miss",
		ExecutorID: contract.VideoPrepareWindowExecutorID, ExecutorVersion: 1,
		Spec: executor.TaskSpec{Version: 1, JobID: "job-cache-miss", ExecutorID: contract.VideoPrepareWindowExecutorID, Payload: payload},
	}
	if _, err := w.dispatchTaskRunner(context.Background(), pte); err == nil {
		t.Fatal("cache miss with an unresolvable source should fail during asset resolution")
	}
	if probe.executed {
		t.Fatal("executor ran after a failed source resolution")
	}
}
