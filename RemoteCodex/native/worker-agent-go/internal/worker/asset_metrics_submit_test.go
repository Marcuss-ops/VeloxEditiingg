package worker

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestSubmitTaskResult_PhaseTimings(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-phases-test", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey: "scene.composite.v1@1",
		DetailedPhases: []taskrunner.DetailedPhaseTiming{
			{
				PhaseOrder:      1,
				Component:       "runner",
				Action:          "cache_lookup",
				StartedAt:       start,
				CompletedAt:     start.Add(10 * time.Millisecond),
				DurationMS:      10,
				Status:          "ok",
				Origin:          "worker",
				Scope:           "attempt",
				EventType:       "completed",
				EventName:       "cache_lookup",
				EventIndex:      0,
				Phase:           "cache_lookup",
				ExecutorID:      "scene.composite.v1",
				ExecutorVersion: 1,
			},
			{
				PhaseOrder:      2,
				Component:       "runner",
				Action:          "report",
				StartedAt:       start.Add(10 * time.Millisecond),
				CompletedAt:     start.Add(12 * time.Millisecond),
				DurationMS:      2,
				Status:          "failed",
				ErrorCode:       "EXECUTE_FAILED",
				ErrorMessage:    "boom",
				Origin:          "worker",
				Scope:           "attempt",
				EventType:       "failed",
				EventName:       "report",
				EventIndex:      1,
				Phase:           "report",
				ExecutorID:      "scene.composite.v1",
				ExecutorVersion: 1,
			},
		},
	}
	pte := &PendingTaskExecution{JobID: "job-phases-test", ExecutorID: "scene.composite.v1", LeaseID: "lease-phases-test"}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-phases-test", "attempt-phases-test", report, nil)

	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v, want TaskResult", message)
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if len(result.PhaseTimings) != 2 {
		t.Fatalf("PhaseTimings len = %d, want 2", len(result.PhaseTimings))
	}
	first := result.PhaseTimings[0]
	if first.PhaseOrder != 1 || first.Component != "runner" || first.Action != "cache_lookup" {
		t.Errorf("first phase = %d %s.%s", first.PhaseOrder, first.Component, first.Action)
	}
	if first.EventIndex != 0 || first.EventType != "completed" || first.Status != "ok" {
		t.Errorf("first phase event = idx=%d type=%q status=%q", first.EventIndex, first.EventType, first.Status)
	}
	if first.Origin != "worker" || first.Scope != "attempt" || first.Phase != "cache_lookup" {
		t.Errorf("first phase taxonomy = %q/%q phase=%q", first.Origin, first.Scope, first.Phase)
	}
	if first.ExecutorId != "scene.composite.v1" || first.ExecutorVersion != 1 {
		t.Errorf("first phase identity = %s@%d", first.ExecutorId, first.ExecutorVersion)
	}
	if first.DurationMs != 10 {
		t.Errorf("first phase duration_ms = %d, want 10", first.DurationMs)
	}
	second := result.PhaseTimings[1]
	if second.LeaseId != "lease-phases-test" {
		t.Errorf("lease stamped = %q, want lease-phases-test", second.LeaseId)
	}
	if second.EventType != "failed" || second.ErrorCode != "EXECUTE_FAILED" || second.ErrorMessage != "boom" {
		t.Errorf("second phase failure = type=%q code=%q msg=%q", second.EventType, second.ErrorCode, second.ErrorMessage)
	}
}

func TestSubmitTaskResult_PreservesTenDistinctEngineEncodeEventsOnSuccessAndFailure(t *testing.T) {
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	phases := make([]taskrunner.DetailedPhaseTiming, 0, 10)
	for i := 0; i < 10; i++ {
		phases = append(phases, taskrunner.DetailedPhaseTiming{
			PhaseOrder:   i + 1,
			Component:    "engine",
			Action:       "encode",
			Origin:       "engine",
			Scope:        "segment",
			EventType:    "completed",
			EventName:    "engine.encode",
			EventIndex:   int64(100 + i),
			Phase:        "encode",
			SegmentIndex: int32(i),
			StartedAt:    start.Add(time.Duration(i) * time.Millisecond),
			CompletedAt:  start.Add(time.Duration(i+1) * time.Millisecond),
			DurationMS:   1,
			Status:       "ok",
			FramesIn:     int64(90 + i),
			FramesOut:    int64(90 + i),
		})
	}

	for _, tc := range []struct {
		name   string
		status string
		err    error
	}{
		{name: "succeeded", status: "succeeded"},
		{name: "failed", status: "failed", err: errors.New("encoder crashed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &recordingTransport{}
			w := &Worker{
				config:    &config.WorkerConfig{WorkerID: "worker-encode-events", ProtocolVersion: "v3"},
				logger:    logger.New(logger.InfoLevel, io.Discard),
				transport: transport,
			}
			report := &taskrunner.TaskExecutionReport{
				Status:         tc.status,
				ExecutorKey:    "scene.composite.v1@1",
				DetailedPhases: phases,
			}
			pte := &PendingTaskExecution{JobID: "job-encode-events", ExecutorID: "scene.composite.v1", ExecutorVersion: 1, LeaseID: "lease-encode-events"}

			wireTestReporter(w, nil).Submit(context.Background(), pte, "task-encode-events", "attempt-encode-events", report, tc.err)

			message, ok := transport.last()
			if !ok {
				t.Fatal("expected a TaskResult message")
			}
			result, ok := message.TypedPayload.(*pb.TaskResult)
			if !ok || result == nil {
				t.Fatalf("typed task result = %#v", message.TypedPayload)
			}
			if result.Status != tc.status {
				t.Fatalf("status = %q, want %q", result.Status, tc.status)
			}
			if len(result.PhaseTimings) != 10 {
				t.Fatalf("phase timings = %d, want 10 distinct engine.encode events", len(result.PhaseTimings))
			}
			for i, phase := range result.PhaseTimings {
				if phase.Component != "engine" || phase.Action != "encode" || phase.Scope != "segment" {
					t.Errorf("phase[%d] identity = %q.%q scope=%q", i, phase.Component, phase.Action, phase.Scope)
				}
				if phase.EventIndex != int64(100+i) || phase.SegmentIndex != int32(i) {
					t.Errorf("phase[%d] event/segment = %d/%d, want %d/%d", i, phase.EventIndex, phase.SegmentIndex, 100+i, i)
				}
				if phase.LeaseId != pte.LeaseID {
					t.Errorf("phase[%d] lease_id = %q, want %q", i, phase.LeaseId, pte.LeaseID)
				}
				if phase.ExecutorId != pte.ExecutorID || phase.ExecutorVersion != 1 {
					t.Errorf("phase[%d] executor identity = %q@%d, want %q@1", i, phase.ExecutorId, phase.ExecutorVersion, pte.ExecutorID)
				}
			}

		})
	}
}

// TestSubmitTaskResult_FailedRenderPreservesDetailedPhases verifies that
// submitTaskResult does not discard the worker report when execution fails.
// Failed renders must still deliver every phase completed before the error,
// including the failed phase itself, so the master can persist partial work.
func TestSubmitTaskResult_FailedRenderPreservesDetailedPhases(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-failed-phases", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey: "scene.composite.v1@1",
		Status:      "failed",
		ErrorCode:   "EXECUTE_FAILED",
		ErrorDetail: "encoder crashed",
		Metrics: map[string]interface{}{
			"engine.frames":             int64(144),
			"engine.speed_x":            float64(1.75),
			"engine.encode_passes":      int64(2),
			"native.total_ms":           int64(2500),
			"quality.ffprobe.valid":     int64(1),
			"quality.black.frame.ratio": float64(0.02),
			"io.disk.read.bytes":        int64(4096),
			"wasted.cpu.ms":             int64(88),
			"wasted.download.bytes":     int64(512),
			"completed.segments":        int64(2),
			"error.component":           "engine",
			"error.phase":               "encode",
		},
		Segments: []taskrunner.SegmentTiming{{
			SegmentIndex: 3, StartedOffsetMS: 1.5, FinishedOffsetMS: 7.25,
			WorkerSlot: 2, CPUThreads: 4, ParallelGroup: "encode-group",
		}},
		DetailedPhases: []taskrunner.DetailedPhaseTiming{
			{
				PhaseOrder: 1, Component: "runner", Action: "cache_lookup",
				StartedAt: started, CompletedAt: started.Add(5 * time.Millisecond),
				DurationMS: 5, Status: "ok", Origin: "worker", Scope: "attempt",
				EventType: "completed", EventName: "cache_lookup", EventIndex: 0,
				Phase: "cache_lookup", ExecutorID: "scene.composite.v1", ExecutorVersion: 1,
			},
			{
				PhaseOrder: 2, Component: "runner", Action: "execute",
				StartedAt: started.Add(5 * time.Millisecond), CompletedAt: started.Add(25 * time.Millisecond),
				DurationMS: 20, Status: "failed", ErrorCode: "EXECUTE_FAILED",
				ErrorMessage: "encoder crashed", Origin: "worker", Scope: "attempt",
				EventType: "failed", EventName: "execute", EventIndex: 1,
				Phase: "render", ExecutorID: "scene.composite.v1", ExecutorVersion: 1,
			},
		},
	}
	pte := &PendingTaskExecution{
		JobID: "job-failed-phases", ExecutorID: "scene.composite.v1", LeaseID: "lease-failed-phases",
	}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-failed-phases", "attempt-failed-phases", report, errors.New("encoder crashed"))

	message, ok := transport.last()
	if !ok || message.Type != controltransport.MsgTaskResult {
		t.Fatalf("last message = %#v, want TaskResult", message)
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if result.Status != "failed" {
		t.Fatalf("status = %q, want failed", result.Status)
	}
	if result.ErrorCode != "EXECUTE_FAILED" {
		t.Fatalf("error code = %q, want EXECUTE_FAILED from the failed report", result.ErrorCode)
	}
	if result.ErrorDetail != "encoder crashed" {
		t.Fatalf("error detail = %q, want encoder crashed", result.ErrorDetail)
	}
	if result.ExecutionMetrics == nil || result.ExecutionMetrics.FramesEncoded != 144 || result.ExecutionMetrics.FfmpegSpeedRatio != 1.75 || result.ExecutionMetrics.EncodePasses != 2 || result.ExecutionMetrics.WallClockSeconds != 2.5 || result.ExecutionMetrics.FfprobeValid != 1 || result.ExecutionMetrics.BlackFrameRatio != 0.02 || result.ExecutionMetrics.DiskReadBytes != 4096 || result.ExecutionMetrics.WastedCpuMs != 88 || result.ExecutionMetrics.WastedDownloadBytes != 512 || result.ExecutionMetrics.CompletedSegments != 2 || result.ExecutionMetrics.ErrorComponent != "engine" || result.ExecutionMetrics.ErrorPhase != "encode" {
		t.Fatalf("typed execution metrics = %+v, want native/quality/io/waste fields", result.ExecutionMetrics)
	}
	if len(result.PhaseTimings) != len(report.DetailedPhases) {
		t.Fatalf("phase timings = %d, want %d", len(result.PhaseTimings), len(report.DetailedPhases))
	}
	for i, phase := range result.PhaseTimings {
		if phase.PhaseOrder != int32(i+1) || phase.EventIndex != int64(i) {
			t.Errorf("phase[%d] order/index = %d/%d, want %d/%d", i, phase.PhaseOrder, phase.EventIndex, i+1, i)
		}
	}
	if result.PhaseTimings[1].Status != "failed" || result.PhaseTimings[1].ErrorCode != "EXECUTE_FAILED" {
		t.Fatalf("failed phase = status:%q code:%q", result.PhaseTimings[1].Status, result.PhaseTimings[1].ErrorCode)
	}
	for i, phase := range result.PhaseTimings {
		if phase.LeaseId != pte.LeaseID {
			t.Errorf("phase[%d] lease_id = %q, want %q", i, phase.LeaseId, pte.LeaseID)
		}
	}
	if len(result.SegmentTimings) != 1 {
		t.Fatalf("segment timings = %d, want 1", len(result.SegmentTimings))
	}
	segment := result.SegmentTimings[0]
	if segment.SegmentIndex != 3 || segment.FinishedOffsetMs != 7.25 || segment.WorkerSlot != 2 || segment.CpuThreads != 4 || segment.ParallelGroup != "encode-group" {
		t.Fatalf("failed segment timing = %+v", segment)
	}
}

func TestSubmitTaskResult_ObservabilitySummaryPhasesReachWire(t *testing.T) {
	transport := &recordingTransport{}
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-summary-wire", ProtocolVersion: "v3"},
		logger:    logger.New(logger.InfoLevel, io.Discard),
		transport: transport,
	}
	categories := []string{"audio", "subtitle", "io", "quality", "retry", "waste"}
	report := &taskrunner.TaskExecutionReport{
		ExecutorKey:    "scene.composite.v1@1",
		DetailedPhases: make([]taskrunner.DetailedPhaseTiming, 0, len(categories)),
	}
	for i, category := range categories {
		report.DetailedPhases = append(report.DetailedPhases, taskrunner.DetailedPhaseTiming{
			Origin: "validation", Scope: "attempt", Component: category,
			Action: "summary", Phase: category, EventType: "summary",
			EventName: category, EventIndex: int64(i), Status: "ok",
			MetadataJSON: `{"events":1}`,
		})
	}
	pte := &PendingTaskExecution{JobID: "job-summary-wire", ExecutorID: "scene.composite.v1", LeaseID: "lease-summary-wire"}

	wireTestReporter(w, nil).Submit(context.Background(), pte, "task-summary-wire", "attempt-summary-wire", report, nil)

	message, ok := transport.last()
	if !ok {
		t.Fatal("expected a TaskResult message")
	}
	result, ok := message.TypedPayload.(*pb.TaskResult)
	if !ok || result == nil {
		t.Fatalf("typed task result = %#v", message.TypedPayload)
	}
	if len(result.PhaseTimings) != len(categories) {
		t.Fatalf("summary phase timings = %d, want %d", len(result.PhaseTimings), len(categories))
	}
	seen := make(map[string]bool, len(categories))
	for i, phase := range result.PhaseTimings {
		if phase.EventType != "summary" || phase.Action != "summary" || phase.MetadataJson != `{"events":1}` {
			t.Errorf("summary[%d] = type:%q action:%q metadata:%q", i, phase.EventType, phase.Action, phase.MetadataJson)
		}
		if phase.EventIndex != int64(i) {
			t.Errorf("summary[%d] event_index = %d, want %d", i, phase.EventIndex, i)
		}
		seen[phase.Component] = true
	}
	for _, category := range categories {
		if !seen[category] {
			t.Errorf("wire summaries missing category %q", category)
		}
	}
}
