package grpcserver

import (
	"testing"
	"time"

	pb "velox-shared/controltransport/pb"
)

func TestHandleTaskResult_DetailedPhaseIdentityUsesMasterCanonicalValues(t *testing.T) {
	handler, taskRepo, _, _ := buildSpoofHandler(t)
	fx := newSpoofFixture()

	tr := &pb.TaskResult{
		TaskId:        fx.taskID,
		AttemptId:     fx.wireAttemptID,
		AttemptNumber: 1,
		LeaseId:       fx.canonicalLease,
		JobId:         fx.wireJobID,
		Status:        "failed",
		PhaseTimings: []*pb.PhaseTimingDetailed{
			{
				Origin:           "engine",
				Scope:            "segment",
				EventIndex:       7,
				Component:        "engine.encode",
				Action:           "frame_submit",
				ExecutorId:       "executor.attacker",
				ExecutorVersion:  999,
				WorkerSnapshotId: "snapshot.attacker",
				SegmentIndex:     2,
				Status:           "failed",
				ErrorCode:        "ENCODE_FAILED",
			},
		},
		PartialPhaseMetrics: []*pb.PhaseTimingDetailed{
			{
				Origin:           "worker",
				Scope:            "attempt",
				EventIndex:       8,
				Component:        "worker",
				Action:           "cleanup",
				ExecutorId:       "executor.attacker",
				ExecutorVersion:  999,
				WorkerSnapshotId: "snapshot.attacker",
				Status:           "ok",
			},
		},
	}

	handler.handleTaskResult(fx.workerID, tr, nil, time.Time{})

	if len(taskRepo.lastPhaseTimings) != 1 {
		t.Fatalf("persisted phase timings = %d; want 1", len(taskRepo.lastPhaseTimings))
	}
	got := taskRepo.lastPhaseTimings[0]
	if got.AttemptID != fx.wireAttemptID || got.TaskID != fx.taskID || got.JobID != fx.wireJobID || got.WorkerID != fx.workerID {
		t.Fatalf("phase identity = (%q, %q, %q, %q); want (%q, %q, %q, %q)", got.AttemptID, got.TaskID, got.JobID, got.WorkerID, fx.wireAttemptID, fx.taskID, fx.wireJobID, fx.workerID)
	}
	if got.WorkerSnapshotID != "snapshot.master" {
		t.Errorf("phase WorkerSnapshotID = %q; want master snapshot", got.WorkerSnapshotID)
	}
	if got.ExecutorID != "executor.master" || got.ExecutorVersion != 7 {
		t.Errorf("phase executor identity = (%q, %d); want (executor.master, 7)", got.ExecutorID, got.ExecutorVersion)
	}

	if len(taskRepo.lastPartialPhases) != 1 {
		t.Fatalf("persisted partial phase timings = %d; want 1", len(taskRepo.lastPartialPhases))
	}
	partial := taskRepo.lastPartialPhases[0]
	if partial.WorkerSnapshotID != "snapshot.master" || partial.ExecutorID != "executor.master" || partial.ExecutorVersion != 7 {
		t.Errorf("partial phase canonical identity = snapshot=%q executor=(%q, %d); want snapshot.master/executor.master/7", partial.WorkerSnapshotID, partial.ExecutorID, partial.ExecutorVersion)
	}
}
