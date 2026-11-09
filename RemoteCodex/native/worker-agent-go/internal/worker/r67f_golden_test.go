package worker

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
	"velox-shared/contract"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/pkg/logger"
)

func TestR67FGoldenTaskOfferAcceptsVideoWindowPreparation(t *testing.T) {
	data, err := os.ReadFile("testdata/golden/r67f_master_job.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Job       map[string]interface{} `json:"job"`
		TaskOffer struct {
			TaskID          string                 `json:"task_id"`
			JobID           string                 `json:"job_id"`
			AttemptID       string                 `json:"attempt_id"`
			LeaseID         string                 `json:"lease_id"`
			AttemptNumber   int32                  `json:"attempt_number"`
			Revision        int32                  `json:"revision"`
			JobRevision     int32                  `json:"job_revision"`
			ExecutorID      string                 `json:"executor_id"`
			ExecutorVersion int32                  `json:"executor_version"`
			TaskSpec        map[string]interface{} `json:"task_spec"`
		} `json:"task_offer"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	if golden.TaskOffer.ExecutorID != contract.VideoPrepareWindowExecutorID {
		t.Fatalf("golden executor = %q", golden.TaskOffer.ExecutorID)
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(golden.Job); err != nil {
		t.Fatalf("captured r67f final plan is invalid: %v", err)
	}

	reg := executor.NewRegistry()
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID: contract.VideoPrepareWindowExecutorID, Version: 1,
		ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalWindowed,
	}})
	rt := &recordingTransport{}
	cfg := newInsecureDevCfg(t)
	cfg.WorkerID = "golden-r67f-worker"
	cfg.WorkerName = "golden-r67f-worker"
	cfg.MaxActiveJobs = 2
	w := &Worker{
		config: cfg, logger: logger.New(logger.InfoLevel, io.Discard), transport: rt,
		status: StatusIdle, executorRegistry: reg,
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(2),
		pendingTasks:       make(map[string]*PendingTaskExecution),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		activeTaskLeases:   make(map[string]*ActiveTaskLease),
		taskIDsByJob:       make(map[string][]string),
	}
	spec, err := structpb.NewStruct(golden.TaskOffer.TaskSpec)
	if err != nil {
		t.Fatal(err)
	}
	offer := &pb.TaskOffer{
		TaskId: golden.TaskOffer.TaskID, JobId: golden.TaskOffer.JobID,
		AttemptId: golden.TaskOffer.AttemptID, LeaseId: golden.TaskOffer.LeaseID,
		AttemptNumber: golden.TaskOffer.AttemptNumber, Revision: golden.TaskOffer.Revision,
		JobRevision: golden.TaskOffer.JobRevision, ExecutorId: golden.TaskOffer.ExecutorID,
		ExecutorVersion: golden.TaskOffer.ExecutorVersion, TaskSpec: spec,
	}
	w.handleTaskOfferMessage(context.Background(), controltransport.NewTypedMessage(
		controltransport.MsgTaskOffer, "master", controltransport.ProtocolVersionCurrent, offer,
	))
	msg, ok := rt.last()
	if !ok || msg.Type != controltransport.MsgTaskAccepted {
		t.Fatalf("worker reply = %q, want %q", msg.Type, controltransport.MsgTaskAccepted)
	}
}
