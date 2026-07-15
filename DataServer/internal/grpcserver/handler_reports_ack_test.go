package grpcserver

import (
	"errors"
	"testing"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"

	pb "velox-shared/controltransport/pb"
)

// =====================================================================
// (E) F1 stale-replay regression-guard. When TransitionTaskToTerminalAtomic
// hits ErrTransitionConflict (someone else already closed the task),
// res.AttemptClosed is false, so step 2.5's metrics persist must be
// SKIPPED. Otherwise we'd silently overwrite the canonical row's
// metrics via INSERT OR REPLACE keyed on attempt_id. This test pins
// the gate; without it a future refactor removing `if res.AttemptClosed`
// would corrupt Prometheus with stale-replay metrics.
// =====================================================================

// =====================================================================
// (F) TaskResultAck — the master must acknowledge every accepted or
// idempotent TaskResult, and must signal a conflict when the same
// attempt_id arrives with a different report hash.
// =====================================================================

// TestHandleTaskResult_SendsTaskResultAckOnSuccess verifies that a
// successful ingestion results in a TaskResultAck with no error.
func TestHandleTaskResult_SendsTaskResultAckOnSuccess(t *testing.T) {
	handler, _, _, _ := buildSpoofHandler(t)
	fx := newSpoofFixture()

	tr := &pb.TaskResult{
		TaskId:        fx.taskID,
		AttemptId:     fx.wireAttemptID,
		AttemptNumber: 1,
		LeaseId:       fx.canonicalLease,
		JobId:         fx.wireJobID,
		Status:        "succeeded",
	}

	sendCh := make(chan *outboundMessage, 1)
	sess := &workerSession{sendCh: sendCh}

	handler.handleTaskResult(fx.workerID, tr, sess)

	select {
	case out := <-sendCh:
		ack := out.Envelope.GetTaskResultAck()
		if ack == nil {
			t.Fatalf("expected TaskResultAck, got %T", out.Envelope.Msg)
		}
		if ack.TaskId != fx.taskID {
			t.Errorf("ack.TaskId = %q; want %q", ack.TaskId, fx.taskID)
		}
		if ack.JobId != fx.wireJobID {
			t.Errorf("ack.JobId = %q; want %q", ack.JobId, fx.wireJobID)
		}
		if ack.AttemptId != fx.wireAttemptID {
			t.Errorf("ack.AttemptId = %q; want %q", ack.AttemptId, fx.wireAttemptID)
		}
		if ack.Error != "" {
			t.Errorf("ack.Error = %q; want empty", ack.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for TaskResultAck")
	}
}

// TestHandleTaskResult_SendsTaskResultAckOnReportConflict verifies that
// when the ingestion service reports a raw-report conflict, the master
// still sends a TaskResultAck with error="report_conflict".
func TestHandleTaskResult_SendsTaskResultAckOnReportConflict(t *testing.T) {
	fx := newSpoofFixture()

	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)

	taskRepo := &spoofStubTaskRepo{
		reportConflictErr: taskattempts.ErrReportConflict,
		listTasks: []taskgraph.Task{
			{ID: fx.taskID, JobID: fx.wireJobID, Status: taskgraph.StatusSucceeded},
		},
	}
	jobsRepo := &spoofStubJobsRepo{
		getJob: &jobs.Job{ID: fx.wireJobID, Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0},
	}
	outputArts := newSpoofStubOutputArts()

	svc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, outputArts)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	handler := NewHandler(
		nil, nil,
		jobsRepo,
		taskRepo,
		attempts,
		nil, nil,
		&HandlerConfig{PushMode: true},
	)
	handler.SetIngestionSvc(svc)

	tr := &pb.TaskResult{
		TaskId:        fx.taskID,
		AttemptId:     fx.wireAttemptID,
		AttemptNumber: 1,
		LeaseId:       fx.canonicalLease,
		JobId:         fx.wireJobID,
		Status:        "succeeded",
		ReportHash:    "hash-b",
	}
	sendCh := make(chan *outboundMessage, 1)
	sess := &workerSession{sendCh: sendCh}
	handler.handleTaskResult(fx.workerID, tr, sess)

	select {
	case out := <-sendCh:
		ack := out.Envelope.GetTaskResultAck()
		if ack == nil {
			t.Fatalf("expected TaskResultAck, got %T", out.Envelope.Msg)
		}
		if ack.Error != "report_conflict" {
			t.Errorf("conflict ack.Error = %q; want report_conflict", ack.Error)
		}
		if ack.AttemptId != fx.wireAttemptID {
			t.Errorf("conflict ack.AttemptId = %q; want %q", ack.AttemptId, fx.wireAttemptID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for conflict TaskResultAck")
	}
}

// TestHandleTaskResult_NoAckOnInternalError verifies that when the
// ingestion service returns a non-conflict error, the handler does NOT
// send a TaskResultAck, allowing the worker to retry.
func TestHandleTaskResult_NoAckOnInternalError(t *testing.T) {
	fx := newSpoofFixture()

	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)

	// Force an internal error by making the task repo return a generic
	// error from IngestTaskResultAtomic.
	taskRepo := &spoofStubTaskRepo{
		transitionErr: errors.New("boom"),
		listTasks: []taskgraph.Task{
			{ID: fx.taskID, JobID: fx.wireJobID, Status: taskgraph.StatusSucceeded},
		},
	}
	jobsRepo := &spoofStubJobsRepo{
		getJob: &jobs.Job{ID: fx.wireJobID, Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0},
	}
	outputArts := newSpoofStubOutputArts()

	svc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, outputArts)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	handler := NewHandler(
		nil, nil,
		jobsRepo,
		taskRepo,
		attempts,
		nil, nil,
		&HandlerConfig{PushMode: true},
	)
	handler.SetIngestionSvc(svc)

	tr := &pb.TaskResult{
		TaskId:        fx.taskID,
		AttemptId:     fx.wireAttemptID,
		AttemptNumber: 1,
		LeaseId:       fx.canonicalLease,
		JobId:         fx.wireJobID,
		Status:        "succeeded",
	}
	sendCh := make(chan *outboundMessage, 1)
	sess := &workerSession{sendCh: sendCh}
	handler.handleTaskResult(fx.workerID, tr, sess)

	select {
	case out := <-sendCh:
		t.Fatalf("expected no ACK on internal error, got %T", out.Envelope.Msg)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
}
