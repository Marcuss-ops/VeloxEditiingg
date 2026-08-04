package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestLogArtifactProtocol_EmitsStableIdentityAndTimingFields(t *testing.T) {
	var output bytes.Buffer
	log := logger.New(logger.InfoLevel, &output)
	w := &Worker{logger: log}
	pte := &PendingTaskExecution{
		TaskID:    "task-log-test",
		JobID:     "job-log-test",
		AttemptID: "attempt-log-test",
		LeaseID:   "lease-log-test",
	}

	w.logArtifactProtocol(
		"ARTIFACT_UPLOAD_PLAN_RECEIVED",
		pte,
		time.Now().Add(-25*time.Millisecond),
		"commit-log-test",
		"artifact-log-test",
		"upload-log-test",
		map[string]interface{}{"target_count": 2},
	)

	event := decodeProtocolEvent(t, output.String())
	for key, want := range map[string]string{
		"event":       "ARTIFACT_UPLOAD_PLAN_RECEIVED",
		"job_id":      "job-log-test",
		"task_id":     "task-log-test",
		"attempt_id":  "attempt-log-test",
		"lease_id":    "lease-log-test",
		"commit_id":   "commit-log-test",
		"artifact_id": "artifact-log-test",
		"upload_id":   "upload-log-test",
	} {
		if got := event[key]; got != want {
			t.Errorf("event[%q] = %#v, want %q", key, got, want)
		}
	}
	if got, ok := event["target_count"].(float64); !ok || got != 2 {
		t.Errorf("event[target_count] = %#v, want 2", event["target_count"])
	}
	if elapsed, ok := event["elapsed_ms"].(float64); !ok || elapsed < 0 {
		t.Errorf("event[elapsed_ms] = %#v, want non-negative number", event["elapsed_ms"])
	}
}

func TestPublishArtifactsV1_LogsProtocolBoundariesInOrder(t *testing.T) {
	var output bytes.Buffer
	log := logger.New(logger.InfoLevel, &output)
	workerID := "worker-protocol-test"
	transport := &protocolRecordingTransport{}
	spoolStore, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer spoolStore.Close()

	publisherRegistry := publisher.NewRegistry()
	if err := publisherRegistry.Register(protocolUploadTransport{}); err != nil {
		t.Fatalf("publisher registry: %v", err)
	}
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        workerID,
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
		},
		logger:            log,
		transport:         transport,
		publisherRegistry: publisherRegistry,
		outputSpool:       spoolStore,
	}
	transport.worker = w

	path := t.TempDir() + "/render.mp4"
	if err := os.WriteFile(path, []byte("rendered"), 0o600); err != nil {
		t.Fatalf("write output: %v", err)
	}
	pte := &PendingTaskExecution{
		TaskID:        "task-protocol-test",
		JobID:         "job-protocol-test",
		AttemptID:     "attempt-protocol-test",
		AttemptNumber: 1,
		LeaseID:       "lease-protocol-test",
		Revision:      7,
	}
	report := &taskrunner.TaskExecutionReport{Outputs: []executor.ArtifactRef{{
		Type:      "render.output",
		URI:       path,
		Hash:      strings.Repeat("a", 64),
		SizeBytes: int64(len("rendered")),
	}}}

	if err := w.publishArtifactsV1(context.Background(), pte, report); err != nil {
		t.Fatalf("publishArtifactsV1: %v", err)
	}

	var events []map[string]interface{}
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.Contains(line, "ARTIFACT_PROTOCOL ") {
			events = append(events, decodeProtocolEvent(t, line))
		}
	}
	wantEvents := []string{
		"ARTIFACT_DECLARE_SENT",
		"ARTIFACT_UPLOAD_PLAN_RECEIVED",
		"ARTIFACT_TRANSFER_STARTED",
		"ARTIFACT_TRANSFER_COMPLETED",
		"ARTIFACT_COMPLETION_SENT",
		"TASK_COMMIT_ACK_RECEIVED",
	}
	if len(events) != len(wantEvents) {
		t.Fatalf("protocol event count = %d, want %d; output=%s", len(events), len(wantEvents), output.String())
	}
	for i, want := range wantEvents {
		if got := events[i]["event"]; got != want {
			t.Errorf("event[%d] = %#v, want %q", i, got, want)
		}
		for _, key := range []string{"job_id", "task_id", "attempt_id", "lease_id", "elapsed_ms"} {
			if _, ok := events[i][key]; !ok {
				t.Errorf("event[%d] missing %s: %#v", i, key, events[i])
			}
		}
	}
	if got := events[1]["commit_id"]; got != "commit-protocol-test" {
		t.Errorf("plan commit_id = %#v, want commit-protocol-test", got)
	}
	if got := events[4]["upload_id"]; got != "upload-protocol-test" {
		t.Errorf("completion upload_id = %#v, want upload-protocol-test", got)
	}
}

func TestSubmitTaskResult_LogsTerminalBoundary(t *testing.T) {
	var output bytes.Buffer
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-result-log-test", ProtocolVersion: controltransport.ProtocolVersionCurrent},
		logger:    logger.New(logger.InfoLevel, &output),
		transport: &recordingTransport{},
	}
	pte := &PendingTaskExecution{
		TaskID:    "task-result-log-test",
		JobID:     "job-result-log-test",
		AttemptID: "attempt-result-log-test",
		LeaseID:   "lease-result-log-test",
	}

	w.submitTaskResult(context.Background(), pte, pte.TaskID, pte.AttemptID, nil, nil)

	event := decodeProtocolEvent(t, output.String())
	if event["event"] != "TASK_RESULT_SENT" {
		t.Fatalf("event = %#v, want TASK_RESULT_SENT", event["event"])
	}
	for key, want := range map[string]string{
		"job_id":     pte.JobID,
		"task_id":    pte.TaskID,
		"attempt_id": pte.AttemptID,
		"lease_id":   pte.LeaseID,
		"status":     "succeeded",
	} {
		if got := event[key]; got != want {
			t.Errorf("event[%q] = %#v, want %q", key, got, want)
		}
	}
	for _, key := range []string{"commit_id", "artifact_id", "upload_id", "elapsed_ms", "report_hash", "artifact_count"} {
		if _, ok := event[key]; !ok {
			t.Errorf("event missing %s: %#v", key, event)
		}
	}
}

func decodeProtocolEvent(t *testing.T, logOutput string) map[string]interface{} {
	t.Helper()
	const marker = "ARTIFACT_PROTOCOL "
	idx := strings.Index(logOutput, marker)
	if idx < 0 {
		t.Fatalf("log output does not contain %q: %q", marker, logOutput)
	}
	payload := strings.TrimSpace(logOutput[idx+len(marker):])
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		t.Fatalf("unmarshal structured event: %v; payload=%q", err, payload)
	}
	return event
}

type protocolUploadTransport struct{}

func (protocolUploadTransport) ID() string { return "protocol-test.v1" }

func (protocolUploadTransport) Upload(context.Context, publisher.UploadRequest) (*publisher.UploadResult, error) {
	return &publisher.UploadResult{UploadID: "upload-protocol-test", UploadedBytes: int64(len("rendered"))}, nil
}

type protocolRecordingTransport struct {
	worker *Worker
}

func (t *protocolRecordingTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}

func (t *protocolRecordingTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}

func (t *protocolRecordingTransport) Send(_ context.Context, message controltransport.ControlMessage) error {
	switch payload := message.TypedPayload.(type) {
	case *pb.TaskOutputDeclared:
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgArtifactUploadPlan,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.ArtifactUploadPlan{
				TaskId: payload.GetTaskId(), AttemptId: payload.GetAttemptId(),
				CommitId: "commit-protocol-test", CommitToken: "token", LeaseId: payload.GetLeaseId(),
				Targets: []*pb.UploadTarget{{
					DeclarationId: "declaration-protocol-test", ArtifactId: "artifact-protocol-test",
					UploadId: "upload-protocol-test", TransportId: "protocol-test.v1",
				}},
			},
		))
	case *pb.ArtifactUploadCompleted:
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgTaskCommitAck,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.TaskCommitAck{
				TaskId: payload.GetTaskId(), AttemptId: payload.GetAttemptId(), JobId: "job-protocol-test",
				CommitId: payload.GetCommitId(), LeaseId: payload.GetLeaseId(), Revision: 7,
			},
		))
	}
	return nil
}

func (*protocolRecordingTransport) Close() error { return nil }
