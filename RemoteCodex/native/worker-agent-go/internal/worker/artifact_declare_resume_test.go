package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// declareResumeTestTransport answers TaskOutputDeclared with a synthetic
// ArtifactUploadPlan and ArtifactUploadCompleted with a fence-valid
// TaskCommitAck — enough for the declare→upload→commit resume path.
type declareResumeTestTransport struct {
	worker    *Worker
	declared  []*pb.TaskOutputDeclared
	completed []*pb.ArtifactUploadCompleted
}

func (t *declareResumeTestTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}
func (t *declareResumeTestTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}
func (t *declareResumeTestTransport) Send(_ context.Context, msg controltransport.ControlMessage) error {
	switch p := msg.TypedPayload.(type) {
	case *pb.TaskOutputDeclared:
		t.declared = append(t.declared, p)
		targets := make([]*pb.UploadTarget, 0, len(p.GetManifests()))
		for i := range p.GetManifests() {
			targets = append(targets, &pb.UploadTarget{
				DeclarationId: "decl-" + string(rune('0'+i)),
				ArtifactId:    "art-" + string(rune('0'+i)),
				UploadId:      "up-" + string(rune('0'+i)),
				TransportId:   "resume-test.v1",
			})
		}
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgArtifactUploadPlan,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.ArtifactUploadPlan{
				TaskId: p.GetTaskId(), AttemptId: p.GetAttemptId(), CommitId: "commit-declare",
				CommitToken: "token-declare", LeaseId: p.GetLeaseId(), Targets: targets,
			},
		))
	case *pb.ArtifactUploadCompleted:
		t.completed = append(t.completed, p)
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgTaskCommitAck,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.TaskCommitAck{
				TaskId: p.GetTaskId(), AttemptId: p.GetAttemptId(), CommitId: p.GetCommitId(),
				JobId: "job-declare", LeaseId: p.GetLeaseId(), Revision: 1,
			},
		))
	}
	return nil
}
func (t *declareResumeTestTransport) Close() error { return nil }

// declareFailTransport fails every Send so the declare failure/backoff path is
// exercisable without a real transport.
type declareFailTransport struct{}

func (declareFailTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}
func (declareFailTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}
func (declareFailTransport) Send(context.Context, controltransport.ControlMessage) error {
	return controltransport.ErrTransportClosed
}
func (declareFailTransport) Close() error { return nil }

// declareResumeTestWorker builds a worker wired for the declare-resume tests.
func declareResumeTestWorker(t *testing.T, transport controltransport.ControlTransport) (*Worker, *spool.Store) {
	t.Helper()
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	registry := publisher.NewRegistry()
	if err := registry.Register(&resumeUploadTransport{}); err != nil {
		t.Fatalf("register upload transport: %v", err)
	}
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        "worker-declare-test",
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
		},
		logger:            logger.New(logger.InfoLevel, io.Discard),
		transport:         transport,
		publisherRegistry: registry,
		outputSpool:       store,
		publisherPool:     NewPublisherPool(2),
		activeTaskLeases: map[string]*ActiveTaskLease{
			"task-declare": {TaskID: "task-declare", JobID: "job-declare", AttemptID: "attempt-declare", LeaseID: "lease-declare", AttemptNumber: 1, Revision: 1},
		},
	}
	if tp, ok := transport.(*declareResumeTestTransport); ok {
		tp.worker = w
	}
	return w, store
}

// seedDeclareRow drives one row to OUTPUT_READY (declare never sent).
func seedDeclareRow(t *testing.T, store *spool.Store, key, kind, path string) *spool.SpoolEntry {
	t.Helper()
	entry, err := store.Insert(context.Background(), spool.SpoolEntry{
		TaskID: "task-declare", AttemptID: "attempt-declare", WorkerSpoolKey: key,
		OutputKind: kind, LocalPath: path, Status: spool.StatusRendering,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := store.MarkReady(context.Background(), entry.SpoolID, strings.Repeat("a", 64), 100); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	return entry
}

// TestResumeDeclaration_RedeclaresAndStashesPlan is the core declare-resume
// contract: an OUTPUT_READY row (declare never reached the master) is
// re-declared, the returned plan is stashed, and the row moves to
// UPLOAD_PENDING so the upload pass can re-drive the bytes.
func TestResumeDeclaration_RedeclaresAndStashesPlan(t *testing.T) {
	transport := &declareResumeTestTransport{}
	w, store := declareResumeTestWorker(t, transport)

	dir := t.TempDir()
	video := filepath.Join(dir, "render.mp4")
	sidecar := filepath.Join(dir, "sidecar.json")
	for _, p := range []string{video, sidecar} {
		if err := os.WriteFile(p, []byte("x"), 0o640); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	videoRow := seedDeclareRow(t, store, "task-declare:output:0", "final_video", video)
	sidecarRow := seedDeclareRow(t, store, "task-declare:output:1", "engine_progress_sidecar", sidecar)

	w.resumeDeclaration(context.Background(), []spool.SpoolEntry{*videoRow, *sidecarRow})

	if len(transport.declared) != 1 {
		t.Fatalf("TaskOutputDeclared count = %d; want 1", len(transport.declared))
	}
	decl := transport.declared[0]
	if decl.GetJobId() != "job-declare" || decl.GetLeaseId() != "lease-declare" || decl.GetAttemptNumber() != 1 || decl.GetRevision() != 1 {
		t.Fatalf("declared identity = %+v; want job-declare/lease-declare/1/1", decl)
	}
	if len(decl.GetManifests()) != 2 {
		t.Fatalf("manifest count = %d; want 2", len(decl.GetManifests()))
	}
	m0 := decl.GetManifests()[0]
	if m0.GetOutputKind() != "final_video" || m0.GetMimeType() != "video/mp4" || m0.GetLogicalName() != "render.mp4" || m0.GetWorkerSpoolKey() != "task-declare:output:0" {
		t.Fatalf("manifest[0] = %+v; want final_video/video/mp4/render.mp4", m0)
	}
	m1 := decl.GetManifests()[1]
	if m1.GetOutputKind() != "engine_progress_sidecar" || m1.GetMimeType() != "application/json" || m1.GetLogicalName() != "sidecar.json" {
		t.Fatalf("manifest[1] = %+v; want engine_progress_sidecar/application/json/sidecar.json", m1)
	}

	for _, row := range []*spool.SpoolEntry{videoRow, sidecarRow} {
		got, err := store.Get(context.Background(), row.SpoolID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Status != spool.StatusUploadPending {
			t.Fatalf("status = %q; want UPLOAD_PENDING after declare resume", got.Status)
		}
		if got.UploadTargetJSON == "" || got.CommitID != "commit-declare" || got.CommitToken != "token-declare" {
			t.Fatalf("stashed plan incomplete: target=%q commit=%q token=%q", got.UploadTargetJSON, got.CommitID, got.CommitToken)
		}
	}
}

// TestResumeDueDeclarations_FullLoopCommits drives the whole resume tick:
// declare pass re-declares and the upload pass re-drives the bytes to
// COMMITTED (never a lost row or a re-render).
func TestResumeDueDeclarations_FullLoopCommits(t *testing.T) {
	transport := &declareResumeTestTransport{}
	w, store := declareResumeTestWorker(t, transport)

	dir := t.TempDir()
	video := filepath.Join(dir, "render.mp4")
	if err := os.WriteFile(video, []byte("full-loop bytes"), 0o640); err != nil {
		t.Fatalf("write video: %v", err)
	}
	row := seedDeclareRow(t, store, "task-declare:output:0", "final_video", video)

	if err := w.resumeDueDeclarations(context.Background()); err != nil {
		t.Fatalf("resumeDueDeclarations: %v", err)
	}
	if err := w.resumeDueArtifactUploads(context.Background()); err != nil {
		t.Fatalf("resumeDueArtifactUploads: %v", err)
	}

	got, err := store.Get(context.Background(), row.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != spool.StatusCommitted {
		t.Fatalf("status = %q; want COMMITTED after declare→upload resume", got.Status)
	}
	if len(transport.completed) != 1 {
		t.Fatalf("ArtifactUploadCompleted count = %d; want 1", len(transport.completed))
	}
}

// TestResumeDeclaration_DeclareFailureSchedulesBackoff pins the failure path:
// a failed re-declare leaves the row OUTPUT_READY with a recorded failure +
// future backoff instant (resumable, never terminal).
func TestResumeDeclaration_DeclareFailureSchedulesBackoff(t *testing.T) {
	w, store := declareResumeTestWorker(t, declareFailTransport{})

	dir := t.TempDir()
	video := filepath.Join(dir, "render.mp4")
	if err := os.WriteFile(video, []byte("x"), 0o640); err != nil {
		t.Fatalf("write video: %v", err)
	}
	row := seedDeclareRow(t, store, "task-declare:output:0", "final_video", video)

	w.resumeDeclaration(context.Background(), []spool.SpoolEntry{*row})

	got, err := store.Get(context.Background(), row.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != spool.StatusOutputReady {
		t.Fatalf("status = %q; want OUTPUT_READY (declare failure is resumable)", got.Status)
	}
	if got.UploadAttemptCount != 1 {
		t.Fatalf("upload_attempt_count = %d; want 1", got.UploadAttemptCount)
	}
	if got.NextUploadAttemptAt.IsZero() || !got.NextUploadAttemptAt.After(time.Now()) {
		t.Fatalf("next_upload_attempt_at = %v; want a future backoff instant", got.NextUploadAttemptAt)
	}
	if !strings.Contains(got.LastError, "transport is closed") {
		t.Fatalf("last_error = %q; want the declare send failure", got.LastError)
	}
}

// TestResumeDueDeclarations_SkipsNotDue pins the due-gate: a row whose backoff
// instant is still in the future is not re-declared.
func TestResumeDueDeclarations_SkipsNotDue(t *testing.T) {
	transport := &declareResumeTestTransport{}
	w, store := declareResumeTestWorker(t, transport)

	dir := t.TempDir()
	video := filepath.Join(dir, "render.mp4")
	if err := os.WriteFile(video, []byte("x"), 0o640); err != nil {
		t.Fatalf("write video: %v", err)
	}
	row := seedDeclareRow(t, store, "task-declare:output:0", "final_video", video)
	if err := store.RecordUploadFailure(context.Background(), row.SpoolID, "once", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("RecordUploadFailure: %v", err)
	}

	if err := w.resumeDueDeclarations(context.Background()); err != nil {
		t.Fatalf("resumeDueDeclarations: %v", err)
	}
	if len(transport.declared) != 0 {
		t.Fatalf("declared count = %d; want 0 (row not due yet)", len(transport.declared))
	}
}
