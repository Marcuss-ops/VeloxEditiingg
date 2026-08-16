package worker

import (
	"context"
	"encoding/json"
	"errors"
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

// resumeUploadTransport is the publisher-transport double for the resume
// tests: it reads LocalPath (like production) and records where it read from.
// fail makes every Upload return an error (transient failure path).
type resumeUploadTransport struct {
	path  string
	bytes int64
	calls int
	fail  bool
}

func (t *resumeUploadTransport) ID() string { return "resume-test.v1" }

func (t *resumeUploadTransport) Upload(_ context.Context, req publisher.UploadRequest) (*publisher.UploadResult, error) {
	t.calls++
	if t.fail {
		return nil, errors.New("resume-test: simulated upload failure")
	}
	data, err := os.ReadFile(req.LocalPath)
	if err != nil {
		return nil, err
	}
	t.path = req.LocalPath
	t.bytes = int64(len(data))
	return &publisher.UploadResult{UploadID: req.Target.UploadID, UploadedBytes: int64(len(data))}, nil
}

// resumeTestTransport is the control-plane double that answers
// ArtifactUploadCompleted with a fence-valid TaskCommitAck dispatched back
// into the worker's ack registry.
type resumeTestTransport struct {
	worker    *Worker
	completed []*pb.ArtifactUploadCompleted
}

func (t *resumeTestTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}
func (t *resumeTestTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}
func (t *resumeTestTransport) Send(_ context.Context, msg controltransport.ControlMessage) error {
	if p, ok := msg.TypedPayload.(*pb.ArtifactUploadCompleted); ok {
		t.completed = append(t.completed, p)
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgTaskCommitAck,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.TaskCommitAck{
				TaskId: p.GetTaskId(), AttemptId: p.GetAttemptId(), CommitId: p.GetCommitId(),
				JobId: "job-resume", LeaseId: p.GetLeaseId(), Revision: 1,
			},
		))
	}
	return nil
}
func (t *resumeTestTransport) Close() error { return nil }

// resumeTestWorker builds the worker composition for resume tests.
func resumeTestWorker(t *testing.T, uploader *resumeUploadTransport, leaseID string) (*Worker, *spool.Store, *resumeTestTransport) {
	t.Helper()
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	registry := publisher.NewRegistry()
	if err := registry.Register(uploader); err != nil {
		t.Fatalf("register upload transport: %v", err)
	}
	transport := &resumeTestTransport{}
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        "worker-resume-test",
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
		},
		logger:            logger.New(logger.InfoLevel, io.Discard),
		transport:         transport,
		publisherRegistry: registry,
		outputSpool:       store,
		activeTaskLeases: map[string]*ActiveTaskLease{
			"task-resume": {TaskID: "task-resume", JobID: "job-resume", AttemptID: "attempt-resume", LeaseID: leaseID, AttemptNumber: 1, Revision: 1},
		},
	}
	transport.worker = w
	return w, store, transport
}

// seedResumeRow drives one row into UPLOADING with a persisted upload target,
// then spills it (repoint local_path to a durable NVMe path) so the resume
// upload must read from the repointed path.
func seedResumeRow(t *testing.T, store *spool.Store, durablePath string) *spool.SpoolEntry {
	t.Helper()
	ctx := context.Background()
	entry, err := store.Insert(ctx, spool.SpoolEntry{
		TaskID: "task-resume", AttemptID: "attempt-resume", WorkerSpoolKey: "task-resume:output:0",
		LocalPath:   "/tmp/volatile/render.mp4",
		Status:      spool.StatusRendering,
		StorageTier: spool.StorageTierTmpfsVolatile,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := store.MarkReady(ctx, entry.SpoolID, strings.Repeat("a", 64), 100); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	targetJSON, err := json.Marshal(publisher.UploadTarget{
		DeclarationID: "decl-resume", ArtifactID: "artifact-resume", UploadID: "up-resume",
		TransportID: "resume-test.v1", UploadURL: "http://master.test/upload",
	})
	if err != nil {
		t.Fatalf("marshal target: %v", err)
	}
	if err := store.StashUploadPlan(ctx, entry.SpoolID, "commit-resume", "up-resume", string(targetJSON), "commit-token"); err != nil {
		t.Fatalf("StashUploadPlan: %v", err)
	}
	if err := store.MarkUploadPending(ctx, entry.SpoolID, "up-resume"); err != nil {
		t.Fatalf("MarkUploadPending: %v", err)
	}
	if err := store.MarkUploading(ctx, entry.SpoolID, 0); err != nil {
		t.Fatalf("MarkUploading: %v", err)
	}
	if err := store.MarkSpilled(ctx, entry.SpoolID, durablePath); err != nil {
		t.Fatalf("MarkSpilled: %v", err)
	}
	got, err := store.Get(ctx, entry.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return got
}

func TestUploadResumeBackoff(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, 2 * time.Second},
		{1, 4 * time.Second},
		{2, 8 * time.Second},
		{3, 16 * time.Second},
		{4, 32 * time.Second},
		{5, 64 * time.Second},
		{6, 2 * time.Minute}, // capped at uploadResumeMax
		{100, 2 * time.Minute},
		{-3, 2 * time.Second},
	}
	for _, tc := range cases {
		if got := uploadResumeBackoff(tc.failures); got != tc.want {
			t.Errorf("uploadResumeBackoff(%d) = %v; want %v", tc.failures, got, tc.want)
		}
	}
}

// TestResumeArtifactUpload_ReuploadsFromRepointedPathAndCompletesCommit is the
// core contract: the resume upload reads the artifact from the spool's
// CURRENT (repointed) local_path — the NVMe path after the spill — not the
// original tmpfs path, then drives the row to COMMITTED via the commit ack.
func TestResumeArtifactUpload_ReuploadsFromRepointedPathAndCompletesCommit(t *testing.T) {
	uploader := &resumeUploadTransport{}
	w, store, transport := resumeTestWorker(t, uploader, "lease-resume")

	durablePath := filepath.Join(t.TempDir(), "spilled.mp4")
	if err := os.WriteFile(durablePath, []byte("spilled nvme bytes"), 0o640); err != nil {
		t.Fatalf("write durable artifact: %v", err)
	}
	entry := seedResumeRow(t, store, durablePath)

	w.resumeArtifactUpload(context.Background(), *entry)

	if uploader.calls != 1 {
		t.Fatalf("uploader.calls = %d; want 1", uploader.calls)
	}
	if uploader.path != durablePath {
		t.Fatalf("uploader read path=%q; want repointed NVMe path=%q", uploader.path, durablePath)
	}
	if len(transport.completed) != 1 || transport.completed[0].GetCommitId() != "commit-resume" {
		t.Fatalf("ArtifactUploadCompleted = %+v; want one completion with commit-resume", transport.completed)
	}

	got, err := store.Get(context.Background(), entry.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != spool.StatusCommitted {
		t.Fatalf("status = %q; want COMMITTED (resume drove the full upload→commit)", got.Status)
	}
}

// TestResumeArtifactUpload_UploadedRowOnlyResendsCompletion pins the lost-ack
// path: once bytes reached the transport and the spool is UPLOADED, resume
// must not upload the file a second time. It only replays the completion
// message and advances the row to COMMITTED after the fenced ack.
func TestResumeArtifactUpload_UploadedRowOnlyResendsCompletion(t *testing.T) {
	uploader := &resumeUploadTransport{}
	w, store, transport := resumeTestWorker(t, uploader, "lease-resume")

	durablePath := filepath.Join(t.TempDir(), "spilled.mp4")
	if err := os.WriteFile(durablePath, []byte("already-uploaded"), 0o640); err != nil {
		t.Fatalf("write durable artifact: %v", err)
	}
	entry := seedResumeRow(t, store, durablePath)
	if err := store.MarkUploaded(context.Background(), entry.SpoolID); err != nil {
		t.Fatalf("MarkUploaded: %v", err)
	}
	entry, err := store.Get(context.Background(), entry.SpoolID)
	if err != nil {
		t.Fatalf("Get uploaded row: %v", err)
	}

	w.resumeArtifactUpload(context.Background(), *entry)

	if uploader.calls != 0 {
		t.Fatalf("uploader.calls = %d; an UPLOADED row must not re-upload", uploader.calls)
	}
	if len(transport.completed) != 1 {
		t.Fatalf("completion messages = %d; want 1", len(transport.completed))
	}
	got, err := store.Get(context.Background(), entry.SpoolID)
	if err != nil {
		t.Fatalf("Get committed row: %v", err)
	}
	if got.Status != spool.StatusCommitted {
		t.Fatalf("status = %q; want COMMITTED", got.Status)
	}
}

// TestResumeArtifactUpload_FailureSchedulesBoundedBackoff: a transient upload
// failure keeps the row resumable (UPLOADING) and bumps the retry ledger.
func TestResumeArtifactUpload_FailureSchedulesBoundedBackoff(t *testing.T) {
	uploader := &resumeUploadTransport{fail: true}
	w, store, _ := resumeTestWorker(t, uploader, "lease-resume")

	durablePath := filepath.Join(t.TempDir(), "spilled.mp4")
	if err := os.WriteFile(durablePath, []byte("x"), 0o640); err != nil {
		t.Fatalf("write durable artifact: %v", err)
	}
	entry := seedResumeRow(t, store, durablePath)

	w.resumeArtifactUpload(context.Background(), *entry)

	got, err := store.Get(context.Background(), entry.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != spool.StatusUploading {
		t.Fatalf("status = %q; want UPLOADING (still resumable)", got.Status)
	}
	if got.UploadAttemptCount != 1 {
		t.Fatalf("upload_attempt_count = %d; want 1", got.UploadAttemptCount)
	}
	if got.NextUploadAttemptAt.IsZero() || !got.NextUploadAttemptAt.After(time.Now()) {
		t.Fatalf("next_upload_attempt_at = %v; want a future backoff instant", got.NextUploadAttemptAt)
	}
	if !strings.Contains(got.LastError, "simulated upload failure") {
		t.Fatalf("last_error = %q; want the simulated failure", got.LastError)
	}
}

// TestResumeArtifactUpload_ExhaustedBudgetRejects: once the bounded retry
// budget is exhausted the row becomes a permanent REJECTED (the master then
// re-schedules the render) instead of spinning forever.
func TestResumeArtifactUpload_ExhaustedBudgetRejects(t *testing.T) {
	uploader := &resumeUploadTransport{fail: true}
	w, store, _ := resumeTestWorker(t, uploader, "lease-resume")

	durablePath := filepath.Join(t.TempDir(), "spilled.mp4")
	if err := os.WriteFile(durablePath, []byte("x"), 0o640); err != nil {
		t.Fatalf("write durable artifact: %v", err)
	}
	entry := seedResumeRow(t, store, durablePath)

	ctx := context.Background()
	for i := 0; i < uploadResumeMaxAttempts; i++ {
		if err := store.RecordUploadFailure(ctx, entry.SpoolID, "boom", time.Now()); err != nil {
			t.Fatalf("RecordUploadFailure %d: %v", i, err)
		}
	}
	// Re-fetch the row so the resume driver sees the fresh retry count (the
	// production loop always lists candidates fresh from the spool).
	fresh, err := store.Get(ctx, entry.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	w.resumeArtifactUpload(ctx, *fresh)

	got, err := store.Get(ctx, entry.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != spool.StatusRejected {
		t.Fatalf("status = %q; want REJECTED after the bounded budget", got.Status)
	}
}

// TestResumeDueArtifactUploads_SkipsNotDue: the due-gate defers rows whose
// backoff instant is still in the future.
func TestResumeDueArtifactUploads_SkipsNotDue(t *testing.T) {
	uploader := &resumeUploadTransport{fail: true}
	w, store, _ := resumeTestWorker(t, uploader, "lease-resume")

	durablePath := filepath.Join(t.TempDir(), "spilled.mp4")
	if err := os.WriteFile(durablePath, []byte("x"), 0o640); err != nil {
		t.Fatalf("write durable artifact: %v", err)
	}
	entry := seedResumeRow(t, store, durablePath)
	// Schedule the next attempt 1 minute out.
	if err := store.RecordUploadFailure(context.Background(), entry.SpoolID, "once", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("RecordUploadFailure: %v", err)
	}

	if err := w.resumeDueArtifactUploads(context.Background()); err != nil {
		t.Fatalf("resumeDueArtifactUploads: %v", err)
	}
	if uploader.calls != 0 {
		t.Fatalf("uploader.calls = %d; want 0 (row not due yet)", uploader.calls)
	}
}
