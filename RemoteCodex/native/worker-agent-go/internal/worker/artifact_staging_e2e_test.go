package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"

	"google.golang.org/protobuf/proto"
)

// stagingE2ERenderBytes is the canonical render payload for both tests.
const stagingE2ERenderBytes = "end-to-end rendered video bytes"

// nvmeFallbackTestResolver builds an ARTIFACT_STAGING resolver whose tmpfs
// reserve headroom is larger than any real filesystem, so every positive
// reservation estimate fails with no_space and the placement falls back to
// durable NVMe. This reproduces "insufficient RAM" deterministically without
// a mounted /dev/shm.
func nvmeFallbackTestResolver(t *testing.T, stagingDir, artifactDir string) *storage.Resolver {
	t.Helper()
	r, err := storage.New(storage.Config{
		CacheDir:            filepath.Join(t.TempDir(), "cache"),
		TempDir:             filepath.Join(t.TempDir(), "temp"),
		ArtifactDir:         artifactDir,
		TmpfsThresholdBytes: 64 * 1024 * 1024,
		ArtifactStaging: storage.ArtifactStagingConfig{
			Enabled:      true,
			Dir:          stagingDir,
			MaxPercent:   99,
			ReserveBytes: 1 << 60,
		},
	})
	if err != nil {
		t.Fatalf("build fallback resolver: %v", err)
	}
	if err := r.EnsureDirs(); err != nil {
		t.Fatalf("ensure fallback dirs: %v", err)
	}
	return r
}

// stagingE2ETransport is the master-side artifact-protocol double: it answers
// TaskOutputDeclared with an ArtifactUploadPlan and ArtifactUploadCompleted
// with a TaskCommitAck, echoing the declared identity so the publish fence
// (task/attempt/job/lease/revision) validates.
type stagingE2ETransport struct {
	worker      *Worker
	transportID string
	commitID    string
	declared    *pb.TaskOutputDeclared
}

func (t *stagingE2ETransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}
func (t *stagingE2ETransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}
func (t *stagingE2ETransport) Send(_ context.Context, msg controltransport.ControlMessage) error {
	switch payload := msg.TypedPayload.(type) {
	case *pb.TaskOutputDeclared:
		t.declared = payload
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgArtifactUploadPlan,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.ArtifactUploadPlan{
				TaskId: payload.GetTaskId(), AttemptId: payload.GetAttemptId(),
				CommitId: t.commitID, CommitToken: "token", LeaseId: payload.GetLeaseId(),
				Targets: []*pb.UploadTarget{{
					DeclarationId: "declaration-staging-e2e",
					ArtifactId:    "artifact-staging-e2e",
					UploadId:      "upload-staging-e2e",
					TransportId:   t.transportID,
				}},
			},
		))
	case *pb.ArtifactUploadCompleted:
		jobID, leaseID := "job-staging-e2e", "lease-staging-e2e"
		revision := int32(1)
		if t.declared != nil {
			jobID = t.declared.GetJobId()
			leaseID = t.declared.GetLeaseId()
			revision = t.declared.GetRevision()
		}
		t.worker.dispatchTypedPlanOrAck(controltransport.NewTypedMessage(
			controltransport.MsgTaskCommitAck,
			t.worker.config.WorkerID,
			controltransport.ProtocolVersionCurrent,
			&pb.TaskCommitAck{
				TaskId: payload.GetTaskId(), AttemptId: payload.GetAttemptId(),
				JobId: jobID, CommitId: payload.GetCommitId(), LeaseId: leaseID, Revision: revision,
			},
		))
	}
	return nil
}
func (t *stagingE2ETransport) Close() error { return nil }

// recordingUploadTransport is the publisher-transport double that reads the
// artifact from LocalPath (exactly like the production uploader: an
// os.Open/read of the path — RAM when it points into tmpfs) and records where
// it read from.
type recordingUploadTransport struct {
	path  string
	bytes int64
}

func (t *recordingUploadTransport) ID() string { return "staging-e2e.v1" }

func (t *recordingUploadTransport) Capabilities() publisher.CapabilitySet { return nil }

func (t *recordingUploadTransport) Upload(_ context.Context, req publisher.UploadRequest) (*publisher.UploadResult, error) {
	data, err := os.ReadFile(req.LocalPath)
	if err != nil {
		return nil, err
	}
	t.path = req.LocalPath
	t.bytes = int64(len(data))
	return &publisher.UploadResult{UploadID: req.Target.UploadID, UploadedBytes: int64(len(data))}, nil
}

// stagingE2EWorker builds the full worker composition for the staging e2e
// tests: resolver + spool + publisher registry + protocol transport.
func stagingE2EWorker(t *testing.T, resolver *storage.Resolver, artifactDir string) (*Worker, *spool.Store, *stagingE2ETransport, *recordingUploadTransport) {
	t.Helper()
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	uploader := &recordingUploadTransport{}
	registry := publisher.NewRegistry()
	if err := registry.Register(uploader); err != nil {
		t.Fatalf("register upload transport: %v", err)
	}
	transport := &stagingE2ETransport{transportID: "staging-e2e.v1", commitID: "commit-staging-e2e"}
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        "worker-staging-e2e",
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			OutputDir:       artifactDir,
		},
		logger:            logger.New(logger.InfoLevel, io.Discard),
		transport:         transport,
		publisherRegistry: registry,
		outputSpool:       store,
		storageResolver:   resolver,
		artifactLocks:     NewArtifactLockRegistry(),
	}
	transport.worker = w
	return w, store, transport, uploader
}

// stagingE2EReportAndPTE builds the publish inputs for one render.output whose
// bytes live at uri.
func stagingE2EReportAndPTE(taskID, jobID, attemptID, uri string) (*PendingTaskExecution, *taskrunner.TaskExecutionReport) {
	pte := &PendingTaskExecution{
		TaskID: taskID, JobID: jobID, AttemptID: attemptID,
		AttemptNumber: 1, LeaseID: "lease-staging-e2e", Revision: 7,
	}
	report := &taskrunner.TaskExecutionReport{Outputs: []executor.ArtifactRef{{
		Type:      "render.output",
		URI:       uri,
		Hash:      strings.Repeat("a", 64),
		SizeBytes: int64(len(stagingE2ERenderBytes)),
	}}}
	return pte, report
}

// TestArtifactStagingE2E_TmpfsUploadAndCleanupAfterCommit is the happy-path
// acceptance test: a render output staged on tmpfs is uploaded (the uploader
// reads LocalPath straight from RAM), then unlinked + reservation-released
// immediately after TaskCommitAck while the spool row remains auditable until
// the terminal TaskResultAck.
func TestArtifactStagingE2E_TmpfsUploadAndCleanupAfterCommit(t *testing.T) {
	stagingDir := t.TempDir()
	artifactDir := t.TempDir()
	resolver := spillTestResolver(t, stagingDir, artifactDir)

	placement, err := resolver.Place(storage.ArtifactStaging, "job-tmpfs.mp4", 1024)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if placement.Backing != storage.BackingTmpfs {
		t.Fatalf("backing = %s, want tmpfs", placement.Backing)
	}
	if err := os.WriteFile(placement.Path, []byte(stagingE2ERenderBytes), 0o640); err != nil {
		t.Fatalf("write tmpfs artifact: %v", err)
	}

	w, store, _, uploader := stagingE2EWorker(t, resolver, artifactDir)
	pte, report := stagingE2EReportAndPTE("task-tmpfs-e2e", "job-tmpfs-e2e", "attempt-tmpfs-e2e", placement.Path)

	if err := w.publishArtifactsV1(context.Background(), pte, report); err != nil {
		t.Fatalf("publishArtifactsV1: %v", err)
	}

	// The uploader read the artifact from the tmpfs path (RAM), unchanged
	// interface: os.ReadFile(LocalPath).
	if uploader.path != placement.Path || uploader.bytes != int64(len(stagingE2ERenderBytes)) {
		t.Fatalf("uploader read path=%q bytes=%d; want tmpfs path=%q bytes=%d",
			uploader.path, uploader.bytes, placement.Path, len(stagingE2ERenderBytes))
	}

	// The artifact-level commit ack authorizes local cleanup. The spool row is
	// still retained for the terminal TaskResultAck/audit transition.
	if _, err := os.Stat(placement.Path); !os.IsNotExist(err) {
		t.Fatalf("tmpfs artifact still exists after commit ACK: %v", err)
	}
	if got := resolver.ReservedTmpfsBytes(); got != 0 {
		t.Fatalf("ReservedTmpfsBytes = %d after commit ACK; want 0", got)
	}

	// Deliver the terminal TaskResultAck: cleanup marks the retained spool row
	// CLEANED; file removal/release is already idempotently complete.
	payload, err := proto.Marshal(&pb.TaskResult{
		TaskId: pte.TaskID, JobId: pte.JobID, AttemptId: pte.AttemptID, ReportHash: "hash-tmpfs-e2e",
	})
	if err != nil {
		t.Fatalf("marshal task result: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), pte.TaskID, pte.AttemptID, "hash-tmpfs-e2e", payload); err != nil {
		t.Fatalf("upsert task result: %v", err)
	}
	wireTestReporter(w, store).HandleAck(&pb.TaskResultAck{
		TaskId: pte.TaskID, JobId: pte.JobID, AttemptId: pte.AttemptID,
	})

	if _, err := os.Stat(placement.Path); !os.IsNotExist(err) {
		t.Fatalf("tmpfs artifact not removed after terminal ACK (stat err=%v)", err)
	}
	if got := resolver.ReservedTmpfsBytes(); got != 0 {
		t.Fatalf("ReservedTmpfsBytes = %d after terminal ACK; want 0", got)
	}
	entry, err := store.ListByAttempt(context.Background(), pte.TaskID, pte.AttemptID)
	if err != nil || len(entry) != 1 || entry[0].Status != spool.StatusCleaned {
		t.Fatalf("spool after terminal ACK = %+v err=%v; want one CLEANED row", entry, err)
	}
}

// TestArtifactStagingE2E_InsufficientRAMFallsBackNvmeAndUploads is the
// fallback acceptance test: when the tmpfs cannot satisfy the reservation the
// render lands on durable NVMe (FallbackReason no_space) and the full
// declare→upload→commit→cleanup flow still completes without failing the job.
func TestArtifactStagingE2E_InsufficientRAMFallsBackNvmeAndUploads(t *testing.T) {
	stagingDir := t.TempDir()
	artifactDir := t.TempDir()
	resolver := nvmeFallbackTestResolver(t, stagingDir, artifactDir)

	placement, err := resolver.Place(storage.ArtifactStaging, "job-nvme.mp4", 1<<30)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if placement.Backing != storage.BackingNvme {
		t.Fatalf("backing = %s, want nvme fallback", placement.Backing)
	}
	if placement.FallbackReason != storage.FallbackNoSpace {
		t.Fatalf("FallbackReason = %q, want %q", placement.FallbackReason, storage.FallbackNoSpace)
	}
	if placement.ReservedBytes != 0 {
		t.Fatalf("nvme fallback must not reserve, got %d", placement.ReservedBytes)
	}
	if err := os.WriteFile(placement.Path, []byte(stagingE2ERenderBytes), 0o640); err != nil {
		t.Fatalf("write nvme artifact: %v", err)
	}

	w, store, _, uploader := stagingE2EWorker(t, resolver, artifactDir)
	pte, report := stagingE2EReportAndPTE("task-nvme-e2e", "job-nvme-e2e", "attempt-nvme-e2e", placement.Path)

	if err := w.publishArtifactsV1(context.Background(), pte, report); err != nil {
		t.Fatalf("publishArtifactsV1: %v", err)
	}
	if uploader.path != placement.Path {
		t.Fatalf("uploader read path=%q; want nvme path=%q", uploader.path, placement.Path)
	}

	// Cleanup after the terminal ACK: the durable NVMe artifact is unlinked
	// and no RAM reservation was ever held.
	payload, err := proto.Marshal(&pb.TaskResult{
		TaskId: pte.TaskID, JobId: pte.JobID, AttemptId: pte.AttemptID, ReportHash: "hash-nvme-e2e",
	})
	if err != nil {
		t.Fatalf("marshal task result: %v", err)
	}
	if err := store.UpsertTaskResult(context.Background(), pte.TaskID, pte.AttemptID, "hash-nvme-e2e", payload); err != nil {
		t.Fatalf("upsert task result: %v", err)
	}
	wireTestReporter(w, store).HandleAck(&pb.TaskResultAck{
		TaskId: pte.TaskID, JobId: pte.JobID, AttemptId: pte.AttemptID,
	})

	if _, err := os.Stat(placement.Path); !os.IsNotExist(err) {
		t.Fatalf("nvme artifact not removed after terminal ACK (stat err=%v)", err)
	}
	if got := resolver.ReservedTmpfsBytes(); got != 0 {
		t.Fatalf("ReservedTmpfsBytes = %d after nvme fallback cleanup; want 0", got)
	}
}
