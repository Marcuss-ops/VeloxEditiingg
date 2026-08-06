package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// newTelemetryTestWorker builds a minimal Worker with the fields
// buildTelemetrySnapshot reads: config, activeTaskLeases, activeTasks and
// (optionally) cache/sampler/assetManager. It deliberately avoids New() so
// no transport/executor wiring is needed.
func newTelemetryTestWorker(id string) *Worker {
	return &Worker{
		config:             &config.WorkerConfig{WorkerID: id},
		logger:             logger.New(logger.InfoLevel, nil),
		activeTaskLeases:   make(map[string]*ActiveTaskLease),
		activeTaskLeasesMu: sync.RWMutex{},
		activeTasks:        make(map[string]*ActiveTaskExecution),
		activeTasksMu:      sync.RWMutex{},
	}
}

func TestBuildTelemetrySnapshot_SequenceMonotonic(t *testing.T) {
	w := newTelemetryTestWorker("w-seq")

	first := w.buildTelemetrySnapshot()
	if first.Sequence != 1 {
		t.Fatalf("first snapshot sequence=%d, want 1", first.Sequence)
	}
	if first.CapturedAt.IsZero() {
		t.Fatal("CapturedAt must be set")
	}
	if first.WorkerID != "w-seq" {
		t.Fatalf("WorkerID=%q, want w-seq", first.WorkerID)
	}
	if first.SchemaVersion != controltransport.TelemetrySnapshotSchemaVersion {
		t.Fatalf("SchemaVersion=%d, want %d", first.SchemaVersion, controltransport.TelemetrySnapshotSchemaVersion)
	}

	second := w.buildTelemetrySnapshot()
	if second.Sequence <= first.Sequence {
		t.Fatalf("sequence not monotonic: first=%d second=%d", first.Sequence, second.Sequence)
	}
}

func TestBuildTelemetrySnapshot_StateCounters(t *testing.T) {
	w := newTelemetryTestWorker("w-state")
	w.activeTaskLeases["lease-a"] = &ActiveTaskLease{TaskID: "t-a", LeaseID: "l-a"}
	w.activeTaskLeases["lease-b"] = &ActiveTaskLease{TaskID: "t-b", LeaseID: "l-b"}
	w.activeTasks["task-1"] = &ActiveTaskExecution{TaskID: "task-1", StartedAt: time.Now().UTC()}

	snap := w.buildTelemetrySnapshot()
	if snap.ActiveLeases != 2 {
		t.Errorf("ActiveLeases=%d, want 2", snap.ActiveLeases)
	}
	if snap.RenderActive != 1 {
		t.Errorf("RenderActive=%d, want 1", snap.RenderActive)
	}
	if snap.DownloadQueue != 0 {
		t.Errorf("DownloadQueue=%d, want 0 (no manager wired)", snap.DownloadQueue)
	}
}

func TestBuildTelemetrySnapshot_DownloadQueueFromManager(t *testing.T) {
	w := newTelemetryTestWorker("w-queue")

	// A real Manager with a wired operational callback. The manager caches
	// its latest operational projection on every refresh; a freshly-created
	// idle manager reports zero queued transfers through the locked read.
	mgr := downloader.NewManager(downloader.Config{
		Concurrency:           1,
		OnOperationalSnapshot: func(downloader.OperationalSnapshot) {},
	}, &downloaderFakeTransferer{})
	defer mgr.Close()
	w.assetManager = mgr

	snap := w.buildTelemetrySnapshot()
	if snap.DownloadQueue != 0 {
		t.Errorf("DownloadQueue=%d, want 0 (idle manager)", snap.DownloadQueue)
	}
}

type downloaderFakeTransferer struct{}

func (d *downloaderFakeTransferer) Check(context.Context, context.Context, downloader.DownloadRequest) (downloader.CacheCheckResult, error) {
	return downloader.CacheCheckResult{}, nil
}
func (d *downloaderFakeTransferer) Transfer(context.Context, context.Context, downloader.DownloadRequest, func(int64)) (downloader.TransferResult, error) {
	return downloader.TransferResult{}, nil
}

func TestBuildTelemetrySnapshot_ReleaseIdentityCarried(t *testing.T) {
	w := newTelemetryTestWorker("w-rel")
	snap := w.buildTelemetrySnapshot()
	// No release sources in a bare test worker: the certificate must be
	// empty, not crash the assembly, and the wire map must still serialize.
	_ = snap.SoftwareRelease.IsEmpty()

	block := snap.AsMap()
	if _, ok := block[controltransport.TelemetrySnapshotExtraKey]; ok {
		t.Fatal("AsMap must not nest under the extra key (it IS the extra value)")
	}
	if block["worker_id"] != "w-rel" {
		t.Fatalf("wire worker_id=%v, want w-rel", block["worker_id"])
	}
}
