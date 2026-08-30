package worker

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/telemetry"
)

// asset_progress.go owns the asset-download progress surface: the durable
// checkpoint projection, the FIFO sender that streams it to the master and
// the rate-limited body wrapper plus the small time/range parsing helpers it
// needs. It does not touch the cache or the byte pipeline.

// emitAssetProgressCheckpoint records one durable asset-download checkpoint
// event (origin: worker, scope: task). The manager throttles these to ~2s or
// 16MB per transfer; terminal transitions always checkpoint. The first
// waiter's telemetry context supplies the recorder — value-reads only, per
// the Transferer context contract.
func emitAssetProgressCheckpoint(ctx context.Context, snap downloader.DownloadSnapshot) {
	// The checkpoint hook is deliberately non-blocking with respect to the
	// downloader. A single FIFO sender owns all progress sends, preserving
	// checkpoint order while transport reconnects are handled by taking a
	// synchronized snapshot of the current session at send time.
	if w, ok := workerFromProgressContext(ctx); ok && w != nil {
		jobs := append([]string(nil), snap.JobIDs...)
		sort.Strings(jobs)
		msg := &pb.AssetDownloadProgress{
			WorkerId: w.config.WorkerID, TransferId: snap.TransferID,
			AssetKey: string(snap.AssetKey), AssetId: snap.AssetID, Role: string(snap.Role),
			State: string(snap.State), BytesDownloaded: snap.BytesDownloaded,
			BytesTotal: snap.BytesTotal, BytesPerSecond: snap.ThroughputBytesPerSecond,
			EtaSeconds: snap.ETASeconds, Attempt: int32(snap.Attempt),
			SharedWaiters: int32(snap.SharedWaiters), CacheHit: snap.CacheHit,
			QueuedAtUnixMs: unixMillis(snap.QueuedAt), StartedAtUnixMs: unixMillis(snap.StartedAt),
			UpdatedAtUnixMs: unixMillis(snap.UpdatedAt), CompletedAtUnixMs: unixMillis(snap.CompletedAt),
			JobIds: jobs, TaskId: snap.TaskID, SceneIds: append([]string(nil), snap.SceneIDs...),
			MimeType: snap.MIMEType, Sha256: string(snap.SHA256), ErrorCode: snap.ErrorCode, ErrorDetail: snap.ErrorDetail,
			CheckpointSequence: snap.CheckpointSequence,
			TransferGeneration: snap.TransferGeneration,
		}
		for _, ref := range snap.JobRefs {
			msg.JobRefs = append(msg.JobRefs, &pb.AssetJobReference{JobId: ref.JobID, TaskId: ref.TaskID, SceneIds: append([]string(nil), ref.SceneIDs...)})
		}
		w.enqueueAssetProgress(msg)
	}

	rec := telemetry.RecorderFromContext(ctx)
	if rec == nil {
		return
	}
	h := rec.Begin(telemetry.EventSpec{
		Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask,
		Component: "worker.asset", Action: "progress_checkpoint",
	})
	h.SetMetadata("asset_id", snap.AssetID)
	h.SetMetadata("transfer_id", snap.TransferID)
	h.SetMetadata("state", string(snap.State))
	h.SetMetadata("progress_percent", fmt.Sprintf("%.1f", snap.ProgressPercent))
	h.SetMetadata("bytes_downloaded", snap.BytesDownloaded)
	h.SetMetadata("bytes_total", snap.BytesTotal)
	h.SetMetadata("throughput_bps", int64(snap.ThroughputBytesPerSecond))
	h.SetMetadata("eta_seconds", snap.ETASeconds)
	h.Complete()
}

func unixMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// workerFromProgressContext extracts the worker carrier installed by the
// asset-manager checkpoint hook. Contexts without that carrier are valid for
// headless tests and simply do not emit worker-to-master progress messages.
func workerFromProgressContext(ctx context.Context) (*Worker, bool) {
	if ctx == nil {
		return nil, false
	}
	w, ok := ctx.Value(assetProgressWorkerContextKey{}).(*Worker)
	return w, ok
}

type assetProgressWorkerContextKey struct{}

type assetProgressEnvelope struct {
	message *pb.AssetDownloadProgress
}

func (w *Worker) enqueueAssetProgress(message *pb.AssetDownloadProgress) {
	if w == nil || message == nil {
		return
	}
	w.assetProgressOnce.Do(func() {
		w.assetProgressQueue = make(chan assetProgressEnvelope, 256)
		go w.assetProgressSender()
	})
	select {
	case w.assetProgressQueue <- assetProgressEnvelope{message: message}:
	case <-w.stopChan:
	default:
		// Progress is diagnostic and throttled. Dropping an intermediate
		// checkpoint under backpressure is preferable to stalling a byte
		// transfer; the next checkpoint or terminal event supersedes it.
		w.logger.Warn("[ASSET_PROGRESS] checkpoint queue full; dropping update")
	}
}

func (w *Worker) assetProgressSender() {
	for {
		select {
		case <-w.stopChan:
			return
		case envelope := <-w.assetProgressQueue:
			if envelope.message == nil {
				continue
			}
			w.transportMu.RLock()
			transport := w.transport
			w.transportMu.RUnlock()
			if transport == nil {
				continue
			}
			w.assetProgressSendMu.Lock()
			err := transport.Send(context.Background(), controltransport.NewTypedMessage(controltransport.MsgAssetDownloadProgress, w.config.WorkerID, w.config.ProtocolVersion, envelope.message))
			w.assetProgressSendMu.Unlock()
			if err != nil {
				w.logger.Warn("[ASSET_PROGRESS] send failed: %v", err)
			}
		}
	}
}

// assetProgressBody wraps an http response body to report streamed bytes to
// the download manager's progress hook (one call per read chunk; the manager
// throttles its own publishes). Counts bytes actually read, so a partial or
// aborted stream reports exactly the bytes received.
type assetProgressBody struct {
	ctx          context.Context
	src          io.ReadCloser
	onProgress   func(downloadedBytes int64)
	done         int64
	maxBPS       int64
	lastRead     time.Time
	networkPacer prefetch.NetworkPacer
}

func (p *assetProgressBody) Read(b []byte) (int, error) {
	if p.maxBPS > 0 && p.lastRead.IsZero() {
		p.lastRead = time.Now()
	}
	n, err := p.src.Read(b)
	if n > 0 {
		// Prefer the shared NetworkAdmissionController over the local maxBPS cap.
		if p.networkPacer != nil {
			if waitErr := p.networkPacer.AcquireBytes(p.ctx, prefetch.NetDirIngress, prefetch.NetPriorityRuntime, int64(n)); waitErr != nil {
				return 0, waitErr
			}
		} else if p.maxBPS > 0 {
			delay := time.Duration(float64(n) / float64(p.maxBPS) * float64(time.Second))
			if err := waitForAssetDuration(p.ctx, delay); err != nil {
				return 0, err
			}
			p.lastRead = time.Now()
		}
		p.done += int64(n)
		p.onProgress(p.done)
	}
	return n, err
}

func (p *assetProgressBody) Close() error { return p.src.Close() }

func waitForAssetDuration(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parseAssetContentRange(value string) (start, end, total int64, err error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid range unit")
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes "), "/", 2)
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid range shape")
	}
	bounds := strings.SplitN(parts[0], "-", 2)
	if len(bounds) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid range bounds")
	}
	start, err = strconv.ParseInt(bounds[0], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	end, err = strconv.ParseInt(bounds[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, fmt.Errorf("invalid range end")
	}
	if parts[1] == "*" {
		return start, end, -1, nil
	}
	total, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || total <= end {
		return 0, 0, 0, fmt.Errorf("invalid range total")
	}
	return start, end, total, nil
}
