package grpcserver

import (
	"context"
	"log"
	"time"

	"velox-server/internal/store"
	pb "velox-shared/controltransport/pb"
)

// AssetDownloadProgressSink is the narrow persistence contract for the
// worker->master latest-state read model.
type AssetDownloadProgressSink interface {
	IngestAssetDownloadProgress(context.Context, store.AssetDownloadProgressRecord) error
}

// handleAssetDownloadProgress validates the worker identity and delegates the
// atomic latest-state + job-reference projection to the configured sink.
func (h *Handler) handleAssetDownloadProgress(workerID string, p *pb.AssetDownloadProgress) {
	if p == nil || p.GetAssetKey() == "" {
		log.Printf("[GRPC] asset download progress from worker %s rejected: missing asset_key", workerID)
		return
	}
	if declared := p.GetWorkerId(); declared != "" && declared != workerID {
		log.Printf("[GRPC] asset download progress from worker %s rejected: worker_id=%s mismatch", workerID, declared)
		return
	}
	if h.assetDownloadProgressSink == nil && h.dbStore == nil {
		log.Printf("[GRPC] asset download progress from worker %s dropped: no persistence sink", workerID)
		return
	}
	sink := h.assetDownloadProgressSink
	if sink == nil {
		sink = h.dbStore
	}
	record := store.AssetDownloadProgressRecord{
		WorkerID:           workerID,
		TransferID:         p.GetTransferId(),
		AssetKey:           p.GetAssetKey(),
		AssetID:            p.GetAssetId(),
		Role:               p.GetRole(),
		State:              p.GetState(),
		BytesDownloaded:    p.GetBytesDownloaded(),
		BytesTotal:         p.GetBytesTotal(),
		BytesPerSecond:     p.GetBytesPerSecond(),
		ETASeconds:         p.GetEtaSeconds(),
		Attempt:            int(p.GetAttempt()),
		SharedWaiters:      int(p.GetSharedWaiters()),
		CacheHit:           p.GetCacheHit(),
		QueuedAt:           progressTime(p.GetQueuedAtUnixMs()),
		StartedAt:          progressTime(p.GetStartedAtUnixMs()),
		UpdatedAt:          progressTime(p.GetUpdatedAtUnixMs()),
		CompletedAt:        progressTime(p.GetCompletedAtUnixMs()),
		TaskID:             p.GetTaskId(),
		JobIDs:             p.GetJobIds(),
		JobRefs:            progressJobRefs(p.GetJobRefs()),
		SceneIDs:           p.GetSceneIds(),
		CheckpointSequence: p.GetCheckpointSequence(),
		TransferGeneration: p.GetTransferGeneration(),
		MIMEType:           p.GetMimeType(),
		SHA256:             p.GetSha256(),
		ErrorCode:          p.GetErrorCode(),
		ErrorDetail:        p.GetErrorDetail(),
		ReceivedAt:         time.Now().UTC(),
	}
	if err := sink.IngestAssetDownloadProgress(context.Background(), record); err != nil {
		log.Printf("[GRPC] asset download progress ingest failed worker=%s asset=%s: %v", workerID, p.GetAssetKey(), err)
	}
}

func progressJobRefs(refs []*pb.AssetJobReference) []store.AssetDownloadJobRef {
	out := make([]store.AssetDownloadJobRef, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		out = append(out, store.AssetDownloadJobRef{JobID: ref.GetJobId(), TaskID: ref.GetTaskId(), SceneIDs: ref.GetSceneIds()})
	}
	return out
}

func progressTime(unixMillis int64) time.Time {
	if unixMillis <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(unixMillis).UTC()
}
