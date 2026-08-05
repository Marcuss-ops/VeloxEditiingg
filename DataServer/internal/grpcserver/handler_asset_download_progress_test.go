package grpcserver

import (
	"context"
	"testing"

	"velox-server/internal/store"
	pb "velox-shared/controltransport/pb"
)

type recordingAssetProgressSink struct {
	records []store.AssetDownloadProgressRecord
}

func (s *recordingAssetProgressSink) IngestAssetDownloadProgress(_ context.Context, record store.AssetDownloadProgressRecord) error {
	s.records = append(s.records, record)
	return nil
}

func TestHandleAssetDownloadProgressUsesTypedRecord(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{AllowInsecure: true})
	sink := &recordingAssetProgressSink{}
	h.SetAssetDownloadProgressSink(sink)
	h.handleAssetDownloadProgress("worker-1", &pb.AssetDownloadProgress{
		WorkerId: "worker-1", TransferId: "transfer-1", AssetKey: "sha256:abc",
		AssetId: "asset-1", State: "DOWNLOADING", BytesDownloaded: 42,
		BytesTotal: 100, JobIds: []string{"job-a", "job-b"},
	})
	if len(sink.records) != 1 {
		t.Fatalf("records=%d, want 1", len(sink.records))
	}
	if got := sink.records[0].BytesDownloaded; got != 42 {
		t.Fatalf("bytes_downloaded=%d, want 42", got)
	}
	if len(sink.records[0].JobIDs) != 2 {
		t.Fatalf("job_ids=%v, want two refs", sink.records[0].JobIDs)
	}
}

func TestHandleAssetDownloadProgressRejectsWorkerSpoof(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{AllowInsecure: true})
	sink := &recordingAssetProgressSink{}
	h.SetAssetDownloadProgressSink(sink)
	h.handleAssetDownloadProgress("worker-1", &pb.AssetDownloadProgress{
		WorkerId: "worker-evil", AssetKey: "sha256:abc", State: "READY",
	})
	if len(sink.records) != 0 {
		t.Fatalf("spoofed worker progress was ingested: %#v", sink.records)
	}
}
