package worker

// active_task_upload_progressive.go handles progressive upload negotiation:
// selecting between ordinary and progressive transport paths, journaling,
// and timing instrumentation.

import (
	"context"
	"fmt"
	"os"
	"time"

	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/telemetry"
)

// uploadWithNegotiatedPath keeps the existing V1 publication contract as the
// compatibility path. Progressive upload is selected only when the resolved
// transport advertises the capability and implements the optional interface;
// otherwise the ordinary Upload method is used automatically.
func uploadWithNegotiatedPath(ctx context.Context, transport publisher.Transport, req publisher.UploadRequest, progressivePartConcurrency int) (*publisher.UploadResult, error) {
	progress := artifactProgressForTask(ctx, req.Target.ArtifactID)
	var trailerToOpenMS int64
	if !progress.FinalizedAt.IsZero() {
		trailerToOpenMS = time.Since(progress.FinalizedAt).Milliseconds()
		if trailerToOpenMS < 0 {
			trailerToOpenMS = 0
		}
	}
	if !publisher.SupportsProgressive(transport) {
		res, err := transport.Upload(ctx, req)
		if err == nil && res != nil {
			res.Breakdown.TrailerToOpenMS = trailerToOpenMS
		}
		return res, err
	}

	progressive, ok := transport.(publisher.ProgressiveTransport)
	if !ok {
		return nil, fmt.Errorf("worker artifact upload: progressive capability negotiated but transport %q does not implement ProgressiveTransport", transport.ID())
	}
	file := publisher.NewGrowingFile()
	if progress.SafeOffsetBytes <= 0 {
		res, err := transport.Upload(ctx, req)
		if err == nil && res != nil {
			res.Breakdown.TrailerToOpenMS = trailerToOpenMS
		}
		return res, err
	}
	// Use the path from progress updates (the .partial path during the mux)
	// when available; fall back to the declared local path.  The .partial
	// file is the same inode as the final path after publishAtomic renames
	// it, so the fd remains valid across the rename.
	openPath := req.LocalPath
	if progress.Path != "" {
		openPath = progress.Path
	}
	file.Update(progress.SafeOffsetBytes, progress.Finalized, 0)
	if progress.Finalized {
		file.MarkDurable(progress.SafeOffsetBytes)
	}
	session, err := progressive.BeginProgressive(ctx, publisher.ProgressiveUploadRequest{
		Target:       req.Target,
		Artifact:     req.Target.ArtifactID,
		ExpectedSize: 0,
		CommitToken:  req.CommitToken,
	})
	if err != nil {
		return nil, fmt.Errorf("worker artifact upload: begin progressive %q: %w", transport.ID(), err)
	}
	st, err := os.Stat(openPath)
	if err != nil {
		_ = session.Abort(ctx)
		return nil, err
	}
	// mux_to_open_us: latency from when the first progress event with a
	// path was received from the C++ engine to when the Go side opened the
	// file for progressive upload. This closes the visibility gap between
	// trailer_to_publish_us (C++ finalization) and the Go upload start.
	var muxToOpenUS int64
	if !progress.FirstProgressAt.IsZero() {
		muxToOpenUS = time.Since(progress.FirstProgressAt).Microseconds()
		if muxToOpenUS < 0 {
			muxToOpenUS = 0
		}
	}
	if !progress.FinalizedAt.IsZero() {
		trailerToOpenMS = time.Since(progress.FinalizedAt).Milliseconds()
		if trailerToOpenMS < 0 {
			trailerToOpenMS = 0
		}
	}
	if progress.Finalized {
		file.Update(st.Size(), true, st.Size())
		file.MarkDurable(st.Size())
	}
	result, err := publisher.RunProgressiveUploadWithJournalAndStoreOptions(ctx, openPath, req.Target.ChunkSize, file, session, progressiveJournalPath(req), nil, "", publisher.ProgressiveUploadOptions{Workers: progressivePartConcurrency}, req.Progress)
	if err != nil {
		_ = session.Abort(ctx)
		return nil, err
	}
	result.Breakdown.TrailerToOpenMS = trailerToOpenMS
	result.Breakdown.MuxToOpenUS = muxToOpenUS
	telemetry.GetPrometheusMetrics().RecordProgressiveUploadTiming(
		time.Duration(result.Breakdown.FirstPartStartedMS)*time.Millisecond,
		result.Breakdown.PartsUploadedBeforeRenderEnd,
		result.Breakdown.BytesUploadedBeforeRenderEnd,
		time.Duration(result.Breakdown.OverlapMS)*time.Millisecond,
	)
	telemetry.GetPrometheusMetrics().RecordMuxToOpenUS(muxToOpenUS)
	return result, nil
}

func progressiveJournalPath(req publisher.UploadRequest) string {
	if req.Target.UploadID == "" || req.LocalPath == "" {
		return ""
	}
	return req.LocalPath + "." + req.Target.UploadID + ".progressive.json"
}
