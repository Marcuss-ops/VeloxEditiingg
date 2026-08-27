package native

import (
	"context"
	"io"
	"strings"
	"testing"

	"velox-worker-agent/pkg/video/pipeline"
)

func TestStreamEngineOutputPropagatesArtifactWriteProgress(t *testing.T) {
	progress := `{"event":"artifact_write_progress","artifact":"final_video","path":"/tmp/video.partial.mp4","high_watermark_bytes":25165824,"safe_offset_bytes":16777216,"finalized":false}
`
	got := make(chan pipeline.ArtifactWriteProgress, 1)
	ctx := pipeline.WithArtifactWriteCallback(context.Background(), func(p pipeline.ArtifactWriteProgress) { got <- p })
	var stderrBuf, stdoutBuf strings.Builder
	done := streamEngineOutput(
		io.NopCloser(strings.NewReader("")),
		io.NopCloser(strings.NewReader(progress)), ctx, nil, nil, &stderrBuf, &stdoutBuf)
	<-done
	select {
	case p := <-got:
		if p.Artifact != "final_video" || p.Path != "/tmp/video.partial.mp4" || p.HighWatermarkBytes != 25165824 || p.SafeOffsetBytes != 16777216 || p.Finalized {
			t.Fatalf("artifact write progress = %+v", p)
		}
	default:
		t.Fatal("artifact write progress callback not invoked")
	}
}

func TestStreamEngineOutputPropagatesSegmentCompleted(t *testing.T) {
	progress := `{"percent":80,"scene":4,"total_scenes":4,"segment":4,"total_segments":4,"segment_completed":true,"phase":"building_segments","frames_encoded":120,"elapsed_ms":2400}
`
	got := make(chan pipeline.ProgressSnapshot, 1)
	var stderrBuf, stdoutBuf strings.Builder
	done := streamEngineOutput(
		io.NopCloser(strings.NewReader("")),
		io.NopCloser(strings.NewReader(progress)),
		context.Background(),
		func(snapshot pipeline.ProgressSnapshot) { got <- snapshot },
		nil,
		&stderrBuf,
		&stdoutBuf,
	)
	<-done

	select {
	case snapshot := <-got:
		if !snapshot.SegmentCompleted || snapshot.Segment != 4 || snapshot.TotalSegments != 4 {
			t.Fatalf("segment completion snapshot = %+v", snapshot)
		}
	case <-context.Background().Done():
		t.Fatal("unreachable")
	default:
		t.Fatal("native progress parser emitted no snapshot")
	}
}
