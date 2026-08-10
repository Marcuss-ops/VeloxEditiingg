package native

import (
	"context"
	"io"
	"strings"
	"testing"

	"velox-worker-agent/pkg/video/pipeline"
)

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
