package executors

import (
	"strings"
	"testing"

	"velox-worker-agent/pkg/video/plan"
)

func TestWriteAudioMixTrackMutesSelectedTimelineRanges(t *testing.T) {
	var filter strings.Builder
	label := writeAudioMixTrack(&filter, 0, plan.AudioTrack{
		Volume:     1,
		MuteRanges: []plan.AudioMuteRange{{StartSeconds: 12.5, EndSeconds: 18}},
	})
	want := "[0:a]volume='if(between(t\\,12.500000\\,18.000000)\\,0\\,1.000000)':eval=frame[a0]"
	if got := filter.String(); got != want {
		t.Fatalf("audio filter = %q, want %q", got, want)
	}
	if label != "[a0]" {
		t.Fatalf("audio label = %q, want [a0]", label)
	}
}
