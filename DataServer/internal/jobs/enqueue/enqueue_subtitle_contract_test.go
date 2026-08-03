package enqueue

import (
	"strings"
	"testing"
)

func TestValidateEnqueueInputRejectsRetiredTopLevelSubtitleTracks(t *testing.T) {
	enqueuer := &Enqueuer{}
	_, _, err := enqueuer.validateEnqueueInput(map[string]interface{}{
		"subtitle_tracks": []interface{}{map[string]interface{}{"source": "velox-asset://subtitle"}},
	})
	if err == nil {
		t.Fatal("validateEnqueueInput accepted retired top-level subtitle_tracks")
	}
	if !strings.Contains(err.Error(), "scenes[].subtitles") {
		t.Fatalf("error = %v, want canonical replacement guidance", err)
	}
}
