package assets

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestKeyframeIndexPersistsAndRejectsSizeMismatch(t *testing.T) {
	index := NewKeyframeIndex(t.TempDir())
	probe := VideoProbe{
		DurationSeconds: 4, Width: 1920, Height: 1080, FPSNum: 24, FPSDen: 1,
		VideoCodec: "h264", PixelFormat: "yuv420p", FrameCount: 96,
		Keyframes: []float64{0, 2, 4},
	}
	sha := strings.Repeat("a", 64)
	if err := index.Store(context.Background(), sha, 1024, probe); err != nil {
		t.Fatalf("Store: %v", err)
	}
	got, ok := index.Lookup(context.Background(), sha, 1024)
	if !ok || len(got.Keyframes) != 3 || got.FrameCount != 96 {
		t.Fatalf("Lookup = %+v, %v; want persisted probe", got, ok)
	}
	// A new index object over the same directory models a process restart: the
	// certification must survive without the original in-memory state.
	reopened := NewKeyframeIndex(index.dir)
	got, ok = reopened.Lookup(context.Background(), sha, 1024)
	if !ok || len(got.Keyframes) != 3 || got.FrameCount != 96 {
		t.Fatalf("reopened Lookup = %+v, %v; want persisted probe", got, ok)
	}
	if stats := index.metrics.Snapshot(); stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("index counters after one hit = %+v, want hits=1 misses=0", stats)
	}
	if stats := reopened.metrics.Snapshot(); stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("reopened index counters = %+v, want hits=1 misses=0", stats)
	}
	if _, ok := index.Lookup(context.Background(), sha, 1025); ok {
		t.Fatal("a size-mismatched source must miss the index")
	}
	if stats := index.metrics.Snapshot(); stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("index counters after size mismatch = %+v, want hits=1 misses=1", stats)
	}
	if _, err := os.Stat(index.path(sha)); err != nil {
		t.Fatalf("index sidecar was not written: %v", err)
	}
}

type countingProbeRunner struct {
	calls int
	body  string
}

func (r *countingProbeRunner) Run(_ context.Context, name string, _ ...string) ([]byte, error) {
	r.calls++
	if name != "ffprobe" {
		return nil, os.ErrInvalid
	}
	return []byte(r.body), nil
}

func TestProbeWithIdentityReusesPersistedKeyframes(t *testing.T) {
	runner := &countingProbeRunner{body: `{
		"streams":[{"codec_type":"video","codec_name":"h264","profile":"High","width":1920,"height":1080,
		"r_frame_rate":"24/1","time_base":"1/90000","pix_fmt":"yuv420p","duration":"4.000000","nb_frames":"96"}],
		"format":{"duration":"4.000000"},
		"frames":[
		{"media_type":"video","key_frame":1,"best_effort_timestamp_time":"0.000000"},
		{"media_type":"video","key_frame":0,"best_effort_timestamp_time":"0.041667"},
		{"media_type":"video","key_frame":1,"best_effort_timestamp_time":"2.000000"}]
	}`}
	dir := t.TempDir()
	trimmer := newVideoTrimmerForTest(runner)
	trimmer.keyframes = NewKeyframeIndex(dir)
	sha := strings.Repeat("b", 64)
	first, err := trimmer.ProbeWithIdentity(context.Background(), "/tmp/source.mp4", sha, 4096)
	if err != nil {
		t.Fatalf("first probe: %v", err)
	}
	if len(first.Keyframes) != 2 || runner.calls != 1 {
		t.Fatalf("first probe keyframes=%v calls=%d", first.Keyframes, runner.calls)
	}
	if stats := trimmer.keyframes.metrics.Snapshot(); stats.Hits != 0 || stats.Misses != 1 {
		t.Fatalf("first probe index counters = %+v, want hits=0 misses=1", stats)
	}
	// A fresh trimmer over the same cache directory models a worker restart.
	// The persisted keyframe list must be reused without another ffprobe.
	restarted := newVideoTrimmerForTest(runner)
	restarted.keyframes = NewKeyframeIndex(dir)
	second, err := restarted.ProbeWithIdentity(context.Background(), "/tmp/source.mp4", sha, 4096)
	if err != nil {
		t.Fatalf("cached probe: %v", err)
	}
	if runner.calls != 1 {
		t.Fatalf("cached probe re-ran ffprobe: calls=%d", runner.calls)
	}
	if len(second.Keyframes) != 2 || second.Keyframes[1] != 2 {
		t.Fatalf("cached probe lost keyframes: %+v", second)
	}
	if stats := restarted.keyframes.metrics.Snapshot(); stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("restarted index counters = %+v, want hits=1 misses=0", stats)
	}
}
