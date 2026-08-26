package taskrunner

import (
	"context"
	"testing"
	"time"
)

func TestWaterfallRecorderIsSerialAndLeavesGapsVisible(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	r := NewWaterfallRecorder(base)
	r.Transition("wait_before_assets", base)
	r.Transition("asset_resolve", base.Add(2*time.Second))
	r.Finish(base.Add(5*time.Second), "ok")
	got := r.Snapshot()
	if len(got) != 2 || got[0].DurationMS != 2000 || got[1].DurationMS != 3000 {
		t.Fatalf("waterfall = %#v, want two serial stages of 2000ms and 3000ms", got)
	}
	if !got[1].StartedAt.Equal(base.Add(2 * time.Second)) {
		t.Fatalf("asset stage started at %v, want %v", got[1].StartedAt, base.Add(2*time.Second))
	}
}

func TestWaterfallRecorderContextRoundTrip(t *testing.T) {
	r := NewWaterfallRecorder(time.Now())
	ctx := WithWaterfallRecorder(context.Background(), r)
	if WaterfallRecorderFromContext(ctx) != r {
		t.Fatal("waterfall recorder was not preserved in context")
	}
}
