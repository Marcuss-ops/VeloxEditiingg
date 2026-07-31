package telemetry

import (
	"sync"
	"testing"
	"time"
)

func TestEventHandleCountersMetadataAndOffsets(t *testing.T) {
	r := NewEventRecorder()
	h := r.Begin(EventSpec{
		Origin: OriginEngine, Scope: ScopeSegment,
		Component: "engine.encode", Action: "setup",
		MetadataJSON: `{"preset":"fast"}`,
	})
	if h == nil {
		t.Fatal("Begin returned nil for registered event")
	}
	h.AddInputBytes(10)
	h.AddOutputBytes(20)
	h.AddFramesIn(3)
	h.AddFramesOut(4)
	h.AddFrames(5)
	h.SetMetadata("segment", 2)
	h.Complete()

	events := r.Flush()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	e := events[0]
	if e.BytesIn != 10 || e.BytesOut != 20 || e.Frames != 5 || e.FramesIn != 3 || e.FramesOut != 4 {
		t.Fatalf("counters = %+v", e)
	}
	if e.MetadataJSON == "" || e.StartedOffsetMS < 0 || e.FinishedOffsetMS < e.StartedOffsetMS {
		t.Fatalf("metadata/offsets = %q/%v/%v", e.MetadataJSON, e.StartedOffsetMS, e.FinishedOffsetMS)
	}
}

func TestEventHandleConcurrentCompleteRecordsOnce(t *testing.T) {
	r := NewEventRecorder()
	h := r.Begin(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "execute"})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Complete()
		}()
	}
	wg.Wait()
	if got := len(r.Flush()); got != 1 {
		t.Fatalf("concurrent completion recorded %d events, want 1", got)
	}
}

func TestEventRecorderOffsetsUseMonotonicClock(t *testing.T) {
	r := NewEventRecorder()
	start := time.Now()
	r.Record(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "cache_lookup"}, start, start.Add(time.Millisecond), 1, StatusOK, "", "")
	e := r.Flush()[0]
	if e.StartedOffsetMS != 0 || e.FinishedOffsetMS != 0 {
		t.Fatalf("explicit Record should preserve explicit-zero offsets, got %v/%v", e.StartedOffsetMS, e.FinishedOffsetMS)
	}
}
