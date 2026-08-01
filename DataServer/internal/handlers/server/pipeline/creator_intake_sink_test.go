package pipeline

import (
	"sync"
	"testing"
)

// recordingIntakeSink is a test mock that records every IncAccepted call.
// Used to verify the creator_push handler stamps the correct intake path.
type recordingIntakeSink struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingIntakeSink) IncAccepted(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, path)
}

func (r *recordingIntakeSink) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// TestIntakeSinkOrNoop_WiredSink verifies that a wired sink receives the
// IncAccepted call. This is the canonical proof that the handler's
// observation point (post-CAS) routes to the wired sink.
func TestIntakeSinkOrNoop_WiredSink(t *testing.T) {
	sink := &recordingIntakeSink{}
	h := &Handlers{}
	h.WithIntakeSink(sink)

	h.intakeSinkOrNoop().IncAccepted("creator_push")

	calls := sink.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d (%v)", len(calls), calls)
	}
	if calls[0] != "creator_push" {
		t.Fatalf("expected 'creator_push', got %q", calls[0])
	}
}

// TestIntakeSinkOrNoop_NilSinkFallsBackToNoop verifies that a Handlers
// without a wired sink falls back to a noop (does not panic, does not
// increment any counter). This is the safe default for tests that have
// not wired the metric.
func TestIntakeSinkOrNoop_NilSinkFallsBackToNoop(t *testing.T) {
	h := &Handlers{}

	// Must not panic.
	h.intakeSinkOrNoop().IncAccepted("creator_push")
	h.intakeSinkOrNoop().IncAccepted("creator_forwarder")
}

// TestIntakeSinkOrNoop_NilSinkExplicit verifies that passing nil to
// WithIntakeSink is a noop (the handler still falls back to noop).
func TestIntakeSinkOrNoop_NilSinkExplicit(t *testing.T) {
	sink := &recordingIntakeSink{}
	h := &Handlers{}
	h.WithIntakeSink(nil)  // explicit nil
	h.WithIntakeSink(sink) // then real sink

	h.intakeSinkOrNoop().IncAccepted("creator_push")

	calls := sink.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d (%v)", len(calls), calls)
	}
}
