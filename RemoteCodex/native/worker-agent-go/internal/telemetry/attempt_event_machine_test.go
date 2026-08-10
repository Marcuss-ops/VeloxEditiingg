package telemetry

import (
	"testing"
	"time"
)

func TestAttemptEventMachine_OrderIdempotencyAndRetryIdentity(t *testing.T) {
	recorder := NewEventRecorder()
	machine := NewAttemptEventMachine(recorder, "attempt-1")
	machine.AttemptStarted()
	machine.AttemptStarted()
	machine.PhaseChanged(PhaseRender)
	machine.PhaseChanged(PhaseRender)
	machine.SegmentStarted(3, PhaseRender)
	machine.SegmentStarted(3, PhaseRender)
	machine.SegmentCompleted(3, PhaseRender)
	machine.SegmentCompleted(3, PhaseRender)
	machine.ArtifactVerifyStarted()
	machine.ArtifactVerifyStarted()
	machine.ArtifactVerified(StatusOK, nil)
	machine.ArtifactVerified(StatusOK, nil)
	machine.DeliveryStarted()
	machine.DeliveryStarted()
	machine.AttemptCompleted(StatusOK)
	machine.AttemptCompleted(StatusOK)

	events := machine.Snapshot()
	if len(events) != 8 {
		t.Fatalf("canonical event count=%d, want 8", len(events))
	}
	want := []string{
		AttemptEventStarted, AttemptEventPhaseChanged, AttemptEventSegmentStarted,
		AttemptEventSegmentCompleted, AttemptEventArtifactVerifyStarted,
		AttemptEventArtifactVerified, AttemptEventDeliveryStarted, AttemptEventCompleted,
	}
	for i, name := range want {
		if events[i].EventName != name {
			t.Fatalf("event[%d]=%q, want %q", i, events[i].EventName, name)
		}
	}

	retry := CanonicalAttemptEvents("attempt-1", recorder.Snapshot())
	if len(retry) != len(events) {
		t.Fatalf("retry projection count=%d, want %d", len(retry), len(events))
	}
	for i := range events {
		if retry[i].EventID != events[i].EventID {
			t.Fatalf("retry event[%d] id=%q, want stable %q", i, retry[i].EventID, events[i].EventID)
		}
	}
}

func TestAttemptEventMachine_ProgressEmitsPhaseAndSegmentEdgesInOrder(t *testing.T) {
	recorder := NewEventRecorder()
	machine := NewAttemptEventMachine(recorder, "attempt-order")
	machine.AttemptStarted()
	machine.ProgressUpdated(PhaseRender, 1, 10, 100, 100, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))

	events := machine.Snapshot()
	want := []string{AttemptEventStarted, AttemptEventPhaseChanged, AttemptEventSegmentStarted, AttemptEventProgressUpdated}
	if len(events) != len(want) {
		t.Fatalf("event count=%d, want %d: %#v", len(events), len(want), events)
	}
	for i, name := range want {
		if events[i].EventName != name {
			t.Fatalf("event[%d]=%q, want %q", i, events[i].EventName, name)
		}
	}

	machine.ProgressUpdated(PhaseRender, 1, 20, 200, 200, time.Date(2026, 8, 10, 12, 0, 1, 0, time.UTC))
	machine.ProgressUpdated(PhaseRender, 2, 30, 300, 300, time.Date(2026, 8, 10, 12, 0, 2, 0, time.UTC))
	events = machine.Snapshot()
	want = []string{AttemptEventStarted, AttemptEventPhaseChanged, AttemptEventSegmentStarted,
		AttemptEventProgressUpdated, AttemptEventSegmentStarted, AttemptEventProgressUpdated}
	if len(events) != len(want) {
		t.Fatalf("transition event count=%d, want %d: %#v", len(events), len(want), events)
	}
	for i, name := range want {
		if events[i].EventName != name {
			t.Fatalf("transition event[%d]=%q, want %q", i, events[i].EventName, name)
		}
	}
}

func TestAttemptEventMachine_ProgressThrottle(t *testing.T) {
	recorder := NewEventRecorder()
	machine := NewAttemptEventMachine(recorder, "attempt-progress")
	base := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	machine.ProgressUpdated(PhaseRender, 1, 10, 100, 100, base)
	machine.ProgressUpdated(PhaseRender, 1, 11, 200, 200, base.Add(time.Second))
	machine.ProgressUpdated(PhaseRender, 1, 11, 200, 200, base.Add(time.Second))
	machine.ProgressUpdated(PhaseRender, 1, 12, 300, 300, base.Add(2*time.Second))

	events := machine.Snapshot()
	// The first direct progress sample also emits the missing phase and
	// segment lifecycle edges; only subsequent progress samples are subject
	// to the two-second throttle.
	if len(events) != 4 {
		t.Fatalf("throttled progress events=%d, want 4", len(events))
	}
	want := []string{AttemptEventPhaseChanged, AttemptEventSegmentStarted,
		AttemptEventProgressUpdated, AttemptEventProgressUpdated}
	for i, name := range want {
		if events[i].EventName != name {
			t.Fatalf("progress event[%d]=%q, want %q", i, events[i].EventName, name)
		}
	}
}
