package pipeline

import (
	"testing"

	"velox-server/internal/store"
)

func TestPrefetchReadModelUsesNewestLifecycleEvent(t *testing.T) {
	// The projection itself is pure over the ordered event stream; keep the
	// event-to-state mapping pinned separately from SQLite integration tests.
	cases := []struct {
		event string
		wantD string
		wantP string
	}{
		{"prefetch.future_plan_sent", "prefetch_queued", "sent"},
		{"prefetch.future_plan_received", "prefetch_planning", "received"},
		{"prefetch.future_plan_applied", "prefetch_preparing", "applied"},
		{"prefetch.prefetch_prepared", "prefetch_ready", "prepared"},
		{"prefetch.prefetch_failed", "prefetch_failed", "failed"},
	}
	for _, tc := range cases {
		t.Run(tc.event, func(t *testing.T) {
			gotD, gotP := prefetchStateFromEvents([]store.JobEvent{{Event: tc.event}})
			if gotD != tc.wantD || gotP != tc.wantP {
				t.Fatalf("state=(%q,%q), want (%q,%q)", gotD, gotP, tc.wantD, tc.wantP)
			}
		})
	}
}

func TestPrefetchReadModelAdvancesAcrossLifecycle(t *testing.T) {
	events := []store.JobEvent{
		{Event: "prefetch.future_plan_sent"},
		{Event: "prefetch.future_plan_received"},
		{Event: "prefetch.future_plan_applied"},
		{Event: "prefetch.prefetch_prepared"},
	}
	gotD, gotP := prefetchStateFromEvents(events)
	if gotD != "prefetch_ready" || gotP != "prepared" {
		t.Fatalf("state=(%q,%q), want final lifecycle state (prefetch_ready,prepared)", gotD, gotP)
	}
}
