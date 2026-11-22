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

func TestTerminalPrefetchProjectionFollowsJobState(t *testing.T) {
	tests := []struct {
		status, dispatch, plan, wantDispatch, wantPlan string
		issue                                          map[string]interface{}
	}{
		{"SUCCEEDED", "prefetch_waiting_assets", "pending", "completed", "completed", map[string]interface{}{"event": "stale"}},
		{"FAILED", "prefetch_waiting_assets", "pending", "job_failed", "failed", nil},
		{"CANCELLED", "prefetch_waiting_assets", "pending", "job_cancelled", "cancelled", nil},
		{"RUNNING", "prefetch_waiting_assets", "pending", "prefetch_waiting_assets", "pending", nil},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			gotDispatch, gotPlan, gotIssue := terminalPrefetchProjection(tt.status, "render failed", tt.dispatch, tt.plan, tt.issue)
			if gotDispatch != tt.wantDispatch || gotPlan != tt.wantPlan {
				t.Fatalf("projection=(%q,%q), want (%q,%q)", gotDispatch, gotPlan, tt.wantDispatch, tt.wantPlan)
			}
			if tt.status == "SUCCEEDED" || tt.status == "CANCELLED" {
				if gotIssue != nil {
					t.Fatalf("issue=%v, want nil", gotIssue)
				}
			}
			if tt.status == "FAILED" {
				if gotIssue == nil || gotIssue["message"] != "render failed" {
					t.Fatalf("failure issue=%v", gotIssue)
				}
			}
		})
	}
}
