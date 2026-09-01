package observability

import "testing"

func TestBuildPrefetchTimelineLinksPlanAndAssetEvents(t *testing.T) {
	events := []JobEvent{
		{JobID: "job-b", Event: "prefetch.future_plan_sent", Payload: map[string]any{
			"asset_keys": []any{"clip-b"}, "task_id": "task-b", "worker_id": "worker-1",
			"plan_id": "plan-1", "plan_version": float64(3), "distance": float64(1),
			"plan_sent_at": "2026-09-01T08:00:00Z",
		}},
		{JobID: "job-b", Event: "prefetch.download_started", Payload: map[string]any{
			"asset_key": "clip-b", "download_started_at": "2026-09-01T08:00:01Z", "origin": "prefetch",
		}},
		{JobID: "job-b", Event: "prefetch.asset_ready", Payload: map[string]any{
			"asset_key": "clip-b", "asset_ready_at": "2026-09-01T08:00:04Z", "cache_hit": false, "origin": "prefetch",
		}},
		{JobID: "job-b", Event: "prefetch.prefetch_ready_lead", Payload: map[string]any{
			"asset_key": "clip-b", "asset_ready_at": "2026-09-01T08:00:04Z", "job_started_at": "2026-09-01T08:00:12Z", "prefetch_ready_lead_ms": float64(8000),
		}},
	}
	got := BuildPrefetchTimeline(events)
	if len(got) != 1 {
		t.Fatalf("timeline entries=%d, want 1", len(got))
	}
	entry := got[0]
	if entry.AssetKey != "clip-b" || entry.Distance != 1 || entry.PlanID != "plan-1" {
		t.Fatalf("entry identity=%+v", entry)
	}
	if entry.PlanSentAt == "" || entry.DownloadStartedAt == "" || entry.AssetReadyAt == "" || entry.JobStartedAt == "" {
		t.Fatalf("incomplete timeline=%+v", entry)
	}
	if entry.PrefetchReadyLeadMS != 8000 || entry.Origin != "prefetch" {
		t.Fatalf("timing/provenance=%+v", entry)
	}
}

func TestBuildPrefetchTimelineShowsUnfinishedPlannedAssets(t *testing.T) {
	got := BuildPrefetchTimeline([]JobEvent{{JobID: "job-c", Event: "prefetch.future_plan_sent", Payload: map[string]any{
		"asset_keys": []any{"clip-c", "clip-d"}, "distance": float64(2),
	}}})
	if len(got) != 2 {
		t.Fatalf("timeline entries=%d, want 2", len(got))
	}
	for _, entry := range got {
		if entry.AssetReadyAt != "" || entry.JobStartedAt != "" {
			t.Fatalf("fabricated timing=%+v", entry)
		}
	}
}
