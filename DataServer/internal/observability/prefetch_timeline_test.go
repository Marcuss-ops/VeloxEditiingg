package observability

import (
	"fmt"
	"strings"
	"testing"
)

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

func TestPrefetchDiagnosticsJoinTwentyNineAssetsAndRedactSources(t *testing.T) {
	keys := make([]any, 29)
	progress := make([]AssetProgressView, 28)
	for i := 0; i < 29; i++ {
		keys[i] = fmt.Sprintf("sha256:%064x", i+1)
		if i == 28 {
			continue
		}
		progress[i] = AssetProgressView{
			JobID: "job-29", WorkerID: "worker-1", AssetKey: keys[i].(string), AssetID: fmt.Sprintf("asset-%d", i),
			Role: "stock", State: "READY", Attempt: 1, HTTPStatus: 206,
			BytesDownloaded: 4096, BytesTotal: 4096, SHA256: fmt.Sprintf("%064x", i+1),
			HashVerified: true, UpdatedAt: "2026-11-22T10:00:00Z",
		}
	}
	planned := BuildPrefetchTimeline([]JobEvent{{JobID: "job-29", Event: "prefetch.future_plan_sent", Payload: map[string]any{
		"asset_keys": keys, "worker_id": "worker-1", "task_id": "task-29", "distance": float64(1),
	}}})
	progress = append(progress, AssetProgressView{
		JobID: "job-29", WorkerID: "worker-1", AssetKey: keys[28].(string), AssetID: "asset-28",
		Role: "stock", State: "RETRY_WAIT", Attempt: 4, HTTPStatus: 503,
		BytesDownloaded: 1024, BytesTotal: 4096, UpdatedAt: "2026-11-22T10:14:59Z",
		ErrorCode: "HTTP_503", ErrorDetail: "GET https://signed.example/a?token=secret upstream unavailable",
	})
	got := mergePrefetchAssetProgress(planned, progress, "job-29")
	if len(got) != 29 {
		t.Fatalf("asset diagnostics=%d, want 29", len(got))
	}
	var blocked *PrefetchTimeline
	ready := 0
	for i := range got {
		if got[i].Phase == "READY" {
			ready++
			if got[i].LastHTTPStatus != 206 || !got[i].HashVerified || got[i].BytesReceived != got[i].BytesExpected {
				t.Fatalf("ready asset missing range/hash evidence: %+v", got[i])
			}
		}
		if got[i].Phase == "RETRY_WAIT" {
			blocked = &got[i]
		}
	}
	if ready != 28 || blocked == nil {
		t.Fatalf("ready=%d blocked=%+v", ready, blocked)
	}
	if blocked.AssetID != "asset-28" || blocked.WorkerID != "worker-1" || blocked.Attempts != 4 || blocked.LastHTTPStatus != 503 || blocked.BytesReceived != 1024 || blocked.LastErrorCode != "HTTP_503" {
		t.Fatalf("blocked asset diagnostic incomplete: %+v", blocked)
	}
	if strings.Contains(blocked.LastError, "https://") || strings.Contains(blocked.LastError, "secret") {
		t.Fatalf("prefetch diagnostic leaked signed source: %q", blocked.LastError)
	}
}
