package observability

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// TestAdminJobInspectAPI_IncludesExecutionWaterfallBlock locks the §20 JSON
// contract consumed by `fleetctl job inspect` (the CLI reprints this payload
// verbatim): execution.waterfall carries wall_ms / accounted_ms /
// unaccounted_ms / coverage_pct and the ordered bucket list straight from the
// worker's durable milestone timeline.
func TestAdminJobInspectAPI_IncludesExecutionWaterfallBlock(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-api-wf"] = &taskgraph.Task{
		ID: "T-api-wf", JobID: "J-api-wf", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	start := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	end := start.Add(328041 * time.Millisecond)
	attempts.attempts["T-api-wf"] = []taskattempts.TaskAttempt{{
		ID: "A-api-wf", TaskID: "T-api-wf", JobID: "J-api-wf", AttemptNumber: 1,
		WorkerID: "worker-api-wf", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &start, CompletedAt: &end,
	}}
	attempts.rawReports = map[string]string{"A-api-wf": realisticAttemptReportJSON}
	svc.WithJobs(&inspectionJobReader{job: &jobs.Job{ID: "J-api-wf", Status: jobs.StatusSucceeded}}).
		WithJobInspection(inspectionExtras{})

	r := gin.New()
	NewModule(svc).RegisterRoutes(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/admin/jobs/J-api-wf", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET admin job = %d: %s", w.Code, w.Body.String())
	}

	var response struct {
		Execution struct {
			Waterfall *struct {
				AttemptID     string  `json:"attempt_id"`
				WallMS        int64   `json:"wall_ms"`
				AccountedMS   int64   `json:"accounted_ms"`
				UnaccountedMS int64   `json:"unaccounted_ms"`
				CoveragePct   float64 `json:"coverage_pct"`
				Buckets       []struct {
					Name       string `json:"name"`
					DurationMS int64  `json:"duration_ms"`
				} `json:"buckets"`
			} `json:"waterfall"`
			Attempts []map[string]any `json:"attempts"`
		} `json:"execution"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode admin job response: %v", err)
	}
	wf := response.Execution.Waterfall
	if wf == nil {
		t.Fatal("execution.waterfall missing from the fleetctl job inspect payload")
	}
	if wf.WallMS != 328041 || wf.AccountedMS != 328041 || wf.UnaccountedMS != 0 {
		t.Fatalf("waterfall = %+v, want fully reconciled 328041ms wall", wf)
	}
	if wf.CoveragePct <= 98 {
		t.Fatalf("coverage_pct = %f, want > 98 for a complete timeline", wf.CoveragePct)
	}
	var assetPrep *int64
	for i := range wf.Buckets {
		if wf.Buckets[i].Name == "asset_preparation" {
			assetPrep = &wf.Buckets[i].DurationMS
		}
	}
	if assetPrep == nil || *assetPrep != 298246 {
		t.Fatalf("asset_preparation bucket = %+v, want the dominant 298246ms", assetPrep)
	}
}

func TestAdminJobInspectAPI_ExposesCanonicalLiveExecution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-api-live"] = &taskgraph.Task{
		ID: "T-api-live", JobID: "J-api-live", Status: taskgraph.StatusRunning, AttemptCount: 1,
	}
	attempts.attempts["T-api-live"] = []taskattempts.TaskAttempt{{
		ID: "A-api-live", TaskID: "T-api-live", JobID: "J-api-live", AttemptNumber: 1,
		WorkerID: "worker-api", Status: taskattempts.AttemptStatusRunning,
	}}
	svc.WithJobs(&inspectionJobReader{job: &jobs.Job{ID: "J-api-live", Status: jobs.StatusRunning}}).
		WithJobInspection(inspectionExtras{}).
		WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
			TaskID: "T-api-live", JobID: "J-api-live", AttemptID: "A-api-live", AttemptNumber: 1,
			WorkerID: "worker-api", RuntimeStatus: "RUNNING", ProgressPercent: 46,
			ProgressPhase: "building_segments", CurrentScene: 7, TotalScenes: 13,
			CurrentSegment: 12, TotalSegments: 26, FramesEncoded: 18432,
			FramesDecoded: 19000, FramesComposited: 18432, FFmpegSpeedX: 2.37,
			ElapsedMS: 183421, LastProgressAt: "2026-08-10T10:03:42Z",
		}})

	r := gin.New()
	NewModule(svc).RegisterRoutes(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/admin/jobs/J-api-live", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET admin job = %d: %s", w.Code, w.Body.String())
	}

	var response struct {
		Execution struct {
			AttemptID      string           `json:"attempt_id"`
			WorkerID       string           `json:"worker_id"`
			Phase          string           `json:"phase"`
			Progress       map[string]any   `json:"progress"`
			LiveMetrics    map[string]any   `json:"live_metrics"`
			LastProgressAt string           `json:"last_progress_at"`
			Attempts       []map[string]any `json:"attempts"`
		} `json:"execution"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode admin job response: %v", err)
	}
	if response.Execution.AttemptID != "A-api-live" || response.Execution.WorkerID != "worker-api" ||
		response.Execution.Phase != "building_segments" || response.Execution.LastProgressAt != "2026-08-10T10:03:42Z" {
		t.Fatalf("execution identity = %#v; want canonical live Attempt fields", response.Execution)
	}
	if got := response.Execution.Progress["scene"]; got != float64(7) {
		t.Fatalf("execution.progress.scene = %#v, want 7", got)
	}
	if got := response.Execution.Progress["segment"]; got != float64(12) {
		t.Fatalf("execution.progress.segment = %#v, want 12", got)
	}
	if got := response.Execution.Progress["scenes_total"]; got != float64(13) {
		t.Fatalf("execution.progress.scenes_total = %#v, want 13", got)
	}
	if got := response.Execution.Progress["segments_total"]; got != float64(26) {
		t.Fatalf("execution.progress.segments_total = %#v, want 26", got)
	}
	if got := response.Execution.LiveMetrics["frames_encoded"]; got != float64(18432) {
		t.Fatalf("execution.live_metrics.frames_encoded = %#v, want 18432", got)
	}
	if got := response.Execution.LiveMetrics["frames_decoded"]; got != float64(19000) {
		t.Fatalf("execution.live_metrics.frames_decoded = %#v, want 19000", got)
	}
	if got := response.Execution.LiveMetrics["frames_composited"]; got != float64(18432) {
		t.Fatalf("execution.live_metrics.frames_composited = %#v, want 18432", got)
	}
	if got := response.Execution.LiveMetrics["elapsed_ms"]; got != float64(183421) {
		t.Fatalf("execution.live_metrics.elapsed_ms = %#v, want 183421", got)
	}
	if got := response.Execution.LiveMetrics["ffmpeg_speed_x"]; got != 2.37 {
		t.Fatalf("execution.live_metrics.ffmpeg_speed_x = %#v, want 2.37", got)
	}
	if len(response.Execution.Attempts) != 1 || response.Execution.Attempts[0]["attempt_id"] != "A-api-live" {
		t.Fatalf("legacy attempts projection changed: %#v", response.Execution.Attempts)
	}
}

func TestAdminJobInspectAPI_PreservesLegacyExecutionShapeWithoutLiveRuntime(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-api-legacy"] = &taskgraph.Task{
		ID: "T-api-legacy", JobID: "J-api-legacy", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	svc.WithJobs(&inspectionJobReader{job: &jobs.Job{ID: "J-api-legacy", Status: jobs.StatusSucceeded}}).
		WithJobInspection(inspectionExtras{})

	r := gin.New()
	NewModule(svc).RegisterRoutes(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/admin/jobs/J-api-legacy", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET legacy admin job = %d: %s", w.Code, w.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode legacy admin job response: %v", err)
	}
	execution, ok := response["execution"].(map[string]any)
	if !ok {
		t.Fatalf("execution missing or wrong type: %#v", response["execution"])
	}
	for _, field := range []string{"task_id", "job_id", "task_status", "attempt_count", "phase_totals", "attempts"} {
		if _, ok := execution[field]; !ok {
			t.Fatalf("legacy execution field %q missing: %#v", field, execution)
		}
	}
	for _, field := range []string{"attempt_id", "worker_id", "phase", "progress", "live_metrics", "last_progress_at"} {
		if _, ok := execution[field]; ok {
			t.Fatalf("live-only field %q unexpectedly present for legacy job: %#v", field, execution)
		}
	}
}
