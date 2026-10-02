package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
)

// TestSceneKindWarningsForPayload_EnvelopeShape pins the accept-envelope
// warning: stable code, scene identifiers, detail mentioning the worker
// behavior, and the sunset date after which this becomes a rejection.
func TestSceneKindWarningsForPayload_EnvelopeShape(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id": "scene-0062", "kind": "clip", "text": "t",
				"stock": map[string]interface{}{"url": "velox-drive://testimony"},
			},
		},
	}
	warnings := sceneKindWarningsForPayload(payload)
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one", warnings)
	}
	if warnings[0]["code"] != sceneKindWarningCode {
		t.Fatalf("code = %v, want %q", warnings[0]["code"], sceneKindWarningCode)
	}
	ids, _ := warnings[0]["scene_ids"].([]string)
	if len(ids) != 1 || ids[0] != "scene-0062" {
		t.Fatalf("scene_ids = %v, want [scene-0062]", ids)
	}
	if warnings[0]["sunset"] != "2026-11-15" {
		t.Fatalf("sunset = %v, want the contract sunset", warnings[0]["sunset"])
	}
	if sceneKindWarningsForPayload(map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{"kind": "stock"}},
	}) != nil {
		t.Fatal("stock-only payload must produce no warnings")
	}
}

// TestWarningPathForIntakeSource pins the bounded metric-path vocabulary.
func TestWarningPathForIntakeSource(t *testing.T) {
	if got := warningPathForIntakeSource("canonical"); got != "api_v1_jobs" {
		t.Fatalf("canonical -> %q, want api_v1_jobs", got)
	}
	if got := warningPathForIntakeSource("batch"); got != "batch" {
		t.Fatalf("batch -> %q, want batch", got)
	}
	if got := warningPathForIntakeSource(""); got != "api_v1_jobs" {
		t.Fatalf("empty -> %q, want api_v1_jobs", got)
	}
}

// TestSummarizeWorkerPayloadForDryRun_Coverage pins the summary counts
// and the final-mix-vs-timeline gate preview: a short final mix must
// report short_final_mix_mux_would_fail (the failure behind the first
// Isabelle clipfix attempt, job_ab666a06d139d5f6).
func TestSummarizeWorkerPayloadForDryRun_Coverage(t *testing.T) {
	workerPayload := map[string]interface{}{
		"copy_only": true,
		"scenes_json": `[
			{"scene_id":"s-clip","kind":"clip","duration_seconds":6.36,"clip":{"url":"velox-drive://c","duration_ms":6360}},
			{"scene_id":"s-stock","kind":"stock","duration_seconds":5,"stock":[{"url":"velox-drive://s","duration_ms":5000}]}
		]`,
		"overlays":       []interface{}{map[string]interface{}{"id": "o1"}},
		"runtime_assets": []interface{}{map[string]interface{}{"asset_id": "a1"}},
		"runtime_audio":  map[string]interface{}{"voiceover_asset_id": "n", "voiceover_duration_seconds": 11.36},
	}
	raw := map[string]interface{}{"scenes_json": workerPayload["scenes_json"]}
	got := SummarizeWorkerPayloadForDryRun(workerPayload, raw)
	if got.Scenes != 2 || got.ClipScenes != 1 || got.StockScenes != 1 {
		t.Fatalf("summary = %+v, want 2 scenes / 1 clip / 1 stock", got)
	}
	if got.Overlays != 1 || got.RuntimeAssets != 1 || !got.RuntimeAudioPresent {
		t.Fatalf("summary = %+v, want overlays/runtime audio present", got)
	}
	if got.FinalMixOmitsClipAudio != 1 {
		t.Fatalf("final_mix_omits_clip_audio = %d, want 1", got.FinalMixOmitsClipAudio)
	}
	if got.AudioCoverage != DryRunAudioOK {
		t.Fatalf("audio_coverage = %q, want ok (11.36s mix covers 11.36s timeline)", got.AudioCoverage)
	}

	short := map[string]interface{}{
		"copy_only": true,
		"scenes_json": `[{"scene_id":"s","duration_seconds":1504.64,
			"stock":[{"url":"velox-drive://s","duration_ms":5000}]}]`,
		"runtime_audio": map[string]interface{}{
			"voiceover_asset_id": "n", "voiceover_duration_seconds": 1270.84,
		},
	}
	shortSummary := SummarizeWorkerPayloadForDryRun(short, short)
	if shortSummary.AudioCoverage != DryRunAudioShort {
		t.Fatalf("audio_coverage = %q, want %q", shortSummary.AudioCoverage, DryRunAudioShort)
	}
	if len(shortSummary.Warnings) == 0 {
		t.Fatal("short final mix must produce a warning")
	}

	silent := map[string]interface{}{
		"scenes_json": `[{"scene_id":"s","duration_seconds":5,
			"stock":[{"url":"velox-drive://s","duration_ms":5000}]}]`,
	}
	if got := SummarizeWorkerPayloadForDryRun(silent, silent); got.AudioCoverage != DryRunAudioSilent {
		t.Fatalf("audio_coverage = %q, want silent", got.AudioCoverage)
	}

	tracksOnly := map[string]interface{}{
		"scenes_json": `[{"scene_id":"s","duration_seconds":5,
			"clip":{"url":"velox-drive://c","duration_ms":5000}}]`,
	}
	mixed := SummarizeWorkerPayloadForDryRun(tracksOnly, tracksOnly)
	if mixed.AudioCoverage != DryRunAudioMixedTracks {
		t.Fatalf("audio_coverage = %q, want %q", mixed.AudioCoverage, DryRunAudioMixedTracks)
	}
	if mixed.FinalMixOmitsClipAudio != 0 {
		t.Fatalf("final_mix_omits_clip_audio = %d, want 0 without final mix", mixed.FinalMixOmitsClipAudio)
	}
}

// TestCreatorPush_KindClipWithoutClip_AcceptedWithWarnings is the intake
// half of the Isabelle regression: a kind="clip" scene without a clip
// asset is ACCEPTED (soft-deprecation) but the 202 envelope carries the
// warning so the generator can migrate before the sunset hard-rejection.
func TestCreatorPush_KindClipWithoutClip_AcceptedWithWarnings(t *testing.T) {
	h, db, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	body := creatorPushE2EBody("creator-warn-01", "creator-warn-job-001", "scene.composite.v1")
	payload := body["payload"].(map[string]interface{})
	payload["scenes"] = []interface{}{
		map[string]interface{}{
			"scene_id":         "scene-warn-62",
			"index":            62,
			"kind":             "clip",
			"text":             "Testimony without clip field",
			"duration_seconds": 6.36,
			"stock": map[string]interface{}{
				"asset_id": "cliprender_warn",
				"url":      "velox-drive://warn-testimony-file",
			},
		},
	}

	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202 (soft-deprecation accepts), got %d body=%s", w.Code, w.Body.String())
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	warnings, _ := decoded["warnings"].([]interface{})
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one", decoded["warnings"])
	}
	first, _ := warnings[0].(map[string]interface{})
	if first["code"] != sceneKindWarningCode {
		t.Fatalf("warning code = %v, want %q", first["code"], sceneKindWarningCode)
	}
	// Accepted means rows exist: warnings must never silently drop the job.
	jobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator-warn-01", "creator-warn-job-001", "scene.composite.v1").String(),
	)
	var jobCount int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, jobID).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("warned accept must still create the job row, got %d", jobCount)
	}
}

// TestCreatorPush_DryRun_CreatesNothing pins the dry-run contract: 200
// with the worker projection summary, zero forwarding/job/task rows.
func TestCreatorPush_DryRun_CreatesNothing(t *testing.T) {
	h, db, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	body := creatorPushE2EBody("creator-dry-01", "creator-dry-job-001", "scene.composite.v1")
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/creator/jobs?dry_run=true", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	var decoded struct {
		OK      bool `json:"ok"`
		DryRun  bool `json:"dry_run"`
		Summary struct {
			Scenes        int    `json:"scenes"`
			AudioCoverage string `json:"audio_coverage"`
			RuntimeAssets int    `json:"runtime_assets"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !decoded.OK || !decoded.DryRun {
		t.Fatalf("decoded = %+v, want ok+dry_run", decoded)
	}
	if decoded.Summary.Scenes != 1 {
		t.Fatalf("summary.scenes = %d, want 1", decoded.Summary.Scenes)
	}
	jobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator-dry-01", "creator-dry-job-001", "scene.composite.v1").String(),
	)
	for _, table := range []string{"jobs", "tasks"} {
		var n int
		col := "job_id"
		if err := db.DB().QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE `+col+` = ?`, jobID).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("dry-run must not create %s rows, got %d", table, n)
		}
	}
	var fwd int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ?`,
		"creator-dry-01", "creator-dry-job-001",
	).Scan(&fwd); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwd != 0 {
		t.Fatalf("dry-run must not create forwarding rows, got %d", fwd)
	}
}

// TestSubmitJob_DryRun_CreatesNothing mirrors the dry-run contract on
// POST /api/v1/jobs: validators + projection run, resolver never does.
func TestSubmitJob_DryRun_CreatesNothing(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	reqBody := validSubmitJobBody("dry-run-jobs-001")
	// Testimony-shaped scene: kind=clip, stock-only. Must pass every
	// validator (valid scheme URLs) and surface as a warning, not a
	// rejection — the dry-run reports what accept would do.
	reqBody.Scenes = []SubmitScene{{
		Text:            "Testimony without clip field",
		Kind:            "clip",
		DurationSeconds: 6.36,
		Stock:           &SubmitClip{URL: "velox-drive://warn-testimony-file"},
		Voiceover:       &SubmitVoiceover{URL: "velox-asset://voiceovers/opening.mp3"},
	}}
	raw, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs?dry_run=true", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer m2mJobsAuthFake-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	var decoded struct {
		OK      bool `json:"ok"`
		DryRun  bool `json:"dry_run"`
		Summary struct {
			Scenes              int      `json:"scenes"`
			KindClipWithoutClip []string `json:"kind_clip_without_clip"`
			AudioCoverage       string   `json:"audio_coverage"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !decoded.OK || !decoded.DryRun || decoded.Summary.Scenes != 1 {
		t.Fatalf("decoded = %+v, want ok+dry_run+1 scene", decoded)
	}
	if len(decoded.Summary.KindClipWithoutClip) != 1 {
		t.Fatalf("kind_clip_without_clip = %v, want the testimony scene flagged", decoded.Summary.KindClipWithoutClip)
	}
	jobID := expectedSubmitJobID("dry-run-jobs-001")
	if n := countRowsByJobID(t, db, "jobs", "job_id", jobID); n != 0 {
		t.Fatalf("dry-run must not create jobs rows, got %d", n)
	}
	var fwd int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ?`,
		"external_api", "dry-run-jobs-001",
	).Scan(&fwd); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwd != 0 {
		t.Fatalf("dry-run must not create forwarding rows, got %d", fwd)
	}
}
