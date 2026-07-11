// Package executors — contract test that codifies the canonical
// "items[].role = voiceover_bed | scene_clip" invariants for the
// RenderPlan.Timeline produced by the worker executor.
//
// Invariants under test:
//  1. items[] is the canonical timeline source. Its length drives
//     RenderPlan.Timeline length, not clips[] length.
//  2. items[i].role routes the URL: voiceover_bed must resolve a
//     "stock" media (scenes[].stock_link), scene_clip must resolve a
//     "final clip" media (scenes[].clip_link). A compiler that
//     reconstructs URLs from clips[] / stock_clip_paths violates
//     this contract.
//  3. items[i].duration drives Timeline[i].DurationSeconds.
//  4. audio_tracks[] map onto the runtime audio bus with
//     start_time_offset preserved per scene.
//
// Subtest "HonorsRoleBasedSceneLookup" is currently RED on main:
// today's hybrid.v1 compiler reads items[i].url verbatim, so when
// items[] entries omit "url" and rely on role + scenes-level
// stock_link / clip_link, the resulting Timeline URLs are empty.
// After the canonical-purity fix lands, that compiler must route the
// URL via role, and this assertion turns GREEN.
package executors

import (
	"context"
	"strings"
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/pipelines/hybrid"
	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
)

// hybridCompilerAdapter exposes the production hybrid.v1 compiler
// through the pipeline.Compiler interface. Mirrors registerPipelines
// (exported privately inside pkg/video) so contract tests can drive
// SceneComposite through the canonical compiler path.
type hybridCompilerAdapter struct{ probe audio.Probe }

func (a *hybridCompilerAdapter) ID() string { return "hybrid.v1" }
func (a *hybridCompilerAdapter) Validate(input map[string]interface{}) error {
	return hybrid.Validate(input)
}
func (a *hybridCompilerAdapter) Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string) (*plan.RenderPlan, error) {
	return hybrid.Compile(ctx, jobID, input, outputPath, a.probe)
}

// newItemsRoleContractWiring wires a SceneComposite executor backed by
// the production hybrid.v1 compiler, plus a recording render client.
func newItemsRoleContractWiring() (*SceneComposite, *fakeRenderClient) {
	reg := pipeline.NewRegistry()
	reg.Register(&hybridCompilerAdapter{probe: nil})
	rc := &fakeRenderClient{}
	runner := pipeline.NewRunner(reg, rc, logger.New(logger.InfoLevel, &strings.Builder{}))
	return NewSceneComposite(runner, "/tmp/velox/items-role-contract"), rc
}

// ── Contract test ───────────────────────────────────────────────────────────────

func TestSceneComposite_ItemsRoleContract(t *testing.T) {

	// Subtest 1 — defensive contract: with explicit per-item URLs and
	// durations, the timeline mirrors items[] order, length equals
	// len(items[]), and clips[] does not inflate. Today this passes
	// (the compiler already reads items[].url/duration verbatim), but
	// it locks the contract so any regression to clips[] fallback is
	// caught by CI rather than by users.
	t.Run("TranslatesRolesToSequentialTimeline", func(t *testing.T) {
		exec, rc := newItemsRoleContractWiring()

		spec := executor.TaskSpec{
			Version:    1,
			JobID:      "items-role-tl",
			ExecutorID: SceneCompositeID,
			Payload: map[string]interface{}{
				"pipeline_id": "hybrid.v1",
				"video_mode":  "clip_stock",
				"items": []interface{}{
					map[string]interface{}{
						"type":     "video",
						"url":      "s1-stock.mp4",
						"duration": 5.5,
						"fit":      "contain",
						"role":     "voiceover_bed",
					},
					map[string]interface{}{
						"type":     "video",
						"url":      "s1-clip.mp4",
						"duration": 4.0,
						"fit":      "contain",
						"role":     "scene_clip",
					},
					map[string]interface{}{
						"type":     "video",
						"url":      "s2-stock.mp4",
						"duration": 6.5,
						"fit":      "contain",
						"role":     "voiceover_bed",
					},
					map[string]interface{}{
						"type":     "video",
						"url":      "s2-clip.mp4",
						"duration": 3.0,
						"fit":      "contain",
						"role":     "scene_clip",
					},
				},
				"audio_tracks": []interface{}{
					map[string]interface{}{
						"source_url":        "vo1.mp3",
						"volume":            1.0,
						"start_time_offset": 0.0,
					},
					map[string]interface{}{
						"source_url":        "vo2.mp3",
						"volume":            1.0,
						"start_time_offset": 9.5,
					},
				},
				"clips": []interface{}{"s1-stock.mp4", "s1-clip.mp4", "s2-stock.mp4", "s2-clip.mp4"},
			},
		}

		res, err := exec.Execute(context.Background(), nil, spec)
		if err != nil {
			t.Fatalf("Execute err = %v, want nil (failure must live in res)", err)
		}
		if res.Status != "succeeded" {
			t.Fatalf("Execute status = %q, want succeeded (code=%q detail=%q)",
				res.Status, res.ErrorCode, res.ErrorDetail)
		}
		if !rc.called || rc.lastPlan == nil {
			t.Fatalf("RenderClient.Render was not invoked")
		}
		p := rc.lastPlan

		if got := len(p.Timeline); got != 4 {
			t.Errorf("Timeline length = %d, want 4 (= len(items[]))", got)
		}

		expected := []struct {
			url      string
			duration float64
		}{
			{"s1-stock.mp4", 5.5},
			{"s1-clip.mp4", 4.0},
			{"s2-stock.mp4", 6.5},
			{"s2-clip.mp4", 3.0},
		}
		for i, want := range expected {
			if i >= len(p.Timeline) {
				t.Errorf("Timeline[%d] missing", i)
				continue
			}
			got := p.Timeline[i]
			if got.Source.URL != want.url {
				t.Errorf("Timeline[%d].Source.URL = %q, want %q (role ordering)", i, got.Source.URL, want.url)
			}
			if got.DurationSeconds != want.duration {
				t.Errorf("Timeline[%d].DurationSeconds = %v, want %v", i, got.DurationSeconds, want.duration)
			}
			if got.Source.Type != "video" {
				t.Errorf("Timeline[%d].Source.Type = %q, want video", i, got.Source.Type)
			}
		}

		// Defensive check against the diagnosed visual bug: voiceover_bed
		// and scene_clip per scene must NOT resolve to the same URL (the
		// upstream payload builder could collapse them).
		for i := 0; i+1 < len(p.Timeline) && (i+1)%2 == 1; i += 2 {
			stock := p.Timeline[i].Source.URL
			clip := p.Timeline[i+1].Source.URL
			if stock != "" && clip != "" && stock == clip {
				t.Errorf("Segments %d (voiceover_bed) and %d (scene_clip) share URL %q: scene-level fallbacks collapsed stock and clip",
					i, i+1, stock)
			}
		}

		if got := len(p.AudioTracks); got != 2 {
			t.Errorf("AudioTracks length = %d, want 2", got)
			return
		}
		if got := p.AudioTracks[0].SourceURL; got != "vo1.mp3" {
			t.Errorf("AudioTracks[0].SourceURL = %q, want vo1.mp3", got)
		}
		if got := p.AudioTracks[0].StartTimeOffset; got != 0.0 {
			t.Errorf("AudioTracks[0].StartTimeOffset = %v, want 0.0", got)
		}
		if got := p.AudioTracks[1].SourceURL; got != "vo2.mp3" {
			t.Errorf("AudioTracks[1].SourceURL = %q, want vo2.mp3", got)
		}
		// The second voiceover must begin AFTER the first scene's timeline
		// duration (5.5 + 4.0 = 9.5). Accept a small tolerance to stay
		// robust against residue from runtime offset recomputation.
		if got := p.AudioTracks[1].StartTimeOffset; got < 9.5 || got > 10.0 {
			t.Errorf("AudioTracks[1].StartTimeOffset = %v, want in [9.5, 10.0)", got)
		}
	})

	// Subtest 2 — defensive length guarantee: 4 items + 20-element
	// legacy clips[] must yield a length-4 Timeline.
	t.Run("IgnoresLegacyClipsArrayLength", func(t *testing.T) {
		exec, rc := newItemsRoleContractWiring()

		var items []interface{}
		for i := 0; i < 4; i++ {
			role := "voiceover_bed"
			if i%2 == 1 {
				role = "scene_clip"
			}
			items = append(items, map[string]interface{}{
				"type":     "video",
				"url":      "scene-" + string(rune('a'+i)) + ".mp4",
				"duration": 3.0,
				"fit":      "contain",
				"role":     role,
			})
		}
		var legacy []interface{}
		for i := 0; i < 20; i++ {
			legacy = append(legacy, "legacy-"+string(rune('a'+i%26))+string(rune('a'+i/26))+".mp4")
		}

		spec := executor.TaskSpec{
			Version:    1,
			JobID:      "items-role-ignore-clips",
			ExecutorID: SceneCompositeID,
			Payload: map[string]interface{}{
				"pipeline_id": "hybrid.v1",
				"video_mode":  "clip_stock",
				"items":       items,
				"clips":       legacy,
			},
		}

		res, err := exec.Execute(context.Background(), nil, spec)
		if err != nil {
			t.Fatalf("Execute err = %v", err)
		}
		if res.Status != "succeeded" {
			t.Fatalf("Execute status = %q, want succeeded (code=%q detail=%q)",
				res.Status, res.ErrorCode, res.ErrorDetail)
		}
		p := rc.lastPlan
		if p == nil {
			t.Fatalf("RenderClient captured nil plan")
		}
		if got := len(p.Timeline); got != 4 {
			t.Errorf("Timeline length = %d, want 4. items[]=4 clips[]=20 → clips[] must not be expanded", got)
		}
	})

	// Subtest 3 — RED today, GREEN after the canonical-purity fix.
	// items[] entries deliberately omit "url" and expose only "role"
	// plus a "scene" reference. The compiler must route the URL via
	// role + scenes[].voiceover_bed URL / scenes[].scene_clip URL.
	// Today the compiler reads item.url="" → Timeline[i].Source.URL=""
	// and the assertion fails. After Step 2 the compiler honours the
	// role and produces the canonical 4-segment layout.
	t.Run("HonorsRoleBasedSceneLookup", func(t *testing.T) {
		exec, rc := newItemsRoleContractWiring()

		spec := executor.TaskSpec{
			Version:    1,
			JobID:      "items-role-lookup",
			ExecutorID: SceneCompositeID,
			Payload: map[string]interface{}{
				"pipeline_id": "hybrid.v1",
				"video_mode":  "clip_stock",
				"scenes": []interface{}{
					map[string]interface{}{
						"stock_link":                  "s1-stock.mp4",
						"clip_link":                   "s1-clip.mp4",
						"voiceover_link":              "vo1.mp3",
						"voiceover_duration_seconds":  5.5,
						"final_clip_duration_seconds": 4.0,
					},
					map[string]interface{}{
						"stock_link":                  "s2-stock.mp4",
						"clip_link":                   "s2-clip.mp4",
						"voiceover_link":              "vo2.mp3",
						"voiceover_duration_seconds":  6.5,
						"final_clip_duration_seconds": 3.0,
					},
				},
				"items": []interface{}{
					// Scena 1 entries — no "url"; the compiler must look up
					// stock_link for role=voiceover_bed, clip_link for
					// role=scene_clip.
					map[string]interface{}{
						"type":     "video",
						"duration": 5.5,
						"fit":      "contain",
						"role":     "voiceover_bed",
						"scene":    0,
					},
					map[string]interface{}{
						"type":     "video",
						"duration": 4.0,
						"fit":      "contain",
						"role":     "scene_clip",
						"scene":    0,
					},
					// Scena 2 entries — same contract.
					map[string]interface{}{
						"type":     "video",
						"duration": 6.5,
						"fit":      "contain",
						"role":     "voiceover_bed",
						"scene":    1,
					},
					map[string]interface{}{
						"type":     "video",
						"duration": 3.0,
						"fit":      "contain",
						"role":     "scene_clip",
						"scene":    1,
					},
				},
			},
		}

		res, err := exec.Execute(context.Background(), nil, spec)
		if err != nil {
			t.Fatalf("Execute err = %v", err)
		}
		if res.Status != "succeeded" {
			t.Fatalf("Execute status = %q, want succeeded (code=%q detail=%q)",
				res.Status, res.ErrorCode, res.ErrorDetail)
		}
		p := rc.lastPlan
		if p == nil {
			t.Fatalf("RenderClient captured nil plan")
		}

		if got := len(p.Timeline); got != 4 {
			t.Errorf("Timeline length = %d, want 4", got)
		}

		expected := []struct {
			url      string
			duration float64
		}{
			{"s1-stock.mp4", 5.5},
			{"s1-clip.mp4", 4.0},
			{"s2-stock.mp4", 6.5},
			{"s2-clip.mp4", 3.0},
		}
		for i, want := range expected {
			if i >= len(p.Timeline) {
				t.Errorf("Timeline[%d] missing", i)
				continue
			}
			got := p.Timeline[i]
			if got.Source.URL != want.url {
				t.Errorf("Timeline[%d].Source.URL = %q, want %q (compiler did not route via role; today it reads items[i].url which is missing here)", i, got.Source.URL, want.url)
			}
			if got.DurationSeconds != want.duration {
				t.Errorf("Timeline[%d].DurationSeconds = %v, want %v", i, got.DurationSeconds, want.duration)
			}
		}
	})
}
