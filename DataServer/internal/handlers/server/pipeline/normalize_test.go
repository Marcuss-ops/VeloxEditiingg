// Package pipeline / normalize_test.go
//
// Per-scene enrichment normalization tests (Phase 2 of the
// render-manifest plan). The headlining test is
// TestNormalizeExternalJobSubmission_PerSceneVoiceoverNotPositionCoupled,
// which locks the central promise of the per-scene voiceover
// enrichment: a scene that carries its own voiceover.url in the
// nested form produces a worker payload where the voiceover URL is
// reachable through scenes_json[i].voiceover.url AND through the
// top-level voiceover_paths[] (merged by ToWorkerPayload), NOT
// through positional coupling with a global voiceover_paths array.
//
// The positional-coupling failure mode this guards against: a
// client that supplies scene[N].voiceover.url but no top-level
// voiceover_paths[] would, under the legacy contract, fall back to
// "voiceover[i] = voiceover_paths[i]" semantics — silently
// re-mapping a scene's voiceover to a different scene's voiceover
// when scenes were reordered. The new contract REJECTS that
// re-mapping: per-scene voiceover is authoritative.
package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestNormalizeExternalJobSubmission_PerSceneVoiceoverNotPositionCoupled
// is the headlining test of the per-scene voiceover enrichment
// (Phase 2 of the render-manifest plan). The contract it pins:
//
//   - A SubmitJobRequest with scenes[i].voiceover.url populated
//     (the per-scene nested form) and NO top-level voiceover_paths
//     MUST produce a CanonicalCompletedPayload where:
//     1. worker_payload["scenes_json"] is a JSON string whose
//        parsed [i].voiceover.url equals the per-scene URL — the
//        worker's authoritative source.
//     2. worker_payload["voiceover_paths"] is non-empty (the merged
//        back-compat array populated by ToWorkerPayload from the
//        per-scene URLs) — legacy workers that still read the
//        top-level array see the right URL.
//     3. NO positional re-mapping: a scene at index 0 carrying
//        voiceover_a must produce voiceover_paths[0] = voiceover_a,
//        even if the request originally supplied voiceover_paths
//        with different content.
//
//   - A SubmitJobRequest with NEITHER per-scene voiceover NOR
//     top-level voiceover_paths MUST produce a CanonicalCompletedPayload
//     with voiceover_paths = nil (NOT a placeholder or empty array).
//
//   - A SubmitJobRequest with BOTH per-scene voiceover AND
//     top-level voiceover_paths (legacy client that hasn't
//     migrated) MUST produce a merged voiceover_paths[] that
//     preserves BOTH sources, with per-scene URLs FIRST
//     (authoritative) and top-level URLs SECOND (legacy).
func TestNormalizeExternalJobSubmission_PerSceneVoiceoverNotPositionCoupled(t *testing.T) {
	t.Parallel()

	const (
		voiceoverScene0 = "https://drive.example.com/voiceover-scene-0.mp3"
		voiceoverScene1 = "https://drive.example.com/voiceover-scene-1.mp3"
		idemKey        = "per-scene-voiceover-001"
	)

	t.Run("per_scene_nested_only", func(t *testing.T) {
		t.Parallel()

		req := SubmitJobRequest{
			IdempotencyKey: idemKey,
			// Deliberately NO top-level voiceover_paths: the entire
			// voiceover surface lives in scenes[i].voiceover.
			VoiceoverPaths: nil,
			Scenes: []SubmitScene{
				{
					Text:            "scene 0 narration",
					SceneID:         "scene-0",
					Index:           0,
					DurationSeconds: 7,
					Voiceover: &SubmitVoiceover{
						AssetID:    "voiceover_scene_0",
						URL:        voiceoverScene0,
						DurationMS: 7000,
						Language:   "en",
					},
				},
				{
					Text:            "scene 1 narration",
					SceneID:         "scene-1",
					Index:           1,
					DurationSeconds: 8,
					Voiceover: &SubmitVoiceover{
						AssetID:    "voiceover_scene_1",
						URL:        voiceoverScene1,
						DurationMS: 8000,
						Language:   "en",
					},
				},
			},
		}

		canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
		if canonical == nil {
			t.Fatal("NormalizeExternalJobSubmission returned nil")
		}
		wp := canonical.WorkerPayload

		// (1) scenes_json carries the per-scene voiceover.url
		// (authoritative source for new workers).
		scenesJSONRaw, ok := wp["scenes_json"].(string)
		if !ok || scenesJSONRaw == "" {
			t.Fatalf("worker_payload[scenes_json] missing or empty: %v", wp["scenes_json"])
		}
		var scenesFromJSON []map[string]interface{}
		if err := json.Unmarshal([]byte(scenesJSONRaw), &scenesFromJSON); err != nil {
			t.Fatalf("scenes_json unmarshal: %v", err)
		}
		if len(scenesFromJSON) != 2 {
			t.Fatalf("scenes_json: got %d scenes, want 2", len(scenesFromJSON))
		}
		vo0, ok := scenesFromJSON[0]["voiceover"].(map[string]interface{})
		if !ok {
			t.Fatalf("scenes_json[0].voiceover missing or wrong type: %+v", scenesFromJSON[0])
		}
		if vo0["url"] != voiceoverScene0 {
			t.Errorf("scenes_json[0].voiceover.url = %v, want %s", vo0["url"], voiceoverScene0)
		}
		vo1, ok := scenesFromJSON[1]["voiceover"].(map[string]interface{})
		if !ok {
			t.Fatalf("scenes_json[1].voiceover missing or wrong type: %+v", scenesFromJSON[1])
		}
		if vo1["url"] != voiceoverScene1 {
			t.Errorf("scenes_json[1].voiceover.url = %v, want %s", vo1["url"], voiceoverScene1)
		}

		// (2) voiceover_paths[] is populated by ToWorkerPayload's
		// merge strategy (per-scene URLs first). Legacy workers
		// that still read this top-level array see the per-scene
		// URLs in the same order they appear in scenes[].
		voiceoverPaths, ok := wp["voiceover_paths"].([]string)
		if !ok {
			t.Fatalf("worker_payload[voiceover_paths] missing or wrong type: %T (%v)", wp["voiceover_paths"], wp["voiceover_paths"])
		}
		if len(voiceoverPaths) != 2 {
			t.Fatalf("voiceover_paths: got %d entries, want 2: %v", len(voiceoverPaths), voiceoverPaths)
		}
		if voiceoverPaths[0] != voiceoverScene0 {
			t.Errorf("voiceover_paths[0] = %q, want %q (NOT positional remap)", voiceoverPaths[0], voiceoverScene0)
		}
		if voiceoverPaths[1] != voiceoverScene1 {
			t.Errorf("voiceover_paths[1] = %q, want %q (NOT positional remap)", voiceoverPaths[1], voiceoverScene1)
		}
	})

	t.Run("per_scene_nested_with_legacy_top_level_merged", func(t *testing.T) {
		t.Parallel()

		// Legacy client that supplies BOTH: voiceover_paths[]
		// (top-level) + per-scene nested voiceover. The merge
		// strategy preserves BOTH: per-scene URLs FIRST
		// (authoritative), top-level URLs SECOND (legacy).
		const topLevelURL = "https://legacy.example.com/voiceover-global.mp3"
		req := SubmitJobRequest{
			IdempotencyKey: "per-scene-with-legacy-001",
			VoiceoverPaths: []string{topLevelURL},
			Scenes: []SubmitScene{
				{
					Text:            "scene with per-scene voiceover",
					DurationSeconds: 5,
					Voiceover: &SubmitVoiceover{
						URL: voiceoverScene0,
					},
				},
			},
		}

		canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
		wp := canonical.WorkerPayload

		voiceoverPaths, ok := wp["voiceover_paths"].([]string)
		if !ok {
			t.Fatalf("worker_payload[voiceover_paths] missing or wrong type")
		}
		// Expect [per-scene URL first, top-level URL second]
		if len(voiceoverPaths) != 2 {
			t.Fatalf("voiceover_paths: got %d entries %v, want 2", len(voiceoverPaths), voiceoverPaths)
		}
		if voiceoverPaths[0] != voiceoverScene0 {
			t.Errorf("voiceover_paths[0] = %q, want per-scene URL %q (authoritative first)", voiceoverPaths[0], voiceoverScene0)
		}
		if voiceoverPaths[1] != topLevelURL {
			t.Errorf("voiceover_paths[1] = %q, want top-level URL %q (legacy second)", voiceoverPaths[1], topLevelURL)
		}
	})

	t.Run("no_voiceover_anywhere_produces_no_voiceover_paths", func(t *testing.T) {
		t.Parallel()

		req := SubmitJobRequest{
			IdempotencyKey: "no-voiceover-001",
			// Neither top-level nor per-scene voiceover.
			VoiceoverPaths: nil,
			Scenes: []SubmitScene{
				{Text: "scene", DurationSeconds: 5},
			},
		}

		canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
		wp := canonical.WorkerPayload

		// voiceover_paths must be ABSENT (not present with empty
		// array) — the canonical "no voiceover anywhere" path.
		if vp, present := wp["voiceover_paths"]; present {
			t.Errorf("worker_payload[voiceover_paths] must be ABSENT, got: %v", vp)
		}
	})

	t.Run("per_scene_voiceover_position_remap_does_not_occur", func(t *testing.T) {
		t.Parallel()

		// Anti-regression test: the legacy contract would re-map
		// scenes[i].voiceover to voiceover_paths[i] when the
		// per-scene field was absent. Under the per-scene
		// enrichment, that re-mapping is FORBIDDEN — a scene's
		// voiceover URL is exactly the per-scene URL, NOT
		// voiceover_paths[i].
		//
		// Setup: voiceover_paths = [wrong_a, wrong_b] (the
		// top-level array has DIFFERENT URLs than the per-scene
		// nested form). Under positional remapping, scene[0] would
		// end up with wrong_a. Under per-scene enrichment, scene[0]
		// MUST end up with right_a.
		const (
			rightA = "https://right.example.com/scene-0.mp3"
			rightB = "https://right.example.com/scene-1.mp3"
			wrongA = "https://wrong.example.com/voiceover-a.mp3"
			wrongB = "https://wrong.example.com/voiceover-b.mp3"
		)
		req := SubmitJobRequest{
			IdempotencyKey: "anti-remap-001",
			VoiceoverPaths: []string{wrongA, wrongB},
			Scenes: []SubmitScene{
				{
					Text:            "scene 0 with per-scene voiceover",
					DurationSeconds: 5,
					Voiceover:      &SubmitVoiceover{URL: rightA},
				},
				{
					Text:            "scene 1 with per-scene voiceover",
					DurationSeconds: 7,
					Voiceover:      &SubmitVoiceover{URL: rightB},
				},
			},
		}

		canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
		wp := canonical.WorkerPayload

		// scenes_json[i].voiceover.url MUST be the per-scene URL,
		// NOT a positional re-map from voiceover_paths[i].
		scenesJSONRaw := wp["scenes_json"].(string)
		var scenesFromJSON []map[string]interface{}
		if err := json.Unmarshal([]byte(scenesJSONRaw), &scenesFromJSON); err != nil {
			t.Fatalf("scenes_json unmarshal: %v", err)
		}
		if len(scenesFromJSON) != 2 {
			t.Fatalf("scenes_json: got %d scenes, want 2", len(scenesFromJSON))
		}
		vo0 := scenesFromJSON[0]["voiceover"].(map[string]interface{})
		if vo0["url"] != rightA {
			t.Errorf("scenes_json[0].voiceover.url = %v, want %s (per-scene authoritative, NOT positional remap to %s)",
				vo0["url"], rightA, wrongA)
		}
		if strings.Contains(scenesJSONRaw, wrongA) || strings.Contains(scenesJSONRaw, wrongB) {
			t.Errorf("scenes_json MUST NOT contain wrong URLs from positional remap: %s", scenesJSONRaw)
		}

		// voiceover_paths[] is the merged array — per-scene URLs
		// first (authoritative), top-level URLs second.
		voiceoverPaths := wp["voiceover_paths"].([]string)
		// Expect [rightA, rightB, wrongA, wrongB] (4 entries, deduped
		// — none of these URLs collide).
		if len(voiceoverPaths) != 4 {
			t.Fatalf("voiceover_paths: got %d entries %v, want 4 (per-scene first, top-level second)",
				len(voiceoverPaths), voiceoverPaths)
		}
		if voiceoverPaths[0] != rightA || voiceoverPaths[1] != rightB {
			t.Errorf("voiceover_paths[0..1] = %v, want [%s %s] (per-scene authoritative first)",
				voiceoverPaths[:2], rightA, rightB)
		}
		if voiceoverPaths[2] != wrongA || voiceoverPaths[3] != wrongB {
			t.Errorf("voiceover_paths[2..3] = %v, want [%s %s] (legacy second)",
				voiceoverPaths[2:], wrongA, wrongB)
		}
	})
}

// TestNormalizeExternalJobSubmission_PerSceneClipAndSubtitlesRoundtrip
// verifies that the per-scene Clip and Subtitles nested objects
// also roundtrip into the worker payload (no positional coupling
// surface for these — they were never positional, but the test
// pins the absence of accidental coupling for symmetry with the
// voiceover case).
func TestNormalizeExternalJobSubmission_PerSceneClipAndSubtitlesRoundtrip(t *testing.T) {
	t.Parallel()

	const (
		clipURL  = "https://drive.example.com/clip-scene-0.mp4"
		subURL   = "https://drive.example.com/subtitle-scene-0.ass"
		idemKey  = "per-scene-clip-sub-001"
		durMS    = 7000
		hashClip = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)

	req := SubmitJobRequest{
		IdempotencyKey: idemKey,
		Scenes: []SubmitScene{
			{
				Text:            "scene with clip + subtitles",
				DurationSeconds: 7,
				Clip: &SubmitClip{
					URL:        clipURL,
					SHA256:     hashClip,
					StartMS:    0,
					EndMS:      durMS,
					DurationMS: durMS,
				},
				Subtitles: &SubmitSubtitles{
					Format:   "ass",
					URL:      subURL,
					Language: "en",
				},
			},
		},
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}
	wp := canonical.WorkerPayload

	scenesJSONRaw, ok := wp["scenes_json"].(string)
	if !ok || scenesJSONRaw == "" {
		t.Fatalf("worker_payload[scenes_json] missing or empty")
	}
	var scenesFromJSON []map[string]interface{}
	if err := json.Unmarshal([]byte(scenesJSONRaw), &scenesFromJSON); err != nil {
		t.Fatalf("scenes_json unmarshal: %v", err)
	}
	if len(scenesFromJSON) != 1 {
		t.Fatalf("scenes_json: got %d scenes, want 1", len(scenesFromJSON))
	}
	clip, ok := scenesFromJSON[0]["clip"].(map[string]interface{})
	if !ok {
		t.Fatalf("scenes_json[0].clip missing or wrong type")
	}
	if clip["url"] != clipURL {
		t.Errorf("scenes_json[0].clip.url = %v, want %s", clip["url"], clipURL)
	}
	if clip["duration_ms"].(float64) != durMS {
		t.Errorf("scenes_json[0].clip.duration_ms = %v, want %d", clip["duration_ms"], durMS)
	}
	subs, ok := scenesFromJSON[0]["subtitles"].(map[string]interface{})
	if !ok {
		t.Fatalf("scenes_json[0].subtitles missing or wrong type")
	}
	if subs["url"] != subURL {
		t.Errorf("scenes_json[0].subtitles.url = %v, want %s", subs["url"], subURL)
	}
	if subs["format"] != "ass" {
		t.Errorf("scenes_json[0].subtitles.format = %v, want ass", subs["format"])
	}
}
