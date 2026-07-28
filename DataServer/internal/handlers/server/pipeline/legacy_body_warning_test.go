package pipeline

import (
	"sync"
	"testing"
)

// recordingLegacyBodySink is a test mock that records every
// IncLegacyBody call. Used to verify the legacy-body-shape warning
// emission locks exactly the (client_kind) tuples that fired
// without relying on a real Prometheus collector.
type recordingLegacyBodySink struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingLegacyBodySink) IncLegacyBody(client_kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, client_kind)
}

func (r *recordingLegacyBodySink) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// TestWithLegacyBodySink_WiredSink verifies the composition-root
// mutator stores the supplied sink so the handler's observation
// point (NormalizeExternalJobSubmission) routes to it.
func TestWithLegacyBodySink_WiredSink(t *testing.T) {
	t.Parallel()
	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(sink)

	// Must not panic.
	h.legacyBodySinkOrNoop().IncLegacyBody(LegacyBodySinkClientKindPreManifestRef)

	calls := sink.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d (%v)", len(calls), calls)
	}
	if calls[0] != LegacyBodySinkClientKindPreManifestRef {
		t.Fatalf("expected %q, got %q",
			LegacyBodySinkClientKindPreManifestRef, calls[0])
	}
}

// TestWithLegacyBodySink_NilSinkFallsBackToNoop verifies that a
// Handlers without a wired sink falls back to noopLegacyBodySink —
// does not panic, does not call any recording sink.
func TestWithLegacyBodySink_NilSinkFallsBackToNoop(t *testing.T) {
	t.Parallel()
	h := &Handlers{}

	// Must not panic on either call.
	h.legacyBodySinkOrNoop().IncLegacyBody(LegacyBodySinkClientKindPreManifestRef)
	h.legacyBodySinkOrNoop().IncLegacyBody(LegacyBodySinkClientKindPreManifestRef)
}

// TestWithLegacyBodySink_ExplicitNil verifies that passing nil to
// WithLegacyBodySink is a noop (the handler still falls back to noop).
func TestWithLegacyBodySink_ExplicitNil(t *testing.T) {
	t.Parallel()
	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(nil) // explicit nil
	h.WithLegacyBodySink(sink)

	h.legacyBodySinkOrNoop().IncLegacyBody(LegacyBodySinkClientKindPreManifestRef)

	if got := sink.Calls(); len(got) != 1 {
		t.Fatalf("expected 1 call after explicit-nil + re-wire, got %d (%v)", len(got), got)
	}
}

// TestIsLegacyCompatShape pins the detection criteria for the
// pre-manifest_ref compat body shape. The function is a pure
// helper; this test calls it directly with a synthetic SubmitJobRequest
// to lock each detection branch and the combination-of-branches
// happy path.
//
// Criteria (any of):
//   - len(VoiceoverPaths) > 0
//   - any Scene.ClipLink non-empty after trim
//   - len(SubtitleTracks) > 0
func TestIsLegacyCompatShape(t *testing.T) {
	t.Parallel()

	t.Run("empty_body_returns_false", func(t *testing.T) {
		t.Parallel()
		if isLegacyCompatShape(SubmitJobRequest{}) {
			t.Fatal("empty body must NOT be legacy compat shape")
		}
	})

	t.Run("voiceover_paths_alone_triggers", func(t *testing.T) {
		t.Parallel()
		req := SubmitJobRequest{VoiceoverPaths: []string{"https://x/y.mp3"}}
		if !isLegacyCompatShape(req) {
			t.Fatal("voiceover_paths alone must trigger legacy detection")
		}
	})

	t.Run("scene_clip_link_alone_triggers", func(t *testing.T) {
		t.Parallel()
		req := SubmitJobRequest{
			Scenes: []SubmitScene{
				{Text: "s", ClipLink: "https://x/y.mp4"},
			},
		}
		if !isLegacyCompatShape(req) {
			t.Fatal("scene.clip_link alone must trigger legacy detection")
		}
	})

	t.Run("scene_clip_link_whitespace_only_does_not_trigger", func(t *testing.T) {
		t.Parallel()
		req := SubmitJobRequest{
			Scenes: []SubmitScene{
				{Text: "s", ClipLink: "   "}, // trim → empty
			},
		}
		if isLegacyCompatShape(req) {
			t.Fatal("whitespace-only clip_link must NOT trigger (trim policy)")
		}
	})

	t.Run("subtitle_tracks_alone_triggers", func(t *testing.T) {
		t.Parallel()
		req := SubmitJobRequest{
			SubtitleTracks: []SubmitSubtitleTrack{{Source: "https://x/sub.ass"}},
		}
		if !isLegacyCompatShape(req) {
			t.Fatal("subtitle_tracks alone must trigger legacy detection")
		}
	})

	t.Run("all_three_together_triggers_once", func(t *testing.T) {
		t.Parallel()
		// Combination: any-of means the function returns true
		// even when all three are present. This test pins that
		// the function does NOT short-circuit on the first
		// match in a way that misses later fields (a future
		// refactor that returned early on the first match would
		// still pass this test; the function is purely boolean).
		req := SubmitJobRequest{
			VoiceoverPaths: []string{"https://x/y.mp3"},
			SubtitleTracks: []SubmitSubtitleTrack{{Source: "https://x/sub.ass"}},
			Scenes: []SubmitScene{
				{Text: "s", ClipLink: "https://x/y.mp4"},
			},
		}
		if !isLegacyCompatShape(req) {
			t.Fatal("combined legacy signals must trigger")
		}
	})

	t.Run("scene_with_nested_clip_does_not_trigger", func(t *testing.T) {
		t.Parallel()
		// The migration target: scene with the new nested
		// Clip{}/Voiceover{}/Subtitles{} objects but NO legacy
		// flat fields. This is NOT a legacy-shape signal —
		// the per-scene enrichment IS the migration.
		req := SubmitJobRequest{
			Scenes: []SubmitScene{
				{
					Text: "s",
					Clip: &SubmitClip{
						URL: "https://x/y.mp4",
					},
				},
			},
		}
		if isLegacyCompatShape(req) {
			t.Fatal("scene with nested Clip{} (migration target) must NOT trigger legacy detection")
		}
	})
}

// TestCountScenesWithClipLink pins the per-scene distribution
// helper used by the legacy-body-shape warning log line. The
// helper counts scenes whose flat `clip_link` field is non-empty
// after trim; operators read the count in the log line to see
// the per-scene distribution without grepping every scene.
func TestCountScenesWithClipLink(t *testing.T) {
	t.Parallel()

	t.Run("nil_returns_zero", func(t *testing.T) {
		t.Parallel()
		if got := countScenesWithClipLink(nil); got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("empty_returns_zero", func(t *testing.T) {
		t.Parallel()
		if got := countScenesWithClipLink([]SubmitScene{}); got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("mix_of_present_and_absent", func(t *testing.T) {
		t.Parallel()
		scenes := []SubmitScene{
			{Text: "a", ClipLink: "https://x/1.mp4"},
			{Text: "b"}, // absent
			{Text: "c", ClipLink: "https://x/2.mp4"},
			{Text: "d", ClipLink: "   "}, // whitespace → not counted
			{Text: "e", ClipLink: "https://x/3.mp4"},
		}
		if got := countScenesWithClipLink(scenes); got != 3 {
			t.Errorf("got %d, want 3 (whitespace-only is excluded)", got)
		}
	})
}

// TestNormalizeExternalJobSubmission_LegacyBodyEmitsWarning pins
// the integration contract: a body that carries the legacy compat
// shape AND no manifest_ref MUST emit one IncLegacyBody call with
// client_kind = pipelinegen_pre_manifest_ref, AND the canonical
// payload must STILL be returned unchanged (non-blocking). This is
// the headlining assertion of the feature.
func TestNormalizeExternalJobSubmission_LegacyBodyEmitsWarning(t *testing.T) {
	t.Parallel()

	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(sink)

	req := SubmitJobRequest{
		IdempotencyKey: "legacy-warn-001",
		// Deliberately no manifest_ref; this is the legacy
		// compat body shape with BOTH voiceover_paths AND
		// scenes[N].clip_link AND subtitle_tracks present.
		VoiceoverPaths: []string{"https://x/y.mp3"},
		SubtitleTracks: []SubmitSubtitleTrack{{Source: "https://x/sub.ass"}},
		Scenes: []SubmitScene{
			{Text: "s", ClipLink: "https://x/y.mp4", DurationSeconds: 5},
		},
	}

	canonical := h.NormalizeExternalJobSubmission(req)

	// (1) Sink received exactly one IncLegacyBody call with the
	// bounded client_kind enum value.
	calls := sink.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 IncLegacyBody call, got %d (%v)", len(calls), calls)
	}
	if calls[0] != LegacyBodySinkClientKindPreManifestRef {
		t.Errorf("client_kind = %q, want %q",
			calls[0], LegacyBodySinkClientKindPreManifestRef)
	}

	// (2) The canonical payload is still returned unchanged —
	// the warning is NON-BLOCKING. This is the canonical
	// operator-visible contract: legacy clients continue to
	// work until they migrate.
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil (warning must NOT block submission)")
	}
	if canonical.SourceProvider != ExternalAPISourceProvider {
		t.Errorf("SourceProvider = %q, want %q", canonical.SourceProvider, ExternalAPISourceProvider)
	}
	if canonical.SourceJobID != "legacy-warn-001" {
		t.Errorf("SourceJobID = %q, want legacy-warn-001", canonical.SourceJobID)
	}
	if canonical.WorkerPayload == nil {
		t.Fatal("WorkerPayload is nil; submission was supposed to pass through unchanged")
	}
}

// TestNormalizeExternalJobSubmission_ManifestRefSuppressedWarning
// pins the migration-path assertion: a client that supplies the
// legacy compat shape AND a manifest_ref (i.e., it has migrated)
// MUST NOT emit the warning — the resolver will use the manifest
// side, so the compat-shape signals are not "legacy" anymore.
func TestNormalizeExternalJobSubmission_ManifestRefSuppressedWarning(t *testing.T) {
	t.Parallel()

	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(sink)

	req := SubmitJobRequest{
		IdempotencyKey: "manifest-ref-001",
		// ALL three legacy signals present...
		VoiceoverPaths: []string{"https://x/y.mp3"},
		SubtitleTracks: []SubmitSubtitleTrack{{Source: "https://x/sub.ass"}},
		Scenes: []SubmitScene{
			{Text: "s", ClipLink: "https://x/y.mp4", DurationSeconds: 5},
		},
		// ...PLUS a manifest_ref: this is the migrated path.
		ManifestRef: &SubmitManifestRef{
			SchemaVersion: "velox.render-manifest.v1",
			URL:           "https://drive.example.com/manifest.json",
			SHA256:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
	}

	canonical := h.NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}

	// Zero IncLegacyBody calls: manifest_ref suppresses the warning.
	if calls := sink.Calls(); len(calls) != 0 {
		t.Fatalf("manifest_ref MUST suppress legacy warning, got %d calls (%v)", len(calls), calls)
	}
}

// TestNormalizeExternalJobSubmission_NoLegacyFieldsNoWarning pins
// the negative boundary: a body with no legacy fields (modern
// per-scene nested form, no manifest_ref needed) MUST NOT emit
// the warning. This is the canonical "fully-migrated client"
// path.
func TestNormalizeExternalJobSubmission_NoLegacyFieldsNoWarning(t *testing.T) {
	t.Parallel()

	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(sink)

	req := SubmitJobRequest{
		IdempotencyKey: "modern-001",
		// No top-level voiceover_paths, no scenes[N].clip_link,
		// no subtitle_tracks. Per-scene nested form only.
		Scenes: []SubmitScene{
			{
				Text: "s",
				// Migration target: per-scene nested form.
				Voiceover: &SubmitVoiceover{URL: "https://x/vo.mp3"},
				Clip:      &SubmitClip{URL: "https://x/clip.mp4"},
				Subtitles: &SubmitSubtitles{URL: "https://x/sub.ass", Format: "ass"},
			},
		},
	}

	canonical := h.NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}

	if calls := sink.Calls(); len(calls) != 0 {
		t.Fatalf("modern per-scene body MUST NOT emit warning, got %d calls (%v)", len(calls), calls)
	}
}

// TestNormalizeExternalJobSubmission_NoSinkStillWorks pins the
// safe-default contract: a Handlers without a wired legacy-body
// sink MUST NOT panic on the legacy-shape warning path; the
// canonical payload is returned unchanged. Without this test a
// future refactor that removed the nil-check would crash every
// test that constructs an empty Handlers{} (e.g., the existing
// creator_intake_sink_test.go + job_submit_test.go suites).
func TestNormalizeExternalJobSubmission_NoSinkStillWorks(t *testing.T) {
	t.Parallel()

	h := &Handlers{} // no sink wired

	req := SubmitJobRequest{
		IdempotencyKey: "no-sink-001",
		VoiceoverPaths: []string{"https://x/y.mp3"},
		Scenes: []SubmitScene{
			{Text: "s", ClipLink: "https://x/y.mp4", DurationSeconds: 5},
		},
	}

	// Must not panic.
	canonical := h.NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil when no sink is wired")
	}
	if canonical.WorkerPayload == nil {
		t.Fatal("WorkerPayload is nil; submission was supposed to pass through unchanged")
	}
}

// TestNormalizeExternalJobSubmission_ClipLinkAloneTriggers pins
// the single-field detection criterion: scenes[N].clip_link
// alone (no top-level voiceover_paths, no subtitle_tracks) MUST
// trigger the warning. Without this test a future refactor that
// changed the OR-logic to require voiceover_paths OR subtitle_tracks
// silently drops the warning for the most common pre-migration
// shape.
func TestNormalizeExternalJobSubmission_ClipLinkAloneTriggers(t *testing.T) {
	t.Parallel()

	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(sink)

	req := SubmitJobRequest{
		IdempotencyKey: "clip-link-only-001",
		// ONLY scenes[N].clip_link — no other legacy signals.
		Scenes: []SubmitScene{
			{Text: "s", ClipLink: "https://x/y.mp4", DurationSeconds: 5},
		},
	}

	canonical := h.NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}

	if calls := sink.Calls(); len(calls) != 1 {
		t.Fatalf("clip_link alone MUST trigger warning, got %d calls (%v)", len(calls), calls)
	}
}

// TestLegacyBodySinkClientKindPreManifestRef_Value locks the
// canonical string value of the bounded client_kind enum. This
// test exists to fail loudly if a future refactor renames the
// constant (which would silently break Prometheus dashboards +
// log greps that filter on the literal value).
func TestLegacyBodySinkClientKindPreManifestRef_Value(t *testing.T) {
	t.Parallel()

	const want = "pipelinegen_pre_manifest_ref"
	if LegacyBodySinkClientKindPreManifestRef != want {
		t.Errorf("LegacyBodySinkClientKindPreManifestRef = %q, want %q (catalog + log line + dashboards all depend on this literal value)",
			LegacyBodySinkClientKindPreManifestRef, want)
	}
}

// TestNormalizeExternalJobSubmission_SubtitleTracksAloneTriggers
// pins the subtitle_tracks-only detection branch. A PipelineGen
// client that submits voiceover inline (no top-level
// voiceover_paths) but with subtitle_tracks is a plausible
// pre-migration shape that MUST still fire the warning.
func TestNormalizeExternalJobSubmission_SubtitleTracksAloneTriggers(t *testing.T) {
	t.Parallel()

	sink := &recordingLegacyBodySink{}
	h := &Handlers{}
	h.WithLegacyBodySink(sink)

	req := SubmitJobRequest{
		IdempotencyKey: "subs-only-001",
		// ONLY subtitle_tracks — no voiceover_paths, no clip_link.
		SubtitleTracks: []SubmitSubtitleTrack{{Source: "https://x/sub.ass"}},
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
	}

	canonical := h.NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}

	if calls := sink.Calls(); len(calls) != 1 {
		t.Fatalf("subtitle_tracks alone MUST trigger warning, got %d calls (%v)", len(calls), calls)
	}
}