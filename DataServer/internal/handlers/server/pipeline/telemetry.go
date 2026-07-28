// Package pipeline — telemetry.go owns:
//   - isLegacyCompatShape: pre-manifest_ref legacy-shape
//     detector that returns true when the request carries the
//     legacy positional `voiceover_paths` without a per-scene
//     nested asset (so the legacy body sink should swallow
//     the warning).
//   - countScenesWithClipLink: telemetry counter helper that
//     returns the number of scenes whose flat clip_link is
//     set (used for legacy-shape migration dashboards).
//   - legacyBodySinkOrNoop: legacy-compat body sink selector
//     invoked when isLegacyCompatShape returns true. Returns
//     the noop sink when h.legacyBodySink is nil (the
//     deprecated-path default) so the call site stays
//     nil-safe.
package pipeline
import (
	"strings"
)


// isLegacyCompatShape reports whether the SubmitJobRequest carries
// the pre-manifest_ref compatibility body shape. Pure function: no
// handler/sink access. Used by NormalizeExternalJobSubmission to gate
// the legacy-body-shape warning emit; the test suite calls it
// directly to pin the detection criteria.
func isLegacyCompatShape(req SubmitJobRequest) bool {
	if len(req.VoiceoverPaths) > 0 {
		return true
	}
	if len(req.SubtitleTracks) > 0 {
		return true
	}
	for _, s := range req.Scenes {
		if strings.TrimSpace(s.ClipLink) != "" {
			return true
		}
	}
	return false
}

// countScenesWithClipLink returns the number of scenes whose
// pre-manifest_ref flat `clip_link` field is non-empty. Used by the
// legacy-body-shape warning log line so operators can see the per-
// scene distribution in the compat body without grepping every
// scene in the structured log.
func countScenesWithClipLink(scenes []SubmitScene) int {
	n := 0
	for _, s := range scenes {
		if strings.TrimSpace(s.ClipLink) != "" {
			n++
		}
	}
	return n
}

// legacyBodySinkOrNoop returns the wired legacy-body-shape sink or a
// noop if not set. Mirrors intakeSinkOrNoop's contract: the handler
// never panics on a missing wiring and never silently drops a metric
// event.
func (h *Handlers) legacyBodySinkOrNoop() LegacyBodySink {
	if h.legacyBodySink == nil {
		return noopLegacyBodySink{}
	}
	return h.legacyBodySink
}

// submitRequestToRawPayload builds the canonical flat-map shape that
// remoteengine.ParseRemotePipelineResult consumes. Mirrors the wire
// shape documented at DataServer/api/openapi.yaml under
// `CreatorPushPayload` — same key names (snake_case, alias paths
// collapsed to canonical) the typed DTO expects.
//
// The map is a one-shot invariant: it is the boundary between the
// Submit-scoped typed structs (SubmitScene, SubmitDeliveryPlanEntry)
// and the remoteengine-typed DTO (RemotePipelineResult). Everything
// downstream of this point sees only the canonical envelope.
//
// Per-scene enrichment (Phase 2 of the render-manifest plan):
// scene[N].Clip / Voiceover / Subtitles nested objects are emitted
// directly as nested maps in the scene entry, replacing the legacy
// position-coupled `voiceover_paths[N] ↔ scenes[N]` contract. The
// flat `clip_link` / `image_link` keys are still emitted when
// present on the wire (back-compat for clients that haven't migrated
// to the nested shape). When BOTH the flat and the nested are
// supplied for the same scene, the nested wins (the canonical
// "new shape" override on the legacy alias key).
//
// RetryBudget boundary contract:
//   - d.RetryBudget == nil              → entry["retry_budget"] = DefaultRetryBudget
//     (the OpenAPI default of 3; mirrors what an omitted int field
//     would have meant historically).
//   - d.RetryBudget != nil, *d == 0    → entry["retry_budget"] = 0
//     (client explicitly chose zero retries; preserved verbatim so
//     downstream enqueue-layer validation can distinguish "0
//     explicitly" from "omitted").
//   - d.RetryBudget != nil, *d > 0     → entry["retry_budget"] = *d
//     (clamp unchanged; a future enrichment could enforce an upper
//     bound here without touching the boundary contract).
//
// Trim policy (matching NormalizeExternalJobSubmission's contract):
// identity-bearing fields are trimmed (IdempotencyKey, VideoName,
// URL-shaped scene clip_link / image_link, delivery destination_id,
// scene nested object URLs). CONTENT fields (ScriptText, scene text)
// are passed through verbatim because legitimate whitespace might
// appear.
