package contract

import (
	"encoding/json"
	"strings"
)

// SunsetSceneKindClipEnforcement is the date after which a scene declared as
// kind="clip" without a clip asset is REJECTED at intake (422) instead of
// being accepted with a warning. Until then the shape is soft-deprecated:
// intake accepts the payload, records telemetry, logs a warning and echoes
// the finding in the accept envelope so generators can migrate.
//
// Rationale (Isabelle Caracristi job_e2adca259c034c1e, 2026-10-02): three
// testimony scenes were declared kind="clip" with the asset inside "stock"
// and no "clip" object. The worker compiles stock scenes through the mute
// background path (no scene_clip_audio track), so the testimony audio could
// never reach the montage. The transfer itself was intact; the declaration
// was wrong. A fail-closed intake would have surfaced this in one second
// instead of after a 709 MB render.
//
// Per ADR 0008 §(b) point 1 the hard rejection is soft-deprecated first:
// kind="clip"+stock-only scenes are still used by in-production background
// fillers (e.g. the "Protected intro clip" scenes), so both C1 (external
// callers still use the shape) and C2 (public intake) hold.
const SunsetSceneKindClipEnforcement = "2026-11-15"

// SceneKindClipAnomalyReasonKindClipWithoutClipAsset identifies a scene
// declared as kind="clip" that carries no clip asset object. The worker
// ignores the informational "kind" field and renders purely from the
// clip/stock fields, so such a scene is compiled as a mute stock
// background. Testimony clips described this way lose their original
// audio by construction.
const SceneKindClipAnomalyReasonKindClipWithoutClipAsset = "kind_clip_without_clip_asset"

// SceneKindClipAnomaly is one soft-deprecated scene declaration.
type SceneKindClipAnomaly struct {
	// Index is the zero-based position in the submitted scene array.
	Index int `json:"index"`
	// SceneID is the submitted scene identifier ("" when absent).
	SceneID string `json:"scene_id,omitempty"`
	// Reason is the anomaly code (see SceneKindClipAnomalyReason*).
	Reason string `json:"reason"`
}

// FindKindClipAnomalies scans canonical scene maps and returns one entry
// per scene declared as kind="clip" without a usable clip asset. A clip
// asset is usable when the "clip" object carries any of asset_id,
// drive_file_id or url. The check is intentionally limited to the
// declaration mismatch it can prove locally; it never guesses intent.
func FindKindClipAnomalies(scenes []map[string]interface{}) []SceneKindClipAnomaly {
	var out []SceneKindClipAnomaly
	for index, scene := range scenes {
		if scene == nil {
			continue
		}
		kind, _ := scene["kind"].(string)
		if !strings.EqualFold(strings.TrimSpace(kind), "clip") {
			continue
		}
		if sceneClipIdentity(scene["clip"]) != "" {
			continue
		}
		sceneID, _ := scene["scene_id"].(string)
		out = append(out, SceneKindClipAnomaly{
			Index:   index,
			SceneID: strings.TrimSpace(sceneID),
			Reason:  SceneKindClipAnomalyReasonKindClipWithoutClipAsset,
		})
	}
	return out
}

// FindKindClipAnomaliesInPayload extracts the scene array from a raw intake
// payload map and runs FindKindClipAnomalies over it. It accepts both the
// "scenes" list form and the "scenes_json" encoded form (the two shapes the
// typed intake DTO normalizes). A missing or unparseable scene array yields
// no anomalies; malformed documents are reported by the intake validators,
// not here.
func FindKindClipAnomaliesInPayload(payload map[string]interface{}) []SceneKindClipAnomaly {
	if payload == nil {
		return nil
	}
	if scenes, err := ParseSceneMaps(payload["scenes"]); err == nil && len(scenes) > 0 {
		return FindKindClipAnomalies(scenes)
	}
	if encoded, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		var raw []map[string]interface{}
		if err := json.Unmarshal([]byte(encoded), &raw); err == nil {
			return FindKindClipAnomalies(NormalizeSceneMaps(raw))
		}
	}
	return nil
}

// sceneClipIdentity mirrors the worker-side asset identity (asset_id,
// drive_file_id, url): the same keys the renderer uses to resolve a clip
// source. Kept local so shared/contract stays free of worker imports.
func sceneClipIdentity(raw interface{}) string {
	item, ok := raw.(map[string]interface{})
	if !ok {
		return ""
	}
	for _, key := range []string{"asset_id", "drive_file_id", "url"} {
		if value, ok := item[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
