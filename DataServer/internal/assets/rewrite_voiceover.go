package assets

import (
	"velox-shared/payload"
)

// rewrite_voiceover.go owns the voiceover-specific payload collector
// and applicator. They are deliberately pure payload navigators —
// no DB, no BlobStore, no SHA-256, no resolver registry. The shared
// applyRewrite orchestrator (in payload_rewrite.go) is what wires
// these helpers to ResolveAndRegister.

// collectVoiceoverReferences returns the canonical voiceover
// reference list extracted from a payload, including legacy aliases
// that must still be tolerated on input even though canonical writers
// no longer emit them.
func collectVoiceoverReferences(payloadMap map[string]interface{}) []string {
	if payloadMap == nil {
		return nil
	}
	var candidates []string

	// PR15.6 canonical input: voiceover_paths is now the only top-level alias.
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	// Legacy aliases (id/run_id/title/voiceover_path/audio_path) are dropped
	// from canonical WRITES but collectors must still tolerate legacy payloads
	// flowing through — e.g. older jobs still in SQLite, or external
	// request bodies that the HTTP boundary adapter translates separately.
	candidates = append(candidates,
		payload.FirstString(payloadMap, "voiceover", "unified_voiceover_link"),
	)
	// Below keys still tolerated on input only (read-fallback). They will
	// never be set by canonical writers in PR15.6+.
	candidates = append(candidates,
		payload.FirstString(payloadMap, "voiceover_path", "audio_path"),
	)

	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		if v, ok := params["voiceover_paths"]; ok {
			candidates = append(candidates, payload.NormalizeToStrings(v)...)
		}
		candidates = append(candidates, payload.FirstString(params, "voiceover_path", "audio_path", "voiceover"))
	}

	return payload.DedupeStrings(candidates)
}

// applyVoiceoverReferences writes the canonical voiceover_paths array
// back to the payload AND mirrors it into the parameters sub-map.
//
// PR15.6 + refactor/payload-v2-single-shape: writes ONLY the canonical
// `voiceover_paths` key (array). The singular `voiceover_path` and
// `audio_path` aliases are intentionally NOT written here — downstream
// HTTP-edge reads via RenderHTTPBoundaryJobResponse still tolerate them
// when reading legacy SQLite rows. The `parameters` sub-map mirror is
// also NOT written: the refactor establishes top-level keys as the
// single source of truth, and any legacy `parameters` mirror present on
// input is left untouched (so the round-trip for old rows is preserved).
func applyVoiceoverReferences(payloadMap map[string]interface{}, refs []string) {
	if len(refs) == 0 || payloadMap == nil {
		return
	}
	payloadMap["voiceover_paths"] = append([]string(nil), refs...)
}
