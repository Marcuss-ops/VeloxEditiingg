package remoteengine

import (
	"strings"

	"velox-shared/compatibility"
	"velox-shared/payload"
)

func convertClipAsset(m map[string]interface{}) *ClipAsset {
	if m == nil {
		return nil
	}
	return &ClipAsset{
		AssetID:     payload.FirstString(m, "asset_id"),
		DriveFileID: payload.FirstString(m, "drive_file_id"),
		URL:         payload.FirstString(m, "url"),
		SHA256:      payload.FirstString(m, "sha256"),
		StartMS:     intFromAnyMap(m["start_ms"]),
		EndMS:       intFromAnyMap(m["end_ms"]),
		DurationMS:  intFromAnyMap(m["duration_ms"]),
	}
}

func convertVoiceoverAsset(m map[string]interface{}) *VoiceoverAsset {
	if m == nil {
		return nil
	}
	return &VoiceoverAsset{
		AssetID:     payload.FirstString(m, "asset_id"),
		DriveFileID: payload.FirstString(m, "drive_file_id"),
		URL:         payload.FirstString(m, "url"),
		SHA256:      payload.FirstString(m, "sha256"),
		DurationMS:  intFromAnyMap(m["duration_ms"]),
		Language:    payload.FirstString(m, "language"),
	}
}

func convertSubtitlesAsset(m map[string]interface{}) *SubtitlesAsset {
	if m == nil {
		return nil
	}
	return &SubtitlesAsset{
		AssetID:  payload.FirstString(m, "asset_id"),
		Format:   payload.FirstString(m, "format"),
		URL:      payload.FirstString(m, "url"),
		SHA256:   payload.FirstString(m, "sha256"),
		Language: payload.FirstString(m, "language"),
	}
}

// extractVoiceoverPathsDTO extracts voiceover references at the ingestion boundary.
// These values support parsing legacy producer responses but are never emitted
// by ToWorkerPayload; canonical scenes own worker-facing audio references.
func extractVoiceoverPathsDTO(flat map[string]interface{}) []string {
	candidates := compatibility.ReadStringList(flat, compatibility.VoiceoverPathsKey)

	if voiceover, ok := flat["voiceover"].(map[string]interface{}); ok {
		if s := payload.FirstString(voiceover, "local_path", "path", "drive_link", "url"); s != "" {
			candidates = append(candidates, s)
		}
	}

	if nested, ok := flat["voiceover_info"].(map[string]interface{}); ok {
		if s := payload.FirstString(nested, "local_path", "path", "drive_link", "url"); s != "" {
			candidates = append(candidates, s)
		}
	}

	// Dedup + trim. The shared registry already normalizes the flat
	// aliases; nested object paths are merged here for DTO compatibility.
	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func extractAssetsDTO(flat map[string]interface{}) []AssetReference {
	var assets []AssetReference

	for _, scene := range extractScenesDTO(flat) {
		if scene.ImageLink != "" {
			assets = append(assets, AssetReference{
				Type: "image",
				URL:  scene.ImageLink,
			})
		}
		if scene.ClipLink != "" {
			assets = append(assets, AssetReference{
				Type: "clip",
				URL:  scene.ClipLink,
			})
		}
	}

	return assets
}
