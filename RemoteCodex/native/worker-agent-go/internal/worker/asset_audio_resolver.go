package worker

import (
	"context"
	"fmt"
	"strings"
)

// firstVoiceoverReference is the package-level helper used by the audio
// resolver to discover a voiceover reference from a task payload map.
func firstVoiceoverReference(params map[string]interface{}) string {
	if params == nil {
		return ""
	}
	for _, key := range []string{"audio_path", "voiceover_path", "voiceover"} {
		if v, ok := params[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if v, ok := params["voiceover_paths"]; ok {
		switch items := v.(type) {
		case []string:
			for _, item := range items {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					return trimmed
				}
			}
		case []interface{}:
			for _, item := range items {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					return strings.TrimSpace(s)
				}
			}
		}
	}
	return ""
}

// resolveVoiceoverAudioPath resolves a voiceover reference only through the
// common verified bridge. Raw URLs and local paths are rejected, and the
// params map must carry SHA-256 and size_bytes for the referenced asset.
func (w *Worker) resolveVoiceoverAudioPath(ctx context.Context, ref string, params map[string]interface{}) (string, error) {
	reference := strings.TrimSpace(ref)
	if reference == "" {
		reference = firstVoiceoverReference(params)
	}
	if reference == "" {
		return "", fmt.Errorf("missing voiceover audio path")
	}
	payload := make(map[string]interface{}, len(params)+1)
	for key, value := range params {
		payload[key] = value
	}
	payload["audio_path"] = reference
	resolved, err := w.resolveCommonAssetPayload(ctx, payload)
	if err != nil {
		return "", err
	}
	path, ok := resolved["audio_path"].(string)
	if !ok || path == reference {
		return "", fmt.Errorf("voiceover reference was not materialized")
	}
	return path, nil
}
