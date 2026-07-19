package worker

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// firstVoiceoverReference is the package-level helper used by the audio
// resolver to discover a voiceover reference from a task payload map.
// Kept package-private: the surface for tests and call sites is
// resolveVoiceoverAudioPath.
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

// resolveVoiceoverAudioPath resolves a single voiceover reference into a
// local path. HTTP(S) and drive.google.com raw URLs are rejected: they must
// be bridged via the velox-asset:// transport scheme and resolved through
// the asset downloader.
func (w *Worker) resolveVoiceoverAudioPath(ctx context.Context, ref string, params map[string]interface{}) (string, error) {
	reference := strings.TrimSpace(ref)
	if reference == "" {
		reference = firstVoiceoverReference(params)
	}
	if reference == "" {
		return "", fmt.Errorf("missing voiceover audio path")
	}

	switch {
	case strings.HasPrefix(reference, "velox-asset://"):
		assetID := strings.TrimSpace(strings.TrimPrefix(reference, "velox-asset://"))
		if assetID == "" || strings.ContainsAny(assetID, `/\`) {
			return "", fmt.Errorf("invalid velox asset reference")
		}
		return w.downloadVeloxAsset(ctx, assetID)
	case strings.HasPrefix(strings.ToLower(reference), "http://"), strings.HasPrefix(strings.ToLower(reference), "https://"), strings.Contains(strings.ToLower(reference), "drive.google.com"):
		return "", fmt.Errorf("unsupported voiceover reference: raw URL must be bridged as velox-asset://")
	default:
		if info, err := os.Stat(reference); err == nil && !info.IsDir() {
			return reference, nil
		}
		return "", fmt.Errorf("voiceover file not found locally: %s", reference)
	}
}

// resolveAudioPayload materializes transport-level velox-asset references
// before the executor/engine sees the immutable task contract. The C++ engine
// deliberately accepts filesystem paths and HTTP(S), not the master-only
// velox-asset scheme.
func (w *Worker) resolveAudioPayload(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	clone := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		clone[key] = value
	}

	for _, key := range []string{"audio_path", "voiceover_path", "audio_url", "voiceover_url"} {
		if raw, ok := clone[key].(string); ok && strings.HasPrefix(strings.TrimSpace(raw), "velox-asset://") {
			resolved, err := w.resolveVoiceoverAudioPath(ctx, raw, clone)
			if err != nil {
				return nil, fmt.Errorf("resolve %s: %w", key, err)
			}
			clone[key] = resolved
		}
	}
	if raw, ok := clone["voiceover_paths"].([]interface{}); ok {
		items := append([]interface{}(nil), raw...)
		for i, item := range items {
			if ref, ok := item.(string); ok && strings.HasPrefix(strings.TrimSpace(ref), "velox-asset://") {
				resolved, err := w.resolveVoiceoverAudioPath(ctx, ref, clone)
				if err != nil {
					return nil, fmt.Errorf("resolve voiceover_paths[%d]: %w", i, err)
				}
				items[i] = resolved
			}
		}
		clone["voiceover_paths"] = items
	}
	if raw, ok := clone["audio_tracks"].([]interface{}); ok {
		tracks := append([]interface{}(nil), raw...)
		for i, item := range tracks {
			track, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			trackClone := make(map[string]interface{}, len(track))
			for key, value := range track {
				trackClone[key] = value
			}
			for _, key := range []string{"source_url", "audio_url", "url"} {
				if ref, ok := trackClone[key].(string); ok && strings.HasPrefix(strings.TrimSpace(ref), "velox-asset://") {
					resolved, err := w.resolveVoiceoverAudioPath(ctx, ref, clone)
					if err != nil {
						return nil, fmt.Errorf("resolve audio_tracks[%d].%s: %w", i, key, err)
					}
					trackClone[key] = resolved
				}
			}
			tracks[i] = trackClone
		}
		clone["audio_tracks"] = tracks
	}
	return clone, nil
}
