package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// assetMetadata is the integrity contract required before a transport asset
// can be materialized. A URI without both fields is never downloaded or
// accepted from cache.
type assetMetadata struct {
	ID        string
	URI       string
	SHA256    string
	SizeBytes int64
}

type assetMetadataIndex map[string]assetMetadata

// resolveCommonAssetPayload is the one worker-side resolver for media assets.
// It covers video, voiceover, music, effects, subtitles/captions, images and
// nested render-manifest asset envelopes. The index is built before walking
// the payload so a manifest asset declaration can provide integrity metadata
// for a reference used elsewhere in the timeline.
func (w *Worker) resolveCommonAssetPayload(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	index := make(assetMetadataIndex)
	if err := collectAssetMetadata(payload, index); err != nil {
		return nil, err
	}
	copyPayload, err := deepCopyAssetValue(payload)
	if err != nil {
		return nil, fmt.Errorf("common asset resolver: deep copy payload: %w", err)
	}
	resolved, err := w.resolveCommonAssetValue(ctx, copyPayload, index, "", false)
	if err != nil {
		return nil, err
	}
	result, ok := resolved.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("common asset resolver: payload is not an object")
	}
	return result, nil
}

func (w *Worker) resolveCommonAssetValue(ctx context.Context, value interface{}, index assetMetadataIndex, field string, mediaContext bool) (interface{}, error) {
	switch typed := value.(type) {
	case string:
		ref := strings.TrimSpace(typed)
		if ref == "" || !mediaContext {
			return value, nil
		}
		return w.resolveVerifiedAssetReference(ctx, ref, index, field)
	case map[string]interface{}:
		assetContext := mediaContext || isAssetEnvelope(typed)
		for key, item := range typed {
			if strings.EqualFold(key, "scenes_json") {
				encoded, ok := item.(string)
				if ok && strings.TrimSpace(encoded) != "" {
					var decoded interface{}
					if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
						return nil, fmt.Errorf("common asset resolver: %s: invalid JSON: %w", fieldPath(field, key), err)
					}
					resolved, err := w.resolveCommonAssetValue(ctx, decoded, index, fieldPath(field, key), false)
					if err != nil {
						return nil, err
					}
					encodedResolved, err := json.Marshal(resolved)
					if err != nil {
						return nil, fmt.Errorf("common asset resolver: %s: encode JSON: %w", fieldPath(field, key), err)
					}
					typed[key] = string(encodedResolved)
					continue
				}
			}
			childMedia := (assetContext && isMediaValueField(key)) || isMediaContainerField(key) || isMediaReferenceField(key)
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fieldPath(field, key), childMedia)
			if err != nil {
				return nil, err
			}
			typed[key] = resolved
		}
		return typed, nil
	case []interface{}:
		// A semantic scene list may contain plain text strings; only lists
		// whose field itself denotes media references are URI lists. Maps
		// inside scenes/tracks are still traversed and their media fields
		// are classified individually.
		listMedia := isMediaReferenceField(field)
		for i, item := range typed {
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fmt.Sprintf("%s[%d]", field, i), listMedia)
			if err != nil {
				return nil, err
			}
			typed[i] = resolved
		}
		return typed, nil
	case []string:
		listMedia := isMediaReferenceField(field)
		for i, item := range typed {
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fmt.Sprintf("%s[%d]", field, i), listMedia)
			if err != nil {
				return nil, err
			}
			typed[i] = resolved.(string)
		}
		return typed, nil
	case []map[string]interface{}:
		for i, item := range typed {
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fmt.Sprintf("%s[%d]", field, i), mediaContext)
			if err != nil {
				return nil, err
			}
			mapValue, ok := resolved.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("common asset resolver: %s[%d] is not an object", field, i)
			}
			typed[i] = mapValue
		}
		return typed, nil
	default:
		return value, nil
	}
}

func (w *Worker) resolveVerifiedAssetReference(ctx context.Context, reference string, index assetMetadataIndex, field string) (string, error) {
	assetID, bridged := parseVeloxAssetReference(reference)
	if !bridged {
		return "", fmt.Errorf("common asset resolver: %s: raw URL or local path rejected; use velox-asset://", field)
	}
	if !validAssetReferenceID(assetID) {
		return "", fmt.Errorf("common asset resolver: %s: invalid velox-asset:// reference", field)
	}
	metadata, ok := lookupAssetMetadata(index, reference, assetID)
	if !ok {
		return "", fmt.Errorf("common asset resolver: %s: SHA-256 and positive size_bytes are required for %s", field, reference)
	}
	if !validSHA256(metadata.SHA256) || metadata.SizeBytes <= 0 {
		return "", fmt.Errorf("common asset resolver: %s: invalid SHA-256 or size_bytes for %s", field, reference)
	}
	resolved, err := w.downloadVeloxAssetWithMetadata(ctx, assetID, metadata.SHA256, metadata.SizeBytes)
	if err != nil {
		return "", fmt.Errorf("common asset resolver: %s: %w", field, err)
	}
	return resolved, nil
}

func collectAssetMetadata(value interface{}, index assetMetadataIndex) error {
	switch typed := value.(type) {
	case map[string]interface{}:
		sha := expectedAssetSHA256(typed)
		size := expectedAssetSize(typed)
		for key, raw := range typed {
			if strings.EqualFold(key, "scenes_json") {
				if encoded, ok := raw.(string); ok && strings.TrimSpace(encoded) != "" {
					var decoded interface{}
					if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
						return fmt.Errorf("common asset resolver: scenes_json metadata: %w", err)
					}
					if err := collectAssetMetadata(decoded, index); err != nil {
						return err
					}
				}
				continue
			}
			ref, ok := raw.(string)
			if !ok || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(ref)), "velox-asset://") || !isAssetReferenceField(key) {
				continue
			}
			if sha == "" || size <= 0 {
				// Do not fail for arbitrary metadata maps here; the reference
				// walker emits the precise field error when it is materialized.
				continue
			}
			registerAssetMetadata(index, ref, typed, sha, size)
		}
		id := firstString(typed, "asset_id", "id")
		uri := firstString(typed, "uri", "url", "source_url")
		if id != "" && uri != "" && sha != "" && size > 0 {
			registerAssetMetadata(index, uri, typed, sha, size)
			registerAssetMetadata(index, "velox-asset://"+id, typed, sha, size)
		}
		for _, item := range typed {
			if err := collectAssetMetadata(item, index); err != nil {
				return err
			}
		}
	case []interface{}:
		for _, item := range typed {
			if err := collectAssetMetadata(item, index); err != nil {
				return err
			}
		}
	case []map[string]interface{}:
		for _, item := range typed {
			if err := collectAssetMetadata(item, index); err != nil {
				return err
			}
		}
	}
	return nil
}

func registerAssetMetadata(index assetMetadataIndex, reference string, fields map[string]interface{}, sha string, size int64) {
	ref := strings.TrimSpace(reference)
	assetID, bridged := parseVeloxAssetReference(ref)
	if !bridged {
		return
	}
	if ref == "" {
		return
	}
	canonicalRef := "velox-asset://" + assetID
	index[ref] = assetMetadata{ID: assetID, URI: canonicalRef, SHA256: strings.TrimSpace(sha), SizeBytes: size}
	index[canonicalRef] = index[ref]
	if id := firstString(fields, "asset_id", "id"); id != "" {
		index["velox-asset://"+id] = index[ref]
	}
}

func lookupAssetMetadata(index assetMetadataIndex, reference, assetID string) (assetMetadata, bool) {
	if metadata, ok := index[reference]; ok {
		return metadata, true
	}
	metadata, ok := index["velox-asset://"+assetID]
	return metadata, ok
}

func validAssetReferenceID(assetID string) bool {
	if assetID == "" || strings.ContainsAny(assetID, "\\\x00\r\n") {
		return false
	}
	for _, segment := range strings.Split(assetID, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func isAssetEnvelope(fields map[string]interface{}) bool {
	kind := strings.ToLower(strings.TrimSpace(firstString(fields, "kind", "asset_kind", "role")))
	return kind == "video" || kind == "audio" || kind == "subtitle" || kind == "image" || kind == "music" || kind == "sfx" || kind == "captions" || kind == "voiceover"
}

func isMediaContainerField(field string) bool {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "scenes", "scene", "scenes_json", "items", "audio_tracks", "voiceover_paths", "voiceover_path", "audio_path", "video", "video_url", "video_path", "clip_segments", "clips", "images", "scene_image_paths", "voiceover", "voiceover_url", "music", "music_url", "music_path", "effects", "effect", "effect_url", "effect_path", "sfx", "sfx_url", "sfx_path", "subtitles", "subtitle_tracks", "subtitle_url", "subtitle_path", "captions", "caption_url", "caption_path", "tracks", "assets", "render_manifest":
		return true
	default:
		return false
	}
}

func isMediaValueField(field string) bool {
	return isMediaReferenceField(field) || isMediaContainerField(field)
}

func isMediaReferenceField(field string) bool {
	return isAssetReferenceField(field) || strings.HasSuffix(strings.ToLower(strings.TrimSpace(field)), "_paths") || strings.HasSuffix(strings.ToLower(strings.TrimSpace(field)), "_links")
}

func isAssetReferenceField(field string) bool {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "uri", "url", "source_url", "source", "audio_path", "video_url", "video_path", "voiceover_path", "voiceover", "voiceover_url", "voiceover_paths", "audio_url", "music", "music_path", "music_url", "effect", "effects", "effect_path", "effect_url", "sfx", "sfx_path", "sfx_url", "subtitle", "subtitles", "subtitle_path", "subtitle_url", "caption", "captions", "caption_path", "caption_url", "clip", "clips", "clip_link", "clip_links", "image", "images", "image_link", "image_links", "scene_image_paths":
		return true
	default:
		return false
	}
}

func fieldPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

func parseVeloxAssetReference(reference string) (string, bool) {
	const scheme = "velox-asset://"
	trimmed := strings.TrimSpace(reference)
	if len(trimmed) < len(scheme) || !strings.EqualFold(trimmed[:len(scheme)], scheme) {
		return "", false
	}
	return strings.TrimSpace(trimmed[len(scheme):]), true
}

func deepCopyAssetValue(value interface{}) (interface{}, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var copyValue interface{}
	if err := json.Unmarshal(data, &copyValue); err != nil {
		return nil, err
	}
	return copyValue, nil
}

func firstString(fields map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := fields[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
