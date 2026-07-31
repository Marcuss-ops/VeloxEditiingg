package worker

import (
	"context"
	"fmt"
	"strings"
)

// assetMetadata is the integrity contract required before a transport asset
// can be materialized. A URI without both fields is never downloaded or
// accepted from cache.
type assetMetadata struct {
	ID         string
	URI        string
	SHA256     string
	SizeBytes  int64
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
	resolved, err := w.resolveCommonAssetValue(ctx, payload, index, "", false)
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
		assetContext := mediaContext || isAssetEnvelope(typed) || isMediaContainerField(field)
		for key, item := range typed {
			childMedia := assetContext && isMediaValueField(key)
			if isMediaContainerField(key) {
				childMedia = true
			}
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fieldPath(field, key), childMedia)
			if err != nil {
				return nil, err
			}
			typed[key] = resolved
		}
		return typed, nil
	case []interface{}:
		for i, item := range typed {
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fmt.Sprintf("%s[%d]", field, i), mediaContext || isMediaContainerField(field))
			if err != nil {
				return nil, err
			}
			typed[i] = resolved
		}
		return typed, nil
	case []string:
		for i, item := range typed {
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fmt.Sprintf("%s[%d]", field, i), mediaContext || isMediaContainerField(field))
			if err != nil {
				return nil, err
			}
			typed[i] = resolved.(string)
		}
		return typed, nil
	case []map[string]interface{}:
		for i, item := range typed {
			resolved, err := w.resolveCommonAssetValue(ctx, item, index, fmt.Sprintf("%s[%d]", field, i), mediaContext || isMediaContainerField(field))
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
	if !strings.HasPrefix(strings.ToLower(reference), "velox-asset://") {
		return "", fmt.Errorf("common asset resolver: %s: raw URL or local path rejected; use velox-asset://", field)
	}
	assetID := strings.TrimSpace(strings.TrimPrefix(reference, "velox-asset://"))
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
	if ref == "" {
		return
	}
	assetID := strings.TrimPrefix(ref, "velox-asset://")
	index[ref] = assetMetadata{ID: assetID, URI: ref, SHA256: strings.TrimSpace(sha), SizeBytes: size}
	index["velox-asset://"+assetID] = index[ref]
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
	case "scenes", "scene", "items", "audio_tracks", "video", "video_url", "video_path", "voiceover", "voiceover_url", "music", "music_url", "effects", "effect", "sfx", "sfx_url", "subtitles", "subtitle_tracks", "subtitle_url", "captions", "caption_url", "tracks", "assets", "render_manifest":
		return true
	default:
		return false
	}
}

func isMediaValueField(field string) bool {
	return isAssetReferenceField(field) || isMediaContainerField(field)
}

func isAssetReferenceField(field string) bool {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "uri", "url", "source_url", "source", "audio_path", "video_url", "video_path", "voiceover_path", "voiceover", "voiceover_url", "voiceover_paths", "audio_url", "music_path", "music_url", "effect", "effect_path", "effect_url", "sfx", "sfx_path", "sfx_url", "subtitle", "subtitles", "subtitle_path", "subtitle_url", "caption", "caption_path", "caption_url", "clip", "clip_link", "clip_links", "image", "image_link", "image_links", "scene_image_paths":
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

func firstString(fields map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := fields[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
