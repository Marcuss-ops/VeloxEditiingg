package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"velox-server/internal/inputsecurity"
)

// RewriteRemoteInputPayload is the single acquisition boundary for remote
// media references that are already present in the worker payload. The
// normalizer may accept several wire shapes, but every supported media field
// must be converted to a content-addressed velox-asset reference before the
// task is persisted.
func (s *AssetService) RewriteRemoteInputPayload(ctx context.Context, payload map[string]interface{}) error {
	if s == nil || payload == nil {
		return nil
	}

	if err := rewriteStringListField(ctx, s, payload, "voiceover_paths", inputsecurity.KindVoiceover); err != nil {
		return err
	}
	if err := rewriteStringField(ctx, s, payload, "audio_url", inputsecurity.KindAudio); err != nil {
		return err
	}
	if err := rewriteStringListField(ctx, s, payload, "scene_image_paths", inputsecurity.KindImage); err != nil {
		return err
	}
	if err := rewriteTransitionSoundEffects(ctx, s, payload); err != nil {
		return err
	}

	for _, key := range []string{"scenes", "render_manifest"} {
		if value, ok := payload[key]; ok {
			if err := rewriteRemoteInputValue(ctx, s, value); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	if encoded, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(encoded), &scenes); err != nil {
			return fmt.Errorf("scenes_json: %w", err)
		}
		if err := rewriteRemoteInputValue(ctx, s, scenes); err != nil {
			return fmt.Errorf("scenes_json: %w", err)
		}
		rewritten, err := json.Marshal(scenes)
		if err != nil {
			return fmt.Errorf("scenes_json: marshal rewritten scenes: %w", err)
		}
		payload["scenes_json"] = string(rewritten)
	}

	if tracks, ok := payload["audio_tracks"]; ok {
		for _, track := range mapList(tracks) {
			if err := rewriteFirstMapField(ctx, s, track, inputsecurity.KindAudio, "source_url", "source", "url"); err != nil {
				return fmt.Errorf("audio_tracks: %w", err)
			}
		}
	}
	if tracks, ok := payload["subtitle_tracks"]; ok {
		for _, track := range mapList(tracks) {
			if err := rewriteFirstMapField(ctx, s, track, inputsecurity.KindSubtitle, "source", "source_url", "url"); err != nil {
				return fmt.Errorf("subtitle_tracks: %w", err)
			}
			if err := rewriteStringField(ctx, s, track, "font", inputsecurity.KindFont); err != nil {
				return fmt.Errorf("subtitle_tracks.font: %w", err)
			}
		}
	}
	if layers, ok := payload["layers"]; ok {
		for _, layer := range mapList(layers) {
			if err := rewriteStringField(ctx, s, layer, "font", inputsecurity.KindFont); err != nil {
				return fmt.Errorf("layers.font: %w", err)
			}
			kind := layerAssetKind(layer)
			if kind == "" {
				continue
			}
			if err := rewriteFirstMapField(ctx, s, layer, kind, "asset", "source"); err != nil {
				return fmt.Errorf("layers.asset: %w", err)
			}
		}
	}
	return nil
}

// rewriteTransitionSoundEffects resolves the configured SFX pool before the
// narrated timeline is built. The normalizer creates audio_tracks from this
// pool later, so the asset declarations must already be present in the
// canonical payload for workers to verify and cache those tracks.
func rewriteTransitionSoundEffects(ctx context.Context, s *AssetService, payload map[string]interface{}) error {
	config, ok := payload["transition_sound_effects"].(map[string]interface{})
	if !ok {
		return nil
	}
	if enabled, present := config["enabled"].(bool); present && !enabled {
		return nil
	}
	values, ok := config["sources"]
	if !ok {
		return nil
	}
	var sources []string
	switch typed := values.(type) {
	case []string:
		sources = typed
	case []interface{}:
		for _, value := range typed {
			if source, ok := value.(string); ok {
				sources = append(sources, source)
			}
		}
	default:
		return nil
	}

	declarations := map[string]map[string]interface{}{}
	rewritten := make([]string, 0, len(sources))
	for _, source := range sources {
		canonical, err := rewriteReference(ctx, s, source, inputsecurity.KindAudio)
		if err != nil {
			return fmt.Errorf("transition_sound_effects.sources: %w", err)
		}
		rewritten = append(rewritten, canonical)
		assetID := strings.TrimPrefix(canonical, VeloxAssetScheme+"://")
		if assetID == canonical || assetID == "" {
			continue
		}
		asset, err := s.Get(ctx, assetID)
		if err != nil {
			return fmt.Errorf("transition_sound_effects asset %q: %w", assetID, err)
		}
		if asset == nil || asset.SHA256 == "" || asset.SizeBytes <= 0 {
			return fmt.Errorf("transition_sound_effects asset %q has incomplete integrity metadata", assetID)
		}
		declarations[assetID] = map[string]interface{}{
			"id":         asset.AssetID,
			"uri":        canonical,
			"kind":       "sfx",
			"sha256":     asset.SHA256,
			"size_bytes": asset.SizeBytes,
		}
	}
	config["sources"] = rewritten
	if len(declarations) == 0 {
		return nil
	}
	assets := mapList(payload["assets"])
	seen := make(map[string]bool, len(assets)+len(declarations))
	for _, item := range assets {
		if id, ok := item["id"].(string); ok {
			seen[id] = true
		}
	}
	for id, declaration := range declarations {
		if !seen[id] {
			assets = append(assets, declaration)
		}
	}
	payload["assets"] = assets
	return nil
}

func rewriteRemoteInputValue(ctx context.Context, s *AssetService, value interface{}) error {
	switch typed := value.(type) {
	case []interface{}:
		for _, item := range typed {
			if err := rewriteRemoteInputValue(ctx, s, item); err != nil {
				return err
			}
		}
	case []map[string]interface{}:
		for _, item := range typed {
			if err := rewriteRemoteInputMap(ctx, s, item); err != nil {
				return err
			}
		}
	case map[string]interface{}:
		return rewriteRemoteInputMap(ctx, s, typed)
	}
	return nil
}

func rewriteRemoteInputMap(ctx context.Context, s *AssetService, item map[string]interface{}) error {
	if item == nil {
		return nil
	}
	if err := rewriteFirstMapField(ctx, s, item, inputsecurity.KindImage, "image_link", "image", "image_url"); err != nil {
		return err
	}
	if err := rewriteStringListField(ctx, s, item, "image_links", inputsecurity.KindImage); err != nil {
		return err
	}
	if err := rewriteFirstMapField(ctx, s, item, inputsecurity.KindClip, "clip_link"); err != nil {
		return err
	}
	if nested, ok := item["clip"].(map[string]interface{}); ok {
		if err := rewriteCanonicalAssetMap(ctx, s, nested, inputsecurity.KindClip); err != nil {
			return err
		}
	}
	if stock, ok := item["stock"]; ok {
		for _, asset := range mapList(stock) {
			if err := rewriteCanonicalAssetMap(ctx, s, asset, inputsecurity.KindClip); err != nil {
				return fmt.Errorf("stock: %w", err)
			}
		}
	}
	if nested, ok := item["voiceover"].(map[string]interface{}); ok {
		if err := rewriteCanonicalAssetMap(ctx, s, nested, inputsecurity.KindVoiceover); err != nil {
			return err
		}
	}
	if nested, ok := item["subtitles"].(map[string]interface{}); ok {
		if err := rewriteFirstMapField(ctx, s, nested, inputsecurity.KindSubtitle, "url", "source", "source_url"); err != nil {
			return err
		}
		if err := rewriteStringField(ctx, s, nested, "font", inputsecurity.KindFont); err != nil {
			return err
		}
	}
	if tracks, ok := item["audio_tracks"]; ok {
		for _, track := range mapList(tracks) {
			if err := rewriteFirstMapField(ctx, s, track, inputsecurity.KindAudio, "source_url", "source", "url"); err != nil {
				return err
			}
		}
	}
	if tracks, ok := item["subtitle_tracks"]; ok {
		for _, track := range mapList(tracks) {
			if err := rewriteFirstMapField(ctx, s, track, inputsecurity.KindSubtitle, "source", "source_url", "url"); err != nil {
				return err
			}
		}
	}
	if layers, ok := item["layers"]; ok {
		for _, layer := range mapList(layers) {
			if err := rewriteRemoteInputMap(ctx, s, layer); err != nil {
				return err
			}
		}
	}
	return nil
}

func rewriteCanonicalAssetMap(ctx context.Context, s *AssetService, item map[string]interface{}, kind inputsecurity.Kind) error {
	if item == nil {
		return nil
	}
	raw, ok := item["url"].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("canonical asset url is required")
	}
	reference := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(reference), VeloxAssetScheme+"://") {
		assetID := strings.TrimSpace(strings.TrimPrefix(reference, VeloxAssetScheme+"://"))
		if assetID == "" {
			return fmt.Errorf("canonical asset id is required")
		}
		if s.repo == nil {
			return fmt.Errorf("canonical asset registry unavailable")
		}
		registered, err := s.Get(ctx, assetID)
		if err != nil {
			return fmt.Errorf("lookup canonical asset %q: %w", assetID, err)
		}
		if registered == nil || registered.AssetID != assetID {
			return fmt.Errorf("canonical asset %q is not registered", assetID)
		}
		item["asset_id"] = assetID
		item["url"] = VeloxAssetScheme + "://" + assetID
		return nil
	}
	asset, err := s.ResolveAndRegister(ctx, ResolveAssetCommand{Kind: string(kind), Reference: reference})
	if err != nil {
		return err
	}
	if asset == nil || asset.AssetID == "" || asset.Reference() == "" {
		return fmt.Errorf("asset resolver returned incomplete canonical asset")
	}
	item["asset_id"] = asset.AssetID
	item["url"] = asset.Reference()
	return nil
}

func rewriteFirstMapField(ctx context.Context, s *AssetService, item map[string]interface{}, kind inputsecurity.Kind, keys ...string) error {
	for _, key := range keys {
		if raw, ok := item[key].(string); ok && strings.TrimSpace(raw) != "" {
			rewritten, err := rewriteReference(ctx, s, raw, kind)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			item[key] = rewritten
			return nil
		}
	}
	return nil
}

func rewriteStringField(ctx context.Context, s *AssetService, item map[string]interface{}, key string, kind inputsecurity.Kind) error {
	if item == nil {
		return nil
	}
	raw, ok := item[key].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	rewritten, err := rewriteReference(ctx, s, raw, kind)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	item[key] = rewritten
	return nil
}

func rewriteStringListField(ctx context.Context, s *AssetService, item map[string]interface{}, key string, kind inputsecurity.Kind) error {
	if item == nil {
		return nil
	}
	switch values := item[key].(type) {
	case []string:
		for index, value := range values {
			rewritten, err := rewriteReference(ctx, s, value, kind)
			if err != nil {
				return fmt.Errorf("%s[%d]: %w", key, index, err)
			}
			values[index] = rewritten
		}
	case []interface{}:
		for index, raw := range values {
			value, ok := raw.(string)
			if !ok || strings.TrimSpace(value) == "" {
				continue
			}
			rewritten, err := rewriteReference(ctx, s, value, kind)
			if err != nil {
				return fmt.Errorf("%s[%d]: %w", key, index, err)
			}
			values[index] = rewritten
		}
	}
	return nil
}

func rewriteReference(ctx context.Context, s *AssetService, reference string, kind inputsecurity.Kind) (string, error) {
	reference = strings.TrimSpace(reference)
	if reference == "" || strings.HasPrefix(strings.ToLower(reference), VeloxAssetScheme+"://") {
		return reference, nil
	}
	asset, err := s.ResolveAndRegister(ctx, ResolveAssetCommand{Kind: string(kind), Reference: reference})
	if err != nil {
		return "", err
	}
	if asset == nil || asset.Reference() == "" {
		return "", fmt.Errorf("asset resolver returned no canonical reference")
	}
	return asset.Reference(), nil
}

func mapList(value interface{}) []map[string]interface{} {
	switch typed := value.(type) {
	case []map[string]interface{}:
		return typed
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(typed))
		for _, item := range typed {
			if mapped, ok := item.(map[string]interface{}); ok {
				out = append(out, mapped)
			}
		}
		return out
	default:
		return nil
	}
}

func layerAssetKind(layer map[string]interface{}) inputsecurity.Kind {
	value := strings.ToLower(strings.TrimSpace(firstMapString(layer, "type", "role")))
	switch value {
	case "image", "still", "logo":
		return inputsecurity.KindImage
	case "video", "clip", "stock_clip":
		return inputsecurity.KindClip
	case "audio", "music", "voiceover", "background_music":
		return inputsecurity.KindAudio
	case "font", "typeface":
		return inputsecurity.KindFont
	default:
		return ""
	}
}

func firstMapString(item map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := item[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
