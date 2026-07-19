package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// resolveSceneImagePayload materializes scene-image velox-asset references
// before the C++ engine sees the task. The engine accepts local paths, while
// velox-asset:// is a master/worker transport reference.
func (w *Worker) resolveSceneImagePayload(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	clone := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		clone[key] = value
	}

	resolve := func(ref string) (string, error) {
		ref = strings.TrimSpace(ref)
		if !strings.HasPrefix(ref, "velox-asset://") {
			return ref, nil
		}
		assetID := strings.TrimPrefix(ref, "velox-asset://")
		if assetID == "" || strings.ContainsAny(assetID, `/\`) {
			return "", fmt.Errorf("invalid scene image asset reference")
		}
		return w.downloadVeloxAsset(ctx, assetID)
	}
	resolveStringList := func(value interface{}, label string) error {
		switch items := value.(type) {
		case []string:
			for i, item := range items {
				resolved, err := resolve(item)
				if err != nil {
					return fmt.Errorf("resolve %s[%d]: %w", label, i, err)
				}
				items[i] = resolved
			}
		case []interface{}:
			for i, item := range items {
				ref, ok := item.(string)
				if !ok {
					continue
				}
				resolved, err := resolve(ref)
				if err != nil {
					return fmt.Errorf("resolve %s[%d]: %w", label, i, err)
				}
				items[i] = resolved
			}
		}
		return nil
	}

	for _, key := range []string{"scene_image_paths", "images"} {
		if value, ok := clone[key]; ok {
			if err := resolveStringList(value, key); err != nil {
				return nil, err
			}
		}
	}
	if items, ok := clone["items"].([]interface{}); ok {
		for i, raw := range items {
			item, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			if ref, ok := item["url"].(string); ok {
				resolved, err := resolve(ref)
				if err != nil {
					return nil, fmt.Errorf("resolve items[%d].url: %w", i, err)
				}
				item["url"] = resolved
			}
		}
	}
	if items, ok := clone["items"].([]map[string]interface{}); ok {
		for i, item := range items {
			if ref, ok := item["url"].(string); ok {
				resolved, err := resolve(ref)
				if err != nil {
					return nil, fmt.Errorf("resolve items[%d].url: %w", i, err)
				}
				item["url"] = resolved
			}
		}
	}
	resolveScene := func(scene map[string]interface{}, label string) error {
		for _, key := range []string{"image", "image_link"} {
			if ref, ok := scene[key].(string); ok {
				resolved, err := resolve(ref)
				if err != nil {
					return fmt.Errorf("resolve %s.%s: %w", label, key, err)
				}
				scene[key] = resolved
			}
		}
		if value, ok := scene["image_links"]; ok {
			return resolveStringList(value, label+".image_links")
		}
		return nil
	}
	if scenes, ok := clone["scenes"].([]interface{}); ok {
		for i, raw := range scenes {
			if scene, ok := raw.(map[string]interface{}); ok {
				if err := resolveScene(scene, fmt.Sprintf("scenes[%d]", i)); err != nil {
					return nil, err
				}
			}
		}
	}
	if scenes, ok := clone["scenes"].([]map[string]interface{}); ok {
		for i, scene := range scenes {
			if err := resolveScene(scene, fmt.Sprintf("scenes[%d]", i)); err != nil {
				return nil, err
			}
		}
	}
	if encoded, ok := clone["scenes_json"].(string); ok && encoded != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(encoded), &scenes); err == nil {
			for i, scene := range scenes {
				if err := resolveScene(scene, fmt.Sprintf("scenes_json[%d]", i)); err != nil {
					return nil, err
				}
			}
			if rewritten, err := json.Marshal(scenes); err == nil {
				clone["scenes_json"] = string(rewritten)
			}
		}
	}
	return clone, nil
}
