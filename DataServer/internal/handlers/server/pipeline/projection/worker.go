// Package projection owns the pure canonical-to-worker projection boundary.
// It accepts only the raw canonical submission map and returns the renderer
// payload, keeping HTTP handlers and intake DTOs out of this layer.
package projection

import (
	"fmt"

	"velox-server/internal/remoteengine"
	"velox-server/internal/routing"
	"velox-shared/assetref"
	"velox-shared/contract"
)

// ProjectWorkerPayload parses the canonical submission map through the typed
// remote-engine DTO and emits the renderer-only worker payload. Delivery and
// publication control-plane fields are removed by the DTO projection.
// rendererMode is supplied by the intake layer so this package does not depend
// on pipeline recipe or request types. The legacy clip_stock timeline
// projection (BuildClipStockTimeline → items/audio_tracks for hybrid.v1) was
// retired together with the hybrid.v1 worker pipeline.
func ProjectWorkerPayload(rawPayload map[string]interface{}, rendererMode string) (map[string]interface{}, error) {
	dto, err := remoteengine.ParseRemotePipelineResult(rawPayload)
	if err != nil {
		return nil, fmt.Errorf("parse canonical submission: %w", err)
	}
	workerPayload, projectionErr := dto.ToWorkerPayloadChecked()
	if projectionErr != nil {
		return nil, fmt.Errorf("project canonical submission: %w", projectionErr)
	}
	// The worker's scene.composite executor now requires an explicit
	// single-source pipeline id.  clip.stock.v1 is the canonical intake
	// recipe for clip timelines, so route it to the registered clips.v1
	// compiler instead of emitting an empty pipeline_id (or the retired
	// hybrid.v1 path).
	switch rendererMode {
	case "clip_stock":
		setPipelineID(workerPayload, "clips.v1")
		ensureClipPipelineInputs(workerPayload, rawPayload)
	case "scene_image", "slideshow":
		setPipelineID(workerPayload, "images.v1")
	}
	preserveFields(workerPayload, rawPayload, "layers", "_placement_pin_worker_id")
	return workerPayload, nil
}

func ensureClipPipelineInputs(dst, raw map[string]interface{}) {
	if dst == nil || raw == nil {
		return
	}
	// Typed JSON projection may emit the clips key as null. Treat null and
	// empty arrays as missing so canonical scene clips are still normalized
	// into the renderer-facing clips.v1 payload.
	existingClips, hasExistingClips := dst["clips"].([]interface{})
	if !hasExistingClips || len(existingClips) == 0 {
		scenes, ok := sceneList(raw["scenes"])
		if !ok {
			scenes, ok = sceneList(dst["scenes"])
		}
		if !ok {
			if encoded, encodedOK := raw["scenes_json"].(string); encodedOK {
				if parsed, err := contract.ParseSceneMapsJSON([]byte(encoded)); err == nil {
					scenes = make([]interface{}, 0, len(parsed))
					for _, scene := range parsed {
						scenes = append(scenes, scene)
					}
					ok = true
				}
			}
		}
		if ok {
			clips := make([]interface{}, 0, len(scenes))
			for _, value := range scenes {
				scene, ok := value.(map[string]interface{})
				if !ok {
					continue
				}
				clip, ok := scene["clip"].(map[string]interface{})
				if !ok {
					continue
				}
				url, _ := clip["url"].(string)
				if url == "" {
					if assetID, assetOK := clip["asset_id"].(string); assetOK && assetID != "" {
						if ref, err := assetref.NewDeferredDrive(assetID); err == nil {
							url = ref.Wire()
						}
					}
				}
				if url == "" {
					continue
				}
				clips = append(clips, map[string]interface{}{
					"url":      url,
					"duration": scene["duration_seconds"],
				})
			}
			if len(clips) > 0 {
				dst["clips"] = clips
			}
		}
	}
	if _, ok := dst["audio_url"]; !ok {
		if audio, ok := raw["audio_url"].(string); ok && audio != "" {
			dst["audio_url"] = audio
		}
	}
}

// sceneList accepts both JSON-decoded []interface{} and the []map form used
// by the typed intake DTO. Both forms are present in production depending on
// whether the payload came directly from HTTP or from the canonical request
// projection.
func sceneList(value interface{}) ([]interface{}, bool) {
	switch scenes := value.(type) {
	case []interface{}:
		return scenes, true
	case []map[string]interface{}:
		out := make([]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, scene)
		}
		return out, true
	default:
		return nil, false
	}
}

// setPipelineID keeps the routing value in the internal metadata key until
// the enqueue normalizer projects it into the renderer-facing pipeline_id.
// The public alias is retained for direct worker-payload consumers.
func setPipelineID(payload map[string]interface{}, id string) {
	if payload == nil || id == "" {
		return
	}
	payload[routing.KeyPipelineID] = id
	payload["pipeline_id"] = id
}

func preserveFields(dst, src map[string]interface{}, keys ...string) {
	if dst == nil || src == nil {
		return
	}
	for _, key := range keys {
		if value, ok := src[key]; ok && value != nil {
			dst[key] = value
		}
	}
}
