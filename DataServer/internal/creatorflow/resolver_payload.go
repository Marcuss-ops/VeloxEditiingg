package creatorflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
)

// BuildWorkerPayload builds the creatorflow worker payload through the same
// canonical pipeline builder used by Resolver.Resolve. It has no database or
// network side effects and is useful for cross-ingress contract checks.
func BuildWorkerPayload(reqPayload map[string]interface{}) (map[string]interface{}, error) {
	return enqueue.BuildPipelinePayload(reqPayload)
}

// buildAndRewritePayload builds the worker payload from the raw remote
// result, optionally rewrites scene-image URLs for the public master,
// and injects the forwarding key. It returns an error if the payload
// cannot be built or rewritten.
func (r *Resolver) buildAndRewritePayload(reqPayload map[string]interface{}, fwdKey routing.ForwardingKey) (map[string]interface{}, error) {
	workerPayload, err := BuildWorkerPayload(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve build worker payload: %w", err)
	}
	// BuildPipelinePayload intentionally rebuilds a renderer map from the
	// typed V2 envelope. Re-attach routing metadata after that projection so
	// pipeline selection cannot disappear before PrepareJobAndTask persists
	// the TaskSpec. This is especially important for clip.stock.v1, whose
	// worker executor requires the explicit clips.v1 pipeline.
	routing.FromPayload(reqPayload).InjectIntoPayload(workerPayload)

	// Skip rewriting when the resolver was constructed without
	// dataDir+masterURL (in-runner path; the remote engine already
	// produced a complete result). Also skip when the payload has no
	// images / scene_image_paths — the scene_image rewrite forces
	// video_mode=scene_image which requires per-scene images. Payloads
	// from POST /api/v1/jobs with audio_tracks but no images would
	// fail the worker render without this guard.
	needsImageRewrite := hasImagesForRewrite(reqPayload)
	if r.dataDir != "" && r.masterURL != "" && needsImageRewrite {
		workerPayload, err = enqueue.BuildSceneImagePayloadForMaster(workerPayload, r.dataDir, r.videosDir, r.masterURL, r.driveResolver)
		if err != nil {
			return nil, fmt.Errorf("creatorflow: Resolve rewrite master URL: %w", err)
		}
		// BuildSceneImagePayloadForMaster creates a fresh typed V2
		// envelope that drops timeline fields. Preserve audio_tracks,
		// layers, etc. from the pre-rewrite payload.
		enqueue.CopyTimelinePayloadFields(workerPayload, reqPayload)
	}

	// Re-inject the forwarding key into the rewritten payload — both
	// BuildPipelinePayload and BuildSceneImagePayloadForMaster produce
	// fresh maps that drop the originally-injected key. This is the
	// same step the legacy Service.ForwardCompleted performed.
	fwdKey.InjectIntoPayload(workerPayload)

	// Re-inject _placement_pin_worker_id from the original payload —
	// BuildPipelinePayload creates a fresh map that drops per-job
	// operator fields. Without this, placement_pin_worker_id in
	// SubmitJobRequest silently fails to route to the pinned worker.
	if pin, ok := reqPayload["_placement_pin_worker_id"].(string); ok && pin != "" {
		workerPayload["_placement_pin_worker_id"] = pin
	}

	return workerPayload, nil
}

// resolverMarshalPayload serializes a worker payload map to canonical
// JSON + SHA-256. Empty inputs yield a literal "{}" payload — the
// caller decides whether empty sha is a fatal input error. Mirrors the
// runner's marshalPayload semantics so the two paths produce identical
// payload_json/payload_sha256 bytes for the same input map.
func resolverMarshalPayload(result map[string]interface{}) (payloadJSON, payloadSHA256 string) {
	if result == nil {
		raw := []byte("{}")
		return string(raw), sha256HexResolver(raw)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return "", ""
	}
	return string(raw), sha256HexResolver(raw)
}

func sha256HexResolver(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// hasImagesForRewrite returns true when the payload carries image
// references that need master-URL rewriting. Payloads without images
// (e.g. voiceover-only or audio_tracks-only from POST /api/v1/jobs)
// skip the scene_image rewrite, avoiding video_mode=scene_image which
// would require per-scene images the worker can't satisfy.
func hasImagesForRewrite(payload map[string]interface{}) bool {
	for _, key := range []string{"images", "image_links", "scene_image_paths", "image_paths"} {
		switch v := payload[key].(type) {
		case []interface{}:
			if len(v) > 0 {
				return true
			}
		case []string:
			if len(v) > 0 {
				return true
			}
		}
	}
	// Check scenes for image_link references.
	if scenes, ok := payload["scenes"].([]interface{}); ok {
		for _, s := range scenes {
			if scene, ok := s.(map[string]interface{}); ok {
				if img, ok := scene["image_link"].(string); ok && img != "" {
					return true
				}
			}
		}
	}
	return false
}
