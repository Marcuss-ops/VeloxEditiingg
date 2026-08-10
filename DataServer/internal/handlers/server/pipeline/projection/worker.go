// Package projection owns the pure canonical-to-worker projection boundary.
// It accepts only the raw canonical submission map and returns the renderer
// payload, keeping HTTP handlers and intake DTOs out of this layer.
package projection

import (
	"fmt"
	"strings"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
)

// ProjectWorkerPayload parses the canonical submission map through the typed
// remote-engine DTO and emits the renderer-only worker payload. Delivery and
// publication control-plane fields are removed by the DTO projection.
// rendererMode is supplied by the intake layer so this package does not depend
// on pipeline recipe or request types.
func ProjectWorkerPayload(rawPayload map[string]interface{}, rendererMode string) (map[string]interface{}, error) {
	dto, err := remoteengine.ParseRemotePipelineResult(rawPayload)
	if err != nil {
		return nil, fmt.Errorf("parse canonical submission: %w", err)
	}
	workerPayload, projectionErr := dto.ToWorkerPayloadChecked()
	if projectionErr != nil {
		return nil, fmt.Errorf("project canonical submission: %w", projectionErr)
	}
	preserveFields(workerPayload, rawPayload, "audio_tracks", "layers", "_placement_pin_worker_id")
	if strings.EqualFold(rendererMode, "clip_stock") {
		items, audioTracks, timelineErr := enqueue.BuildClipStockTimeline(rawPayload)
		if timelineErr != nil {
			return nil, fmt.Errorf("build clip-stock timeline: %w", timelineErr)
		}
		if len(items) > 0 {
			workerPayload["items"] = items
		}
		if len(audioTracks) > 0 {
			workerPayload["audio_tracks"] = audioTracks
		}
	}
	return workerPayload, nil
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
