// Package grpcserver / handler_payload_projection.go
//
// Wire-contract projection for TaskOffers: selects the canonical vs
// legacy payload shape from the executor capability negotiated during
// the worker Hello handshake. Extracted from handler_workers.go
// (split per responsabilità).
package grpcserver

import (
	"encoding/json"
	"fmt"

	"velox-shared/contract"
)

// projectPayloadForWorker emits the canonical wire contract for every worker.
// Legacy worker projections were removed; all admitted workers now consume
// the same scene.composite payload and therefore cannot create a second audio
// timeline during compatibility translation.
func projectPayloadForWorker(canonical map[string]interface{}, executorVersion int) (map[string]interface{}, error) {
	_ = executorVersion
	workerPayload, err := contract.RenderOnlyPayload(canonical)
	if err != nil {
		return nil, fmt.Errorf("project render-only worker payload: %w", err)
	}

	// The protobuf Struct boundary accepts JSON-native arrays/maps. The
	// enqueue adapter intentionally preserves Go collection types for its
	// package-level compatibility tests, so normalize only this wire copy.
	encoded, err := json.Marshal(workerPayload)
	if err != nil {
		return nil, fmt.Errorf("encode worker payload: %w", err)
	}
	var wirePayload map[string]interface{}
	if err := json.Unmarshal(encoded, &wirePayload); err != nil {
		return nil, fmt.Errorf("decode worker payload: %w", err)
	}
	return wirePayload, nil
}
