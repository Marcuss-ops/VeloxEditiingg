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

	"velox-server/internal/jobs/enqueue"
	"velox-shared/contract"
)

// workerSupportsCanonicalPayload is the single compatibility predicate for
// the executor capability negotiated during the worker Hello handshake. For
// the scene.composite executor, ExecutorVersion is the advertised payload
// contract capability (not an unrelated build version): versions below the
// canonical payload version, including unknown zero, are legacy; version 2+
// explicitly accepts the canonical payload contract.
func workerSupportsCanonicalPayload(executorVersion int) bool {
	return executorVersion >= contract.PayloadContractVersionCanonical
}

// projectPayloadForWorker selects the wire contract from the executor
// capability negotiated during the worker Hello handshake. Legacy workers
// receive a compatibility projection; canonical payloads are never mutated.
func projectPayloadForWorker(canonical map[string]interface{}, executorVersion int) (map[string]interface{}, error) {
	workerPayload := canonical
	if !workerSupportsCanonicalPayload(executorVersion) {
		var err error
		workerPayload, err = enqueue.ProjectLegacyWorkerPayload(canonical)
		if err != nil {
			return nil, err
		}
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
