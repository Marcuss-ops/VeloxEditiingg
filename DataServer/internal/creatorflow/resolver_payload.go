package creatorflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
	"velox-shared/contract"
)

// BuildWorkerPayload validates and passes the producer-owned V2 plan through
// without rebuilding its timeline or resolving assets on the Master.
func BuildWorkerPayload(reqPayload map[string]interface{}) (map[string]interface{}, error) {
	flat := enqueue.FlattenPipelineResult(reqPayload)
	planJSON, jsonOK := flat[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	planSHA, shaOK := flat[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	if !jsonOK || !shaOK || strings.TrimSpace(planJSON) == "" || strings.TrimSpace(planSHA) == "" {
		return nil, fmt.Errorf("creatorflow: producer-compiled CompiledRenderPlanV2 is required")
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(flat); err != nil {
		return nil, fmt.Errorf("creatorflow: invalid CompiledRenderPlanV2: %w", err)
	}
	return cloneControlPlaneMap(flat), nil
}

// buildAndRewritePayload builds the worker payload from the raw remote
// result, optionally rewrites scene-image URLs for the public master,
// and injects the forwarding key. It returns an error if the payload
// cannot be built or rewritten.
func (r *Resolver) buildAndRewritePayload(reqPayload map[string]interface{}, fwdKey routing.ForwardingKey) (map[string]interface{}, error) {
	workerPayload, err := BuildWorkerPayload(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve validate worker payload: %w", err)
	}
	// Routing identity is control-plane metadata. The only worker payload
	// mutation is the deterministic forwarding key needed by enqueue.
	fwdKey.InjectIntoPayload(workerPayload)
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
