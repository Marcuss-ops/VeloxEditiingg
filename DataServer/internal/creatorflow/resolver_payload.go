package creatorflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
)

// buildAndRewritePayload builds the worker payload from the raw remote
// result, optionally rewrites scene-image URLs for the public master,
// and injects the forwarding key. It returns an error if the payload
// cannot be built or rewritten.
func (r *Resolver) buildAndRewritePayload(reqPayload map[string]interface{}, fwdKey routing.ForwardingKey) (map[string]interface{}, error) {
	workerPayload, err := enqueue.BuildPipelinePayload(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve build worker payload: %w", err)
	}

	// Skip rewriting when the resolver was constructed without
	// dataDir+masterURL (in-runner path; the remote engine already
	// produced a complete result).
	if r.dataDir != "" && r.masterURL != "" {
		workerPayload, err = enqueue.BuildSceneImagePayloadForMaster(workerPayload, r.dataDir, r.videosDir, r.masterURL)
		if err != nil {
			return nil, fmt.Errorf("creatorflow: Resolve rewrite master URL: %w", err)
		}
	}

	// Re-inject the forwarding key into the rewritten payload — both
	// BuildPipelinePayload and BuildSceneImagePayloadForMaster produce
	// fresh maps that drop the originally-injected key. This is the
	// same step the legacy Service.ForwardCompleted performed.
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
