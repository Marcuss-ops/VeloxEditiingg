package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// resolveTaskAssets is the only entry point for materialising transport-level
// velox-asset:// references inside a task payload. It composes the audio
// resolver and the scene-image resolver, returning the final payload the
// C++ engine can consume (filesystem paths + HTTP(S) only).
//
// The split keeps each resolver focused on a single media domain and the
// downloader/cache files focused on the transport — the bridge here is a
// pure orchestrator with no I/O knowledge of its own.
//
// Errors propagate unchanged so the observable error chain at the call site
// (task_dispatch.go:dispatchTaskRunner) stays byte-identical to the
// pre-split behaviour where the caller invoked resolveAudioPayload and
// resolveSceneImagePayload directly. Any top-level wrapping belongs to the
// caller — never to the orchestrator — to keep the refactor purely
// structural (zero comportamento/schema/API/protoc).
func (w *Worker) resolveTaskAssets(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	resolved, err := w.resolveAudioPayload(ctx, payload)
	if err != nil {
		return nil, err
	}
	resolved, err = w.resolveSceneImagePayload(ctx, resolved)
	if err != nil {
		return nil, err
	}
	// Older task envelopes may carry canonical media fields below a
	// parameters object. Resolve that nested payload as well so Chronon
	// never receives a master-only velox-asset:// URI.
	for _, nestedKey := range []string{"payload", "parameters"} {
		if encoded, ok := resolved[nestedKey].(string); ok && strings.HasPrefix(strings.TrimSpace(encoded), "{") {
			var decoded map[string]interface{}
			if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
				return nil, fmt.Errorf("decode nested %s payload: %w", nestedKey, err)
			}
			resolved[nestedKey] = decoded
		}
		if nested, ok := resolved[nestedKey].(map[string]interface{}); ok && nested != nil {
			resolvedNested, nestedErr := w.resolveTaskAssets(ctx, nested)
			if nestedErr != nil {
				return nil, nestedErr
			}
			resolved[nestedKey] = resolvedNested
		}
	}
	materialized, err := w.materializeVeloxAssetRefs(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if payload, ok := materialized.(map[string]interface{}); ok {
		return payload, nil
	}
	return resolved, nil
}

// materializeVeloxAssetRefs is the final transport boundary: task envelopes
// have appeared in several shapes over time, so every nested map/list is
// walked and every master-only velox-asset:// URI is replaced by a cached
// filesystem path before a renderer sees it.
func (w *Worker) materializeVeloxAssetRefs(ctx context.Context, value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		ref := strings.TrimSpace(v)
		if !strings.HasPrefix(ref, "velox-asset://") {
			return value, nil
		}
		assetID := strings.TrimPrefix(ref, "velox-asset://")
		if assetID == "" || strings.ContainsAny(assetID, `/\\`) {
			return nil, fmt.Errorf("invalid velox asset reference")
		}
		return w.downloadVeloxAsset(ctx, assetID)
	case map[string]interface{}:
		for key, item := range v {
			resolved, err := w.materializeVeloxAssetRefs(ctx, item)
			if err != nil {
				return nil, fmt.Errorf("resolve asset field %s: %w", key, err)
			}
			v[key] = resolved
		}
	case []interface{}:
		for i, item := range v {
			resolved, err := w.materializeVeloxAssetRefs(ctx, item)
			if err != nil {
				return nil, fmt.Errorf("resolve asset item %d: %w", i, err)
			}
			v[i] = resolved
		}
	}
	return value, nil
}
