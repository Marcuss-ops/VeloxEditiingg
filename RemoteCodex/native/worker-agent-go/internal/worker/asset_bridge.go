package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	if payload == nil {
		return nil, nil
	}
	// Older task envelopes may carry JSON-encoded canonical payloads below
	// payload/parameters. Deep-copy before decoding so the caller's task
	// payload remains immutable while integrity declarations are indexed.
	copied, err := deepCopyAssetValue(payload)
	if err != nil {
		return nil, fmt.Errorf("copy task asset payload: %w", err)
	}
	resolved, ok := copied.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("copy task asset payload: object expected")
	}
	for _, nestedKey := range []string{"payload", "parameters"} {
		if encoded, ok := resolved[nestedKey].(string); ok && strings.HasPrefix(strings.TrimSpace(encoded), "{") {
			var decoded map[string]interface{}
			if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
				return nil, fmt.Errorf("decode nested %s payload: %w", nestedKey, err)
			}
			resolved[nestedKey] = decoded
		}
	}
	originalAudioRef, _ := resolved["audio_url"].(string)
	resolved, err = w.resolveCommonAssetPayload(ctx, resolved)
	if err != nil {
		return nil, err
	}
	// Legacy clips.v1 consumes audio_url directly. Keep an explicit
	// post-condition at this boundary: a transport reference must never reach
	// the pipeline compiler even if a legacy envelope shape bypassed the
	// generic recursive field classifier. Reuse the verified bridge so this is
	// idempotent for already-local paths and preserves cache accounting.
	if audioRef, ok := resolved["audio_url"].(string); ok && strings.HasPrefix(strings.TrimSpace(audioRef), "velox-") {
		localPath, err := w.resolveVoiceoverAudioPath(ctx, audioRef, resolved)
		if err != nil {
			return nil, fmt.Errorf("resolve audio_url: %w", err)
		}
		resolved["audio_url"] = localPath
	} else if strings.HasPrefix(strings.TrimSpace(originalAudioRef), "velox-") && !regularFile(resolved["audio_url"]) {
		// A cache index can outlive an evicted blob. Re-resolve from the
		// original immutable reference so the renderer never receives a
		// stale local path from a cache-hit projection.
		localPath, err := w.resolveVoiceoverAudioPath(ctx, originalAudioRef, resolved)
		if err != nil {
			return nil, fmt.Errorf("re-resolve audio_url: %w", err)
		}
		resolved["audio_url"] = localPath
	}
	return resolved, nil
}

func regularFile(value interface{}) bool {
	path, ok := value.(string)
	if !ok || strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
