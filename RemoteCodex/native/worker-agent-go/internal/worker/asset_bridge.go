package worker

import (
	"context"
	"fmt"
)

// resolveTaskAssets is the only entry point for materialising transport-level
// velox-asset:// references inside a task payload. It composes the audio
// resolver and the scene-image resolver, returning the final payload the
// C++ engine can consume (filesystem paths + HTTP(S) only).
//
// The split keeps each resolver focused on a single media domain and the
// downloader/cache files focused on the transport — the bridge here is a
// pure orchestrator with no I/O knowledge of its own.
func (w *Worker) resolveTaskAssets(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	resolved, err := w.resolveAudioPayload(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("resolve task audio assets: %w", err)
	}
	resolved, err = w.resolveSceneImagePayload(ctx, resolved)
	if err != nil {
		return nil, fmt.Errorf("resolve task scene-image assets: %w", err)
	}
	return resolved, nil
}
