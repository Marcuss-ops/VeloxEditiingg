package worker

import (
	"context"
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
	return resolved, nil
}
