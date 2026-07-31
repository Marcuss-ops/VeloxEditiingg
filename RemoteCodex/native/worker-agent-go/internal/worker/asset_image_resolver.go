package worker

import "context"

// resolveSceneImagePayload is retained as a compatibility wrapper. Video,
// image and scene media references are now resolved by the common bridge so
// they receive the same velox-asset://, SHA-256 and size_bytes policy as
// audio, music, effects and subtitles.
func (w *Worker) resolveSceneImagePayload(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	return w.resolveCommonAssetPayload(ctx, payload)
}
