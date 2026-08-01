package enqueue

import "context"

// NormalizeSceneVideoPayload applies the canonical scene-video normalization
// used by Enqueuer before persistence. The adapter keeps the implementation
// private while allowing ingress contract tests and composition layers to
// exercise the exact production normalizer.
func NormalizeSceneVideoPayload(payload map[string]interface{}) (map[string]interface{}, error) {
	return normalizeSceneVideoPayloadContext(context.Background(), payload)
}

// NormalizeSceneVideoPayloadContext is the context-aware form used when
// strict render-manifest compilation must observe request cancellation.
func NormalizeSceneVideoPayloadContext(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	return normalizeSceneVideoPayloadContext(ctx, payload)
}
