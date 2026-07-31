package telemetry

import "context"

type recorderContextKey struct{}

// WithRecorder binds an attempt-scoped recorder to a context. The recorder
// is intentionally not global: concurrent attempts must never share indexes.
func WithRecorder(ctx context.Context, recorder *EventRecorder) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, recorderContextKey{}, recorder)
}

// RecorderFromContext returns the attempt recorder bound to ctx, if any.
func RecorderFromContext(ctx context.Context) *EventRecorder {
	if ctx == nil {
		return nil
	}
	recorder, _ := ctx.Value(recorderContextKey{}).(*EventRecorder)
	return recorder
}
