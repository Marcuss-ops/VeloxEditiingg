package pipeline

import "context"

// NativeRenderBudget is the per-render CPU allocation passed to the native
// engine. It prevents concurrent renders from each claiming the whole host.
type NativeRenderBudget struct {
	EffectiveCPUCores int
	RenderCPUBudget   int
	DecoderThreads    int
	EncoderThreads    int
	SegmentWorkers    int
}

// ComputeNativeRenderBudget divides usable CPU capacity across concurrent
// renders and retains one core for worker/control-plane work.
func ComputeNativeRenderBudget(effectiveCores, maxConcurrentRenders int) NativeRenderBudget {
	if effectiveCores < 1 {
		effectiveCores = 1
	}
	if maxConcurrentRenders < 1 {
		maxConcurrentRenders = 1
	}
	usable := effectiveCores - 1
	if usable < 1 {
		usable = 1
	}
	perRender := usable / maxConcurrentRenders
	if perRender < 1 {
		perRender = 1
	}
	segmentWorkers := 1
	if perRender >= 6 {
		segmentWorkers = 2
	}
	threadsPerSegment := perRender / segmentWorkers
	if threadsPerSegment < 1 {
		threadsPerSegment = 1
	}
	decoderThreads := 1
	encoderThreads := threadsPerSegment - decoderThreads
	if encoderThreads < 1 {
		encoderThreads = 1
	}
	return NativeRenderBudget{
		EffectiveCPUCores: effectiveCores,
		RenderCPUBudget:   perRender,
		DecoderThreads:    decoderThreads,
		EncoderThreads:    encoderThreads,
		SegmentWorkers:    segmentWorkers,
	}
}

type nativeRenderBudgetContextKey struct{}

func WithNativeRenderBudget(ctx context.Context, budget NativeRenderBudget) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if budget.EffectiveCPUCores < 1 || budget.RenderCPUBudget < 1 || budget.DecoderThreads < 1 || budget.EncoderThreads < 1 || budget.SegmentWorkers < 1 {
		budget = ComputeNativeRenderBudget(budget.EffectiveCPUCores, 1)
	}
	return context.WithValue(ctx, nativeRenderBudgetContextKey{}, budget)
}

func NativeRenderBudgetFromContext(ctx context.Context) (NativeRenderBudget, bool) {
	if ctx == nil {
		return NativeRenderBudget{}, false
	}
	budget, ok := ctx.Value(nativeRenderBudgetContextKey{}).(NativeRenderBudget)
	return budget, ok
}
