package pipeline

import (
	"context"
	"os"
	"strconv"
	"strings"
)

// NativeRenderBudget is the per-render CPU allocation passed to the native
// engine. It prevents concurrent renders from each claiming the whole host.
type NativeRenderBudget struct {
	EffectiveCPUCores int
	RenderCPUBudget   int
	DecoderThreads    int
	EncoderThreads    int
	SegmentWorkers    int
} // Segment-worker policy constants.
const (
	// maxParallelSegmentWorkers is the native engine's own admission cap for
	// VELOX_NATIVE_SEGMENT_WORKERS (std::min(parsed, 8) in
	// render_engine_timeline.cpp). The budget must never advertise more.
	maxParallelSegmentWorkers = 8
	// defaultParallelSegmentWorkers is the conservative computed default: two
	// in-flight clips once a render owns at least three cores. Raising it is a
	// per-host, benchmark-certified decision (docs/performance-gates.md tier 2 +
	// docs/100-percent-plan/parallelism-certification.md), which the operator
	// makes with VELOX_NATIVE_SEGMENT_WORKERS. The budget does not widen it on
	// its own: more in-flight segments also multiply frame-pool memory, and
	// that trade is a measurement, not a guess.
	defaultParallelSegmentWorkers = 2
)

// envSegmentWorkers is the operator knob for in-flight clips per render. It is
// read here (not only in the engine) because the per-segment THREAD split must
// follow the actual worker count: the engine clamps the worker count to 8 but
// knows nothing about the CPU budget this function divides, so ignoring the
// override here produced an oversubscribed host (e.g. 8 workers sharing a
// 15-core split still budgeted for 2 workers → 8 × 6 encoder threads).
const envSegmentWorkers = "VELOX_NATIVE_SEGMENT_WORKERS"

// ComputeNativeRenderBudget divides usable CPU capacity across concurrent
// renders and retains one core for worker/control-plane work.

// An explicit VELOX_NATIVE_SEGMENT_WORKERS in the worker environment remains
// authoritative (the engine reads it and the computed values only fill absent
// knobs — engine_process.go setEnvIfAbsent); this function now also lets that
// override drive the thread split, so raising concurrency cannot silently
// oversubscribe the host.
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
	// Keep at least two independent segment workers once a render has three
	// usable cores. This avoids serialising dozens of short clips while still
	// reserving one core for worker/control-plane work.
	segmentWorkers := 1
	if perRender >= 3 {
		segmentWorkers = defaultParallelSegmentWorkers
	}
	// Operator override, clamped by BOTH the engine cap and this render's own
	// budget: a request wider than the budget would otherwise oversubscribe
	// the host instead of failing closed.
	if requested := strings.TrimSpace(os.Getenv(envSegmentWorkers)); requested != "" {
		if parsed, err := strconv.Atoi(requested); err == nil && parsed > 0 {
			segmentWorkers = parsed
			if segmentWorkers > maxParallelSegmentWorkers {
				segmentWorkers = maxParallelSegmentWorkers
			}
			if segmentWorkers > perRender {
				segmentWorkers = perRender
			}
		}
	}
	threadsPerSegment := perRender / segmentWorkers
	if threadsPerSegment < 1 {
		threadsPerSegment = 1
	}
	// Split the per-segment budget across decode and encode. Decode stays at
	// 1 thread only while the budget is small; once threadsPerSegment grows,
	// one extra decoder thread prevents decode from starving the encode
	// pipeline on high-resolution sources (4K HEVC), where a single decode
	// thread cannot feed many encoder threads. Keep the encoder majority so
	// x264 stays the primary consumer.
	decoderThreads := 1
	if threadsPerSegment >= 6 {
		decoderThreads = 2
	}
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
