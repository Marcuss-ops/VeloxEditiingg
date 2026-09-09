package pipeline

import (
	"context"
	"testing"
)

func TestComputeNativeRenderBudget(t *testing.T) {
	tests := []struct {
		cores, renders int
		want           NativeRenderBudget
	}{
		{8, 2, NativeRenderBudget{EffectiveCPUCores: 8, RenderCPUBudget: 3, DecoderThreads: 1, EncoderThreads: 2, SegmentWorkers: 1}},
		// 16 cores / 2 renders: 7 cores per render, 2 segment workers,
		// 3 threads per segment — below the decoder bump, so decode stays
		// at 1 and the encoder keeps the majority.
		{16, 2, NativeRenderBudget{EffectiveCPUCores: 16, RenderCPUBudget: 7, DecoderThreads: 1, EncoderThreads: 2, SegmentWorkers: 2}},
		{1, 8, NativeRenderBudget{EffectiveCPUCores: 1, RenderCPUBudget: 1, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 1}},
		// Large single-render budget: 7 threads per segment after 2 segment
		// workers → 2 decoders + 5 encoders per segment; decode no longer
		// pins at 1 thread on big hosts while the total stays at 15.
		{16, 1, NativeRenderBudget{EffectiveCPUCores: 16, RenderCPUBudget: 15, DecoderThreads: 2, EncoderThreads: 5, SegmentWorkers: 2}},
	}
	for _, tc := range tests {
		if got := ComputeNativeRenderBudget(tc.cores, tc.renders); got != tc.want {
			t.Errorf("ComputeNativeRenderBudget(%d, %d) = %+v, want %+v", tc.cores, tc.renders, got, tc.want)
		}
	}
}

func TestNativeRenderBudgetContext(t *testing.T) {
	want := ComputeNativeRenderBudget(8, 2)
	ctx := WithNativeRenderBudget(context.Background(), want)
	got, ok := NativeRenderBudgetFromContext(ctx)
	if !ok || got != want {
		t.Fatalf("budget from context = %+v, %v; want %+v, true", got, ok, want)
	}
}
