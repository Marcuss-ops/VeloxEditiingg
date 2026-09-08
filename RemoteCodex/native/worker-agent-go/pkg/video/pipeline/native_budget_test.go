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
		{16, 2, NativeRenderBudget{EffectiveCPUCores: 16, RenderCPUBudget: 7, DecoderThreads: 1, EncoderThreads: 2, SegmentWorkers: 2}},
		{1, 8, NativeRenderBudget{EffectiveCPUCores: 1, RenderCPUBudget: 1, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 1}},
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
