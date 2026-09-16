package pipeline

import (
	"context"
	"testing"
)

func TestComputeNativeRenderBudget(t *testing.T) {
	tests := []struct {
		name           string
		cores, renders int
		want           NativeRenderBudget
	}{
		{name: "8 cores / 2 renders", cores: 8, renders: 2,
			want: NativeRenderBudget{EffectiveCPUCores: 8, RenderCPUBudget: 3, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 2}},
		// 16 cores / 2 renders: 7 cores per render, 2 segment workers,
		// 3 threads per segment — below the decoder bump, so decode stays
		// at 1 and the encoder keeps the majority.
		{name: "16 cores / 2 renders", cores: 16, renders: 2,
			want: NativeRenderBudget{EffectiveCPUCores: 16, RenderCPUBudget: 7, DecoderThreads: 1, EncoderThreads: 2, SegmentWorkers: 2}},
		{name: "1 core / 8 renders never oversubscribes", cores: 1, renders: 8,
			want: NativeRenderBudget{EffectiveCPUCores: 1, RenderCPUBudget: 1, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 1}},
		// Large single-render budget: the computed default stays conservative
		// (two in-flight clips, 2 decoders + 5 encoders per clip). Widening it
		// is an explicit, benchmark-certified operator decision — see the
		// override tests below.
		{name: "16 cores / 1 render keeps the conservative default", cores: 16, renders: 1,
			want: NativeRenderBudget{EffectiveCPUCores: 16, RenderCPUBudget: 15, DecoderThreads: 2, EncoderThreads: 5, SegmentWorkers: 2}},
		// 64 cores / 1 render under the conservative default: two clips, each
		// claiming 31 threads (2 decoders + 29 encoders). That is the shape of
		// the pre-existing default, and it is exactly why the operator override
		// below exists — 8 clips x 7 threads beats 2 clips x 31 on a host this
		// wide. The budget only reports it; widening it is the certified
		// operator decision, never an implicit default change.
		{name: "64 cores / 1 render keeps the conservative default", cores: 64, renders: 1,
			want: NativeRenderBudget{EffectiveCPUCores: 64, RenderCPUBudget: 63, DecoderThreads: 2, EncoderThreads: 29, SegmentWorkers: 2}},
		{name: "4 cores / 1 render uses the two-worker floor", cores: 4, renders: 1,
			want: NativeRenderBudget{EffectiveCPUCores: 4, RenderCPUBudget: 3, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 2}},
		{name: "2 cores / 1 render is single-worker", cores: 2, renders: 1,
			want: NativeRenderBudget{EffectiveCPUCores: 2, RenderCPUBudget: 1, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 1}},
		{name: "zero inputs are normalised", cores: 0, renders: 0,
			want: NativeRenderBudget{EffectiveCPUCores: 1, RenderCPUBudget: 1, DecoderThreads: 1, EncoderThreads: 1, SegmentWorkers: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envSegmentWorkers, "")
			got := ComputeNativeRenderBudget(tc.cores, tc.renders)
			if got != tc.want {
				t.Fatalf("ComputeNativeRenderBudget(%d, %d) = %+v, want %+v", tc.cores, tc.renders, got, tc.want)
			}
			assertBudgetReservationFits(t, got)
		})
	}
}

// assertBudgetReservationFits pins the engine's real admission contract: one
// claim per segment worker, and that claim is max(decoder_threads,
// encoder_threads) — decode and encode share the same frame pipeline, they do
// NOT add up (render_engine_timeline.cpp: SegmentResourceClaim{
// max(decoder_threads, encoder_threads), 0}). Computing the claim as a SUM
// would over-report the reservation and mis-size the budget.
func assertBudgetReservationFits(t *testing.T, got NativeRenderBudget) {
	t.Helper()
	claim := got.DecoderThreads
	if got.EncoderThreads > claim {
		claim = got.EncoderThreads
	}
	if reserved := got.SegmentWorkers * claim; reserved > got.RenderCPUBudget {
		t.Fatalf("segment reservation %d (%d workers x %d threads) exceeds the render budget %d",
			reserved, got.SegmentWorkers, claim, got.RenderCPUBudget)
	}
	if got.SegmentWorkers > maxParallelSegmentWorkers {
		t.Fatalf("SegmentWorkers = %d exceeds the engine cap %d", got.SegmentWorkers, maxParallelSegmentWorkers)
	}
}

// TestComputeNativeRenderBudgetHonoursOperatorSegmentWorkers pins the fix for
// the oversubscription trap: raising VELOX_NATIVE_SEGMENT_WORKERS must re-split
// the SAME per-render budget across the requested number of clips. Before this,
// the engine honoured the wider worker count while the Go side kept budgeting
// threads for two workers, so 8 workers each took the two-worker thread share.
func TestComputeNativeRenderBudgetHonoursOperatorSegmentWorkers(t *testing.T) {
	t.Setenv(envSegmentWorkers, "4")
	got := ComputeNativeRenderBudget(16, 1)
	if got.SegmentWorkers != 4 {
		t.Fatalf("SegmentWorkers = %d, want the operator value 4", got.SegmentWorkers)
	}
	// 15 usable cores / 4 workers = 3 threads per clip (1 decoder + 2 encoders),
	// i.e. the budget is re-divided, not multiplied.
	if got.DecoderThreads != 1 || got.EncoderThreads != 2 {
		t.Fatalf("thread split = %d dec / %d enc, want 1/2 after re-splitting the budget",
			got.DecoderThreads, got.EncoderThreads)
	}
	assertBudgetReservationFits(t, got)

	t.Setenv(envSegmentWorkers, "8")
	got = ComputeNativeRenderBudget(16, 1)
	if got.SegmentWorkers != 8 || got.EncoderThreads != 1 {
		t.Fatalf("8 workers on a 15-core budget = %+v, want 8 workers x 1 encoder thread", got)
	}
	assertBudgetReservationFits(t, got)
}

// TestComputeNativeRenderBudgetClampsOperatorSegmentWorkers pins that an
// out-of-range or nonsensical override fails safe instead of oversubscribing.
func TestComputeNativeRenderBudgetClampsOperatorSegmentWorkers(t *testing.T) {
	// Above the engine cap: clamped to the cap and the budget is re-split.
	t.Setenv(envSegmentWorkers, "64")
	got := ComputeNativeRenderBudget(16, 1)
	if got.SegmentWorkers != maxParallelSegmentWorkers {
		t.Fatalf("SegmentWorkers = %d, want the engine cap %d", got.SegmentWorkers, maxParallelSegmentWorkers)
	}
	assertBudgetReservationFits(t, got)

	// Wider than this render's own budget: clamped to the budget, never more
	// workers than cores to put them on.
	t.Setenv(envSegmentWorkers, "6")
	got = ComputeNativeRenderBudget(4, 1)
	if got.SegmentWorkers != 3 {
		t.Fatalf("SegmentWorkers = %d on a 3-core budget, want the budget clamp 3", got.SegmentWorkers)
	}
	assertBudgetReservationFits(t, got)

	// Garbage / zero / negative values fall back to the computed default.
	for _, value := range []string{"", "  ", "banana", "0", "-3"} {
		t.Setenv(envSegmentWorkers, value)
		got := ComputeNativeRenderBudget(16, 1)
		if got.SegmentWorkers != defaultParallelSegmentWorkers {
			t.Fatalf("override %q → SegmentWorkers %d, want the default %d",
				value, got.SegmentWorkers, defaultParallelSegmentWorkers)
		}
		assertBudgetReservationFits(t, got)
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

// TestWithNativeRenderBudgetRejectsInvalidBudget pins the fail-closed path: an
// unusable budget (any zeroed field) is replaced by the computed one instead of
// being handed to the engine.
func TestWithNativeRenderBudgetRejectsInvalidBudget(t *testing.T) {
	ctx := WithNativeRenderBudget(context.Background(), NativeRenderBudget{EffectiveCPUCores: 8})
	got, ok := NativeRenderBudgetFromContext(ctx)
	if !ok {
		t.Fatal("budget missing from context")
	}
	if want := ComputeNativeRenderBudget(8, 1); got != want {
		t.Fatalf("invalid budget was not replaced: got %+v, want %+v", got, want)
	}
	if _, ok := NativeRenderBudgetFromContext(nil); ok {
		t.Fatal("nil context must not yield a budget")
	}
}
