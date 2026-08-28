package performance

import "testing"

func TestClassifyBottleneckCPU(t *testing.T) {
	r := ClassifyBottleneck(BottleneckObservation{CPUP95Percent: 96, RunQueueP95: 9, EffectiveCPUCores: 8, IOWaitP95Percent: 3})
	if r.Kind != BottleneckCPU {
		t.Fatalf("kind = %q", r.Kind)
	}
}

func TestClassifyBottleneckMemoryPrecedesCPU(t *testing.T) {
	r := ClassifyBottleneck(BottleneckObservation{MemoryPeakRatio: .91, CPUP95Percent: 99})
	if r.Kind != BottleneckMemory {
		t.Fatalf("kind = %q", r.Kind)
	}
}
