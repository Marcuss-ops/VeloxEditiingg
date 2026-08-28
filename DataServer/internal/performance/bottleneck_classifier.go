package performance

// Bottleneck classification is deliberately independent of HTTP and storage.
// Callers provide the measured window and receive one explainable verdict.
type Bottleneck string

const (
	BottleneckCPU     Bottleneck = "CPU_BOUND"
	BottleneckMemory  Bottleneck = "MEMORY_BOUND"
	BottleneckIO      Bottleneck = "IO_BOUND"
	BottleneckNetwork Bottleneck = "NETWORK_BOUND"
	BottleneckFD      Bottleneck = "FD_BOUND"
	BottleneckGPU     Bottleneck = "GPU_BOUND"
	BottleneckUnknown Bottleneck = "UNKNOWN"
)

type BottleneckObservation struct {
	CPUP95Percent      float64
	RunQueueP95        float64
	EffectiveCPUCores  float64
	IOWaitP95Percent   float64
	DiskUtilP95Percent float64
	MemoryPeakRatio    float64
	SwapDeltaBytes     int64
	MajorFaultsDelta   uint64
	FDUtilizationPeak  float64
	NetworkUtilization float64
	PublishWallRatio   float64
	GPUUtilization     float64
	EncoderUtilization float64
	VRAMPeakRatio      float64
}

type BottleneckResult struct {
	Kind       Bottleneck `json:"limiting_resource"`
	Confidence float64    `json:"confidence"`
	Evidence   []string   `json:"evidence"`
}

func ClassifyBottleneck(o BottleneckObservation) BottleneckResult {
	if o.MemoryPeakRatio >= .85 || o.SwapDeltaBytes > 0 {
		return BottleneckResult{BottleneckMemory, .95, []string{"memory safety threshold reached"}}
	}
	if o.FDUtilizationPeak >= .80 {
		return BottleneckResult{BottleneckFD, .90, []string{"file descriptor utilization reached 80%"}}
	}
	if o.GPUUtilization >= .90 || o.EncoderUtilization >= .90 || o.VRAMPeakRatio >= .90 {
		return BottleneckResult{BottleneckGPU, .90, []string{"GPU/encoder/VRAM saturation threshold reached"}}
	}
	if o.IOWaitP95Percent >= 25 && o.DiskUtilP95Percent >= 90 {
		return BottleneckResult{BottleneckIO, .90, []string{"iowait and disk utilization are saturated"}}
	}
	if o.NetworkUtilization >= .90 && o.PublishWallRatio >= 1.25 {
		return BottleneckResult{BottleneckNetwork, .85, []string{"network utilization and publish latency increased"}}
	}
	if o.CPUP95Percent >= 90 && o.RunQueueP95 >= o.EffectiveCPUCores && o.IOWaitP95Percent < 15 {
		return BottleneckResult{BottleneckCPU, .93, []string{"CPU p95 and run queue indicate saturation"}}
	}
	return BottleneckResult{Kind: BottleneckUnknown, Confidence: 0, Evidence: []string{"no resource safety threshold crossed"}}
}
