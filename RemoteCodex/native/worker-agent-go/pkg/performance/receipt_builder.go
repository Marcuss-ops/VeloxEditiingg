package performance

// receipt_builder.go is the single PerformanceReceiptAssembler boundary.
// RunMetrics and AttemptSnapshot are input adapters; neither owns receipt
// construction or derived KPI formulas.

type receiptAssemblyInput struct {
	Identity      PerformanceIdentity
	Workload      WorkloadProfile
	Timing        TimingMetrics
	Process       ProcessMetrics
	CPU           CPUMetrics
	IO            IOMetrics
	Media         MediaMetrics
	Memory        MemoryMetrics
	Scheduling    SchedulingMetrics
	FramePipeline FramePipelineMetrics
	Phases        []PhaseTiming
	Segments      []SegmentTiming
	Coverage      *TelemetryCoverage
	Raw           RawMetrics
}

func assemblePerformanceReceipt(input receiptAssemblyInput) *PerformanceReceiptV1 {
	receipt := NewPerformanceReceiptV1()
	receipt.Identity = input.Identity
	receipt.Workload = input.Workload
	receipt.Timing = input.Timing
	receipt.Process = input.Process
	receipt.CPU = input.CPU
	receipt.IO = input.IO
	receipt.Media = input.Media
	receipt.Memory = input.Memory
	receipt.Scheduling = input.Scheduling
	receipt.FramePipeline = input.FramePipeline
	receipt.Phases = input.Phases
	receipt.Segments = input.Segments
	receipt.Coverage = input.Coverage
	receipt.Derived = Derive(input.Raw)
	// The CPU section is a projection of the same canonical derived value;
	// it is never calculated by a second ratio formula.
	receipt.CPU.CPUWallRatio = receipt.Derived.CPUWallRatio
	return receipt
}
