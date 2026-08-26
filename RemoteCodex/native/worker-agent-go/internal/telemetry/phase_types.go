package telemetry

import "time"

// ── Phase timing accumulator ───────────────────────────────────────────────

// PhaseTiming holds accumulated timing and data metrics for one phase.
type PhaseTiming struct {
	Duration    time.Duration
	Count       int64
	BytesIn     int64
	BytesOut    int64
	FramesIn    int64
	FramesOut   int64
	CPUMs       float64
	QueueWaitMs float64
	Errors      int64
}

// Add merges src into this timing.
func (t *PhaseTiming) Add(src PhaseTiming) {
	t.Duration += src.Duration
	t.Count += src.Count
	t.BytesIn += src.BytesIn
	t.BytesOut += src.BytesOut
	t.FramesIn += src.FramesIn
	t.FramesOut += src.FramesOut
	t.CPUMs += src.CPUMs
	t.QueueWaitMs += src.QueueWaitMs
	t.Errors += src.Errors
}

// DurationMs returns the duration in milliseconds.
func (t PhaseTiming) DurationMs() int64 {
	return t.Duration.Milliseconds()
}

// ScenePhaseTiming is per-scene timing data.
type ScenePhaseTiming struct {
	SceneID          string
	SourceDurationMs int64
	OutputDurationMs int64
	Phases           map[string]PhaseTiming
	InputBytes       int64
	OutputBytes      int64
	FramesDecoded    int64
	FramesEncoded    int64
	FPS              float64
}

// TotalMs returns the sum of all phase durations for this scene.
func (s ScenePhaseTiming) TotalMs() int64 {
	var total time.Duration
	for _, p := range s.Phases {
		total += p.Duration
	}
	return total.Milliseconds()
}

// RenderSpeed returns the render speed multiplier (media duration / processing time).
func (s ScenePhaseTiming) RenderSpeed() float64 {
	total := s.TotalMs()
	if total <= 0 {
		return 0
	}
	return float64(s.OutputDurationMs) / float64(total)
}

// ── Named containers for ordered output ────────────────────────────────────

// PhaseTimingWithName pairs a phase name with its accumulated timing.
type PhaseTimingWithName struct {
	Name   string
	Timing PhaseTiming
}

// SceneTimingWithName pairs a scene ID with its timing.
type SceneTimingWithName struct {
	SceneID string
	Timing  ScenePhaseTiming
}

// ── GPU transfer metrics ───────────────────────────────────────────────────

// GPUTransferMetrics tracks VRAM ↔ RAM data movement.
type GPUTransferMetrics struct {
	GPUToCPUMs          int64 `json:"gpu_to_cpu_transfer_ms"`
	CPUToGPUMs          int64 `json:"cpu_to_gpu_transfer_ms"`
	GPUToCPUBytes       int64 `json:"gpu_to_cpu_bytes"`
	CPUToGPUBytes       int64 `json:"cpu_to_gpu_bytes"`
	FramesDownloadedGPU int64 `json:"frames_downloaded_from_gpu"`
	FramesUploadedGPU   int64 `json:"frames_uploaded_to_gpu"`
}

// Add merges src into these metrics.
func (g *GPUTransferMetrics) Add(src GPUTransferMetrics) {
	g.GPUToCPUMs += src.GPUToCPUMs
	g.CPUToGPUMs += src.CPUToGPUMs
	g.GPUToCPUBytes += src.GPUToCPUBytes
	g.CPUToGPUBytes += src.CPUToGPUBytes
	g.FramesDownloadedGPU += src.FramesDownloadedGPU
	g.FramesUploadedGPU += src.FramesUploadedGPU
}
