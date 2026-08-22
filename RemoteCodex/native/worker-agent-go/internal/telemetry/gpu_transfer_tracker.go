// gpu_transfer_tracker.go — GPU ↔ CPU frame transfer instrumentation.
//
// GPUTransferTracker monitors the lifecycle of decoded frames across the GPU
// and CPU boundaries. The NVDEC decoder outputs frames on the GPU; when those
// frames are consumed by a CPU-side operation (subtitle raster via libass,
// blur filter, watermark composition, final composite), a GPU→CPU download
// transfer is inferred. When CPU-processed frames are fed back to NVENC for
// encoding, a CPU→GPU upload transfer is inferred.
//
// The tracker works by analyzing the detailed phase stream from the C++
// engine. GPU-bound operations (decode, encode via NVENC) and CPU-bound
// operations (subtitle, blur, watermark, composite) are classified by their
// component/action pair. When a CPU operation follows a GPU decode on the
// same frame set, the decoded frame data is assumed to cross the PCIe bus.
//
// Target for CUDA-ideal pipeline:
//
//	frames_downloaded_from_gpu == 0
//	frames_uploaded_to_gpu == 0
//
// Usage:
//
//	tracker := NewGPUTransferTracker()
//	tracker.IngestEnginePhases(engineDetailedPhases)
//	metrics := tracker.Snapshot()
//	rawMetrics.PopulateFromGPUTransfers(metrics)
package telemetry

import (
	"sync"
)

// GPUTransferTracker accumulates frame transfer metrics across the GPU↔CPU
// boundary by analyzing the engine's detailed phase stream. It is thread-safe
// and scoped to a single job.
type GPUTransferTracker struct {
	mu sync.Mutex
	metrics GPUTransferMetrics
}

// NewGPUTransferTracker returns a ready-to-use tracker.
func NewGPUTransferTracker() *GPUTransferTracker {
	return &GPUTransferTracker{}
}

// IngestEnginePhases analyzes a set of engine detailed phases and infers
// GPU↔CPU transfers. This is the primary ingestion path: after the engine run
// completes, the executor feeds all phases here.
//
// Inference logic:
//   - engine.video.decode → GPU-side NVDEC output. FramesOut are on GPU.
//   - engine.video.subtitle* → CPU-side libass. FramesIn are downloaded.
//   - engine.video.blur → CPU-side filter. FramesIn are downloaded.
//   - engine.video.watermark* → CPU-side composition. FramesIn are downloaded.
//   - engine.composite → CPU-side final composite. FramesIn are downloaded.
//   - engine.encode.* → GPU-side NVENC. FramesIn are uploaded.
func (t *GPUTransferTracker) IngestEnginePhases(phases []PhaseIngest) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, phase := range phases {
		op := classifyGPUOperation(phase.Component, phase.Action)
		switch op {
		case gpuOpDecode:
			// NVDEC outputs frames on GPU. The output frames count is tracked;
			// transfers are recorded when those frames are consumed by CPU ops.
			t.metrics.FramesUploadedGPU += phase.FramesOut
		case gpuOpCPUConsume:
			// A CPU-side operation consumed frames that may have been decoded
			// on GPU. The input frames imply a GPU→CPU download.
			if phase.FramesIn > 0 {
				t.metrics.FramesDownloadedGPU += phase.FramesIn
				t.metrics.GPUToCPUMs += phase.DurationMS
				// Estimate bytes: assume NV12 (1.5 bytes/pixel) at 1080p per frame.
				// This is a heuristic; real byte count requires format introspection.
				t.metrics.GPUToCPUBytes += phase.BytesIn
			}
		case gpuOpEncode:
			// NVENC encodes frames on GPU. The input frames imply a CPU→GPU
			// upload (frames were on CPU from subtitle/blur/composite).
			if phase.FramesIn > 0 {
				t.metrics.FramesUploadedGPU += phase.FramesIn
				t.metrics.CPUToGPUMs += phase.DurationMS
				t.metrics.CPUToGPUBytes += phase.BytesIn
			}
		case gpuOpUpload:
			// Explicit upload operation (e.g. hwupload_cuda filter).
			if phase.FramesIn > 0 {
				t.metrics.FramesUploadedGPU += phase.FramesIn
				t.metrics.CPUToGPUMs += phase.DurationMS
				t.metrics.CPUToGPUBytes += phase.BytesIn
			}
		case gpuOpDownload:
			// Explicit download operation (e.g. hwdownload filter).
			if phase.FramesOut > 0 {
				t.metrics.FramesDownloadedGPU += phase.FramesOut
				t.metrics.GPUToCPUMs += phase.DurationMS
				t.metrics.GPUToCPUBytes += phase.BytesOut
			}
		}
	}
}

// IngestTransfer records an explicit GPU↔CPU transfer (e.g. from an ffmpeg
// hwupload/hwdownload filter or an explicit cudaMemcpy call). Frames, bytes,
// and duration are accumulated additively.
func (t *GPUTransferTracker) IngestTransfer(direction TransferDirection, frames, bytes, durationMs int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch direction {
	case TransferGPUToCPU:
		t.metrics.FramesDownloadedGPU += frames
		t.metrics.GPUToCPUBytes += bytes
		t.metrics.GPUToCPUMs += durationMs
	case TransferCPUToGPU:
		t.metrics.FramesUploadedGPU += frames
		t.metrics.CPUToGPUBytes += bytes
		t.metrics.CPUToGPUMs += durationMs
	}
}

// Snapshot returns an independent copy of the accumulated metrics.
func (t *GPUTransferTracker) Snapshot() GPUTransferMetrics {
	if t == nil {
		return GPUTransferMetrics{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.metrics
}

// Merge combines another tracker's metrics into this one. Useful for combining
// CI engine phases with additional explicit ffmpeg transfer phases.
func (t *GPUTransferTracker) Merge(other *GPUTransferTracker) {
	if t == nil || other == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.Add(other.Snapshot())
}

// ── GPU operation classification ─────────────────────────────────────────

type gpuOperation int

const (
	gpuOpNone       gpuOperation = iota
	gpuOpDecode                  // GPU-side: NVDEC
	gpuOpCPUConsume              // CPU-side: subtitle, blur, watermark, composite
	gpuOpEncode                  // GPU-side: NVENC
	gpuOpUpload                  // Explicit GPU upload (hwupload_cuda)
	gpuOpDownload                // Explicit GPU download (hwdownload)
)

// PhaseIngest is a lightweight projection of the engine's detailed phase
// for GPU transfer analysis. Only the fields needed for classification
// are included.
type PhaseIngest struct {
	Component   string
	Action      string
	FramesIn    int64
	FramesOut   int64
	BytesIn     int64
	BytesOut    int64
	DurationMS  int64
}

// TransferDirection names the direction of a GPU↔CPU data movement.
type TransferDirection int

const (
	TransferGPUToCPU TransferDirection = iota
	TransferCPUToGPU
)

// classifyGPUOperation maps a component/action pair to its GPU operation type.
func classifyGPUOperation(component, action string) gpuOperation {
	// Explicit upload/download filters.
	key := component + "." + action
	switch key {
	case "engine.video.hwupload", "engine.gpu.upload", "engine.encode.hwupload":
		return gpuOpUpload
	case "engine.video.hwdownload", "engine.gpu.download", "engine.decode.hwdownload":
		return gpuOpDownload
	}

	// GPU-side decode: NVDEC runs on the GPU.
	if component == "engine.video" && action == "decode" {
		return gpuOpDecode
	}

	// GPU-side encode: NVENC runs on the GPU.
	switch {
	case component == "engine.encode" && (action == "frame_submit" || action == "flush" || action == "setup"):
		return gpuOpEncode
	case component == "engine.video" && action == "encode":
		return gpuOpEncode
	}

	// CPU-side operations that consume decoded frames from GPU:
	// subtitle raster (libass), blur filter, watermark composition, final composite.
	switch {
	case component == "engine.video" && action == "subtitle":
		return gpuOpCPUConsume
	case component == "engine.video" && action == "subtitle_raster":
		return gpuOpCPUConsume
	case component == "engine.video" && action == "subtitle_composite":
		return gpuOpCPUConsume
	case component == "engine.subtitle" && (action == "render" || action == "raster" || action == "composite"):
		return gpuOpCPUConsume
	case component == "engine.video" && action == "blur":
		return gpuOpCPUConsume
	case component == "engine.video" && action == "filter":
		return gpuOpCPUConsume
	case component == "engine.video" && action == "watermark":
		return gpuOpCPUConsume
	case component == "engine.video" && action == "watermark_upload":
		return gpuOpCPUConsume
	case component == "engine.video" && action == "watermark_composite":
		return gpuOpCPUConsume
	case component == "engine.watermark" && (action == "render" || action == "upload" || action == "composite"):
		return gpuOpCPUConsume
	case (component == "engine.video" && action == "composite") || (component == "engine" && action == "composite"):
		return gpuOpCPUConsume
	}

	return gpuOpNone
}

// IdentifyBottleneck returns a human-readable classification of the GPU
// transfer bottleneck. When frames_downloaded_from_gpu == frames_decoded
// and frames_uploaded_to_gpu == frames_encoded, the pipeline is fully
// crossing the PCIe bus — the classic NVDEC→download→CPU→upload→NVENC
// round-trip.
func (t *GPUTransferTracker) IdentifyBottleneck(decoded, encoded int64) string {
	if t == nil {
		return "no_gpu_data"
	}
	m := t.Snapshot()
	switch {
	case m.FramesDownloadedGPU == 0 && m.FramesUploadedGPU == 0:
		return "no_gpu_transfers" // ideal: zero-copy CUDA pipeline or packet-copy
	case m.FramesDownloadedGPU == decoded && m.FramesUploadedGPU == encoded:
		return "full_pcie_roundtrip" // NVDEC→download→CPU→upload→NVENC
	case m.FramesDownloadedGPU > 0 && m.FramesUploadedGPU == 0:
		return "gpu_to_cpu_only" // decode then CPU processing with software encode
	default:
		return "partial_gpu_transfers"
	}
}