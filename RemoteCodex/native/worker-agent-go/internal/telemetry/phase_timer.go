// phase_timer.go — fine-grained job-level phase timer.
//
// This file declares the complete fine-grained phase vocabulary for per-job
// timing decomposition. Unlike the 12-phase canonical model (canonical_phases.go),
// these phases provide sub-phase resolution (e.g. video.decode → video.subtitle,
// video.blur, video.composite, video.encode) so operators can pinpoint exactly
// where wall time is spent.
//
// JobPhaseTimer is thread-safe: multiple goroutines may call Begin/End
// concurrently, making it suitable for executors that parallelize work.
// All durations are accumulated with monotonic clock precision.
//
// Phase hierarchy (top-level → sub-phase):
//
//	queue_wait
//	job_setup
//	asset.resolve
//	asset.download
//	asset.verify
//	asset.materialize
//	audio.prepare
//	audio.timeline_build
//	render_plan_build
//	video.decode
//	    video.subtitle (libass)
//	        video.subtitle_raster
//	        video.subtitle_composite
//	    video.watermark
//	        video.watermark_upload
//	        video.watermark_composite
//	    video.blur
//	    video.filter
//	    video.composite
//	video.encode
//	video.concat
//	audio.mux
//	output_finalize
//	artifact.hash (sha256)
//	artifact.probe (ffprobe)
//	artifact.verify
//	artifact.write
//	drive.upload
//	drive.verify
package telemetry

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── Fine-grained phase name constants ──────────────────────────────────────
//
// These are the stable labels for the fine-grained timing decomposition.
// They extend (not replace) the 12 canonical phases in canonical_phases.go.
// Naming convention: "domain.action" with underscores for sub-domains.

const (
	// ── Pre-render phases ─────────────────────────────────────────────
	PhaseQueueWait       = "queue_wait"
	PhaseJobSetup        = "job_setup"
	PhaseAssetResolve    = "asset.resolve"
	PhaseAssetDownload   = "asset.download"
	PhaseAssetVerify     = "asset.verify"
	PhaseAssetMaterialize = "asset.materialize"

	// ── Audio phases ──────────────────────────────────────────────────
	PhaseAudioPrepare       = "audio.prepare"
	PhaseAudioTimelineBuild = "audio.timeline_build"

	// ── Render planning ───────────────────────────────────────────────
	PhaseRenderPlanBuild = "render_plan_build"

	// ── Video pipeline: decode ────────────────────────────────────────
	PhaseVideoDecode = "video.decode"

	// ── Video pipeline: subtitle (libass) ─────────────────────────────
	PhaseVideoSubtitle          = "video.subtitle"
	PhaseVideoSubtitleRaster    = "video.subtitle_raster"
	PhaseVideoSubtitleComposite = "video.subtitle_composite"

	// ── Video pipeline: watermark ─────────────────────────────────────
	PhaseVideoWatermark          = "video.watermark"
	PhaseVideoWatermarkUpload    = "video.watermark_upload"
	PhaseVideoWatermarkComposite = "video.watermark_composite"

	// ── Video pipeline: effects ───────────────────────────────────────
	PhaseVideoBlur   = "video.blur"
	PhaseVideoFilter = "video.filter"

	// ── Video pipeline: composite ─────────────────────────────────────
	PhaseVideoComposite = "video.composite"

	// ── Video pipeline: encode ────────────────────────────────────────
	PhaseVideoEncode = "video.encode"

	// ── Video pipeline: concat ────────────────────────────────────────
	PhaseVideoConcat = "video.concat"

	// ── Audio mux ─────────────────────────────────────────────────────
	PhaseAudioMux = "audio.mux"

	// ── Output & verification ─────────────────────────────────────────
	PhaseOutputFinalize = "output_finalize"
	PhaseArtifactHash   = "artifact.hash"
	PhaseArtifactProbe  = "artifact.probe"
	PhaseArtifactVerify = "artifact.verify"
	PhaseArtifactWrite  = "artifact.write"

	// ── Drive / upload ────────────────────────────────────────────────
	PhaseDriveUpload = "drive.upload"
	PhaseDriveVerify = "drive.verify"

	// ── Download / cache timing sub-phases ────────────────────────────
	PhaseDriveDownload     = "asset.download_drive"
	PhaseBlobstoreDownload = "asset.download_blobstore"
	PhaseLocalCacheRead    = "asset.cache_read"
	PhaseAssetDownloadWait = "asset.download_wait"

	// ── Disk I/O sub-phases ───────────────────────────────────────────
	PhaseOutputWrite = "output.write"
	PhaseTempWrite   = "temp.write"
	PhaseFinalRead   = "final.read"
)

// FineGrainedPhaseOrder defines the stable, deterministic iteration order for
// all fine-grained phases. Callers rely on this for consistent report output.
var FineGrainedPhaseOrder = []string{
	PhaseQueueWait,
	PhaseJobSetup,
	PhaseAssetResolve,
	PhaseAssetDownload,
	PhaseAssetVerify,
	PhaseAssetMaterialize,
	PhaseAudioPrepare,
	PhaseAudioTimelineBuild,
	PhaseRenderPlanBuild,
	PhaseVideoDecode,
	PhaseVideoSubtitle,
	PhaseVideoSubtitleRaster,
	PhaseVideoSubtitleComposite,
	PhaseVideoWatermark,
	PhaseVideoWatermarkUpload,
	PhaseVideoWatermarkComposite,
	PhaseVideoBlur,
	PhaseVideoFilter,
	PhaseVideoComposite,
	PhaseVideoEncode,
	PhaseVideoConcat,
	PhaseAudioMux,
	PhaseOutputFinalize,
	PhaseArtifactHash,
	PhaseArtifactProbe,
	PhaseArtifactVerify,
	PhaseArtifactWrite,
	PhaseDriveUpload,
	PhaseDriveVerify,
	PhaseDriveDownload,
	PhaseBlobstoreDownload,
	PhaseLocalCacheRead,
	PhaseAssetDownloadWait,
	PhaseOutputWrite,
	PhaseTempWrite,
	PhaseFinalRead,
}

// PhaseDisplayNames maps phase constants to human-readable labels for reports.
var PhaseDisplayNames = map[string]string{
	PhaseQueueWait:              "Queue wait",
	PhaseJobSetup:               "Job setup",
	PhaseAssetResolve:           "Asset resolve",
	PhaseAssetDownload:          "Asset download",
	PhaseAssetVerify:            "Asset verify",
	PhaseAssetMaterialize:       "Asset materialize",
	PhaseAudioPrepare:           "Audio prepare",
	PhaseAudioTimelineBuild:     "Audio timeline build",
	PhaseRenderPlanBuild:        "Plan build",
	PhaseVideoDecode:            "Video decode",
	PhaseVideoSubtitle:          "Subtitle (libass)",
	PhaseVideoSubtitleRaster:    "  Subtitle raster",
	PhaseVideoSubtitleComposite: "  Subtitle composite",
	PhaseVideoWatermark:         "Watermark",
	PhaseVideoWatermarkUpload:   "  Watermark upload",
	PhaseVideoWatermarkComposite: "  Watermark composite",
	PhaseVideoBlur:              "Blur",
	PhaseVideoFilter:            "Filter",
	PhaseVideoComposite:         "Composite",
	PhaseVideoEncode:            "Video encode",
	PhaseVideoConcat:            "Concat",
	PhaseAudioMux:               "Audio mux",
	PhaseOutputFinalize:         "Output finalize",
	PhaseArtifactHash:           "SHA256",
	PhaseArtifactProbe:          "ffprobe",
	PhaseArtifactVerify:         "Artifact verify",
	PhaseArtifactWrite:          "Artifact write",
	PhaseDriveUpload:            "Drive upload",
	PhaseDriveVerify:            "Drive verify",
	PhaseDriveDownload:          "  Drive download",
	PhaseBlobstoreDownload:      "  Blobstore download",
	PhaseLocalCacheRead:         "  Local cache read",
	PhaseAssetDownloadWait:      "  Download wait",
	PhaseOutputWrite:            "Output write",
	PhaseTempWrite:              "Temp write",
	PhaseFinalRead:              "Final read",
}

// fineGrainedSet enables O(1) phase name validation.
var fineGrainedSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(FineGrainedPhaseOrder))
	for _, p := range FineGrainedPhaseOrder {
		m[p] = struct{}{}
	}
	return m
}()

// IsFineGrainedPhase reports whether name is a registered fine-grained phase.
func IsFineGrainedPhase(name string) bool {
	_, ok := fineGrainedSet[name]
	return ok
}

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
	SceneID             string
	SourceDurationMs    int64
	OutputDurationMs    int64
	Phases              map[string]PhaseTiming
	InputBytes          int64
	OutputBytes         int64
	FramesDecoded       int64
	FramesEncoded       int64
	FPS                 float64
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

// ── JobPhaseTimer ──────────────────────────────────────────────────────────

// JobPhaseTimer is a thread-safe, per-job phase accumulator. It records wall
// clock durations, data volumes, frame counts, and CPU time for each
// fine-grained phase. Multiple goroutines may call Begin/End concurrently.
//
// A zero-value JobPhaseTimer is NOT usable; always create via NewJobPhaseTimer.
type JobPhaseTimer struct {
	mu          sync.Mutex
	startedAt   time.Time
	phases      map[string]*PhaseTiming
	scenes      map[string]*ScenePhaseTiming
	activeSpans map[string]time.Time // phase → monotonic start (keyed by span ID for nesting)

	// Cache byte tracking (separate from phase durations to avoid
	// conflating cumulative bytes with wall-clock timing).
	cacheMut      sync.Mutex
	cacheHitBytes int64
	cacheMissBytes int64
}

// NewJobPhaseTimer returns a ready-to-use timer with the default wall clock.
func NewJobPhaseTimer() *JobPhaseTimer {
	return &JobPhaseTimer{
		startedAt:   time.Now(),
		phases:      make(map[string]*PhaseTiming, len(FineGrainedPhaseOrder)),
		scenes:      make(map[string]*ScenePhaseTiming),
		activeSpans: make(map[string]time.Time),
	}
}

// NewJobPhaseTimerWithClock allows injecting a fixed clock for tests.
func NewJobPhaseTimerWithClock(clock func() time.Time) *JobPhaseTimer {
	t := NewJobPhaseTimer()
	t.startedAt = clock()
	return t
}

// Begin starts timing a fine-grained phase. It returns a unique span key that
// must be passed to End. Unknown phases are silently ignored (noop span).
// Begin is thread-safe.
func (t *JobPhaseTimer) Begin(phase string) string {
	if t == nil || !IsFineGrainedPhase(phase) {
		return ""
	}
	spanID := fmt.Sprintf("%s.%d", phase, time.Now().UnixNano())
	t.mu.Lock()
	t.activeSpans[spanID] = time.Now()
	t.mu.Unlock()
	return spanID
}

// BeginScene starts a phase within a specific scene. The sceneID is used to
// attribute timing to the per-scene breakdown.
func (t *JobPhaseTimer) BeginScene(sceneID, phase string) string {
	if t == nil || !IsFineGrainedPhase(phase) {
		return ""
	}
	t.mu.Lock()
	if _, exists := t.scenes[sceneID]; !exists {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	t.mu.Unlock()
	spanID := fmt.Sprintf("%s.%s.%d", sceneID, phase, time.Now().UnixNano())
	t.mu.Lock()
	t.activeSpans[spanID] = time.Now()
	t.mu.Unlock()
	return spanID
}

// End completes a span started by Begin (or BeginScene) and accumulates the
// duration. The spanID is the value returned by Begin. It is safe to call End
// multiple times with the same spanID (subsequent calls are no-ops).
//
// End accumulates the duration into the phase identified by the span.
func (t *JobPhaseTimer) End(spanID string) {
	if t == nil || spanID == "" {
		return
	}
	t.mu.Lock()
	start, ok := t.activeSpans[spanID]
	if !ok {
		t.mu.Unlock()
		return
	}
	delete(t.activeSpans, spanID)
	t.mu.Unlock()

	duration := time.Since(start)

	// Parse spanID to determine phase and optional scene.
	// Format: "phase.nano" or "sceneID.phase.nano"
	parts := strings.SplitN(spanID, ".", 3)
	var sceneID, phase string
	if len(parts) == 3 {
		sceneID = parts[0]
		phase = parts[1]
	} else if len(parts) == 2 {
		phase = parts[0]
	} else {
		return
	}
	if !IsFineGrainedPhase(phase) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Accumulate in global phases
	if t.phases[phase] == nil {
		t.phases[phase] = &PhaseTiming{}
	}
	t.phases[phase].Duration += duration
	t.phases[phase].Count++

	// Accumulate in per-scene if applicable
	if sceneID != "" {
		if t.scenes[sceneID] == nil {
			t.scenes[sceneID] = &ScenePhaseTiming{
				SceneID: sceneID,
				Phases:  make(map[string]PhaseTiming),
			}
		}
		s := t.scenes[sceneID]
		pt := s.Phases[phase]
		pt.Duration += duration
		pt.Count++
		s.Phases[phase] = pt
	}
}

// AddPhaseData records data volumes, frames, and CPU time for a phase.
// Can be called independently of Begin/End or combined via DataSpan.
func (t *JobPhaseTimer) AddPhaseData(phase string, bytesIn, bytesOut, framesIn, framesOut int64, cpuMs, queueWaitMs float64) {
	if t == nil || !IsFineGrainedPhase(phase) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phases[phase] == nil {
		t.phases[phase] = &PhaseTiming{}
	}
	p := t.phases[phase]
	p.BytesIn += bytesIn
	p.BytesOut += bytesOut
	p.FramesIn += framesIn
	p.FramesOut += framesOut
	p.CPUMs += cpuMs
	p.QueueWaitMs += queueWaitMs
}

// AddSceneData records per-scene frame and byte metrics.
func (t *JobPhaseTimer) AddSceneData(sceneID string, sourceDurationMs, outputDurationMs int64, inputBytes, outputBytes int64, framesDecoded, framesEncoded int64, fps float64) {
	if t == nil || sceneID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.scenes[sceneID] == nil {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	s := t.scenes[sceneID]
	s.SourceDurationMs = sourceDurationMs
	s.OutputDurationMs = outputDurationMs
	s.InputBytes = inputBytes
	s.OutputBytes = outputBytes
	s.FramesDecoded = framesDecoded
	s.FramesEncoded = framesEncoded
	s.FPS = fps
}

// AddScenePhaseData records per-scene phase data.
func (t *JobPhaseTimer) AddScenePhaseData(sceneID, phase string, bytesIn, bytesOut, framesIn, framesOut int64, cpuMs float64) {
	if t == nil || sceneID == "" || !IsFineGrainedPhase(phase) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.scenes[sceneID] == nil {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	s := t.scenes[sceneID]
	pt := s.Phases[phase]
	pt.BytesIn += bytesIn
	pt.BytesOut += bytesOut
	pt.FramesIn += framesIn
	pt.FramesOut += framesOut
	pt.CPUMs += cpuMs
	s.Phases[phase] = pt
}

// AddCacheHitBytes records bytes served from the local cache (cache hit).
// Thread-safe; may be called from download workers.
func (t *JobPhaseTimer) AddCacheHitBytes(n int64) {
	if t == nil {
		return
	}
	t.cacheMut.Lock()
	t.cacheHitBytes += n
	t.cacheMut.Unlock()
}

// AddCacheMissBytes records bytes downloaded from remote (cache miss).
// Thread-safe; may be called from download workers.
func (t *JobPhaseTimer) AddCacheMissBytes(n int64) {
	if t == nil {
		return
	}
	t.cacheMut.Lock()
	t.cacheMissBytes += n
	t.cacheMut.Unlock()
}

// CacheBytes returns the accumulated cache hit and miss bytes.
func (t *JobPhaseTimer) CacheBytes() (hitBytes, missBytes int64) {
	if t == nil {
		return 0, 0
	}
	t.cacheMut.Lock()
	hitBytes = t.cacheHitBytes
	missBytes = t.cacheMissBytes
	t.cacheMut.Unlock()
	return
}

// PhaseTimings returns a defensive copy of all phase timings, ordered by
// FineGrainedPhaseOrder. Phases with no data are included with zero values.
func (t *JobPhaseTimer) PhaseTimings() []PhaseTimingWithName {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.phaseTimingsLocked()
}

func (t *JobPhaseTimer) phaseTimingsLocked() []PhaseTimingWithName {
	out := make([]PhaseTimingWithName, 0, len(FineGrainedPhaseOrder))
	for _, name := range FineGrainedPhaseOrder {
		pt := PhaseTiming{}
		if t != nil && t.phases[name] != nil {
			pt = *t.phases[name]
		}
		out = append(out, PhaseTimingWithName{Name: name, Timing: pt})
	}
	return out
}

// SceneTimings returns scene timings sorted by descending total duration
// (slowest first). Useful for TOP SLOWEST SCENES reporting.
func (t *JobPhaseTimer) SceneTimings() []SceneTimingWithName {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]SceneTimingWithName, 0, len(t.scenes))
	for id, s := range t.scenes {
		out = append(out, SceneTimingWithName{SceneID: id, Timing: *s})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Timing.TotalMs() > out[j].Timing.TotalMs()
	})
	return out
}

// TotalDuration returns the sum of all phase durations. Note that this may
// exceed wall clock when phases overlap (e.g. parallel execution).
func (t *JobPhaseTimer) TotalDuration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total time.Duration
	for _, p := range t.phases {
		total += p.Duration
	}
	return total
}

// StartedAt returns when the timer was created (approximate job start).
func (t *JobPhaseTimer) StartedAt() time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.startedAt
}

// DataSpan provides a context-like interface for recording data alongside
// timing. Call Begin to start, then Add* methods accumulate counters, and
// Complete records the end time and flushes all data.
type DataSpan struct {
	timer   *JobPhaseTimer
	spanID  string
	phase   string
	sceneID string

	mu        sync.Mutex
	bytesIn   int64
	bytesOut  int64
	framesIn  int64
	framesOut int64
	cpuMs     float64
	queueMs   float64
}

// BeginDataSpan starts a phase span with data tracking. Use the returned
// DataSpan to add counters before calling Complete().
func (t *JobPhaseTimer) BeginDataSpan(phase string) *DataSpan {
	if t == nil || !IsFineGrainedPhase(phase) {
		return nil
	}
	spanID := fmt.Sprintf("%s.%d", phase, time.Now().UnixNano())
	t.mu.Lock()
	t.activeSpans[spanID] = time.Now()
	t.mu.Unlock()
	return &DataSpan{timer: t, spanID: spanID, phase: phase}
}

// BeginSceneDataSpan starts a data span within a scene.
func (t *JobPhaseTimer) BeginSceneDataSpan(sceneID, phase string) *DataSpan {
	if t == nil || !IsFineGrainedPhase(phase) {
		return nil
	}
	t.mu.Lock()
	if _, exists := t.scenes[sceneID]; !exists {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	t.mu.Unlock()
	spanID := fmt.Sprintf("%s.%s.%d", sceneID, phase, time.Now().UnixNano())
	t.mu.Lock()
	t.activeSpans[spanID] = time.Now()
	t.mu.Unlock()
	return &DataSpan{timer: t, spanID: spanID, phase: phase, sceneID: sceneID}
}

// AddBytesIn adds input bytes to the span.
func (d *DataSpan) AddBytesIn(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.bytesIn += n
	d.mu.Unlock()
}

// AddBytesOut adds output bytes to the span.
func (d *DataSpan) AddBytesOut(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.bytesOut += n
	d.mu.Unlock()
}

// AddFramesIn adds input frames to the span.
func (d *DataSpan) AddFramesIn(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.framesIn += n
	d.mu.Unlock()
}

// AddFramesOut adds output frames to the span.
func (d *DataSpan) AddFramesOut(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.framesOut += n
	d.mu.Unlock()
}

// AddCPUMs adds CPU milliseconds to the span.
func (d *DataSpan) AddCPUMs(ms float64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.cpuMs += ms
	d.mu.Unlock()
}

// AddQueueWaitMs adds queue wait milliseconds to the span.
func (d *DataSpan) AddQueueWaitMs(ms float64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.queueMs += ms
	d.mu.Unlock()
}

// Complete ends the span and records all accumulated data.
func (d *DataSpan) Complete() {
	if d == nil || d.timer == nil || d.spanID == "" {
		return
	}
	d.timer.End(d.spanID)
	d.mu.Lock()
	bytesIn := d.bytesIn
	bytesOut := d.bytesOut
	framesIn := d.framesIn
	framesOut := d.framesOut
	cpuMs := d.cpuMs
	queueMs := d.queueMs
	d.mu.Unlock()

	if d.sceneID != "" {
		d.timer.AddScenePhaseData(d.sceneID, d.phase, bytesIn, bytesOut, framesIn, framesOut, cpuMs)
	} else {
		d.timer.AddPhaseData(d.phase, bytesIn, bytesOut, framesIn, framesOut, cpuMs, queueMs)
	}
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