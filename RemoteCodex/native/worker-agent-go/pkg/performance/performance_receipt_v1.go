// Package performance defines PerformanceReceiptV1 — the canonical,
// typed, final performance artifact for a single Attempt.
//
// The receipt is the single object that closes the loop
//
//	measure → correlate → save → compare → block regressions
//
// for the Velox render path. It is deliberately NOT another logger and
// NOT another metrics system: every telemetry source that already exists
// (pipeline.RunMetrics, the C++ engine sidecar phase/segment timing,
// worker process counters) is aggregated into this one typed document by
// the PerformanceReceiptAssembler. Units are explicit per field: wall
// clocks and phase timings are milliseconds, the workload duration is
// microseconds, the media duration is seconds, all byte counts are
// absolute bytes, and ratios are dimensionless floats in [0, +∞).
//
// The zero value is safe: JSON-marshaling it produces the canonical
// shape with all counters at zero, so an assembler that fails early can
// still emit a structurally valid receipt.
package performance

import (
	"encoding/json"
	"errors"

	"velox-shared/telemetry"
)

// PerformanceReceiptVersionV1 is the version stamped on every V1
// receipt. Bump the type (not this constant) when the shape changes
// incompatibly; keep V1 readers able to parse receipts with this exact
// version.
const PerformanceReceiptVersionV1 = 1

// PerformanceReceiptV1 is the canonical typed performance artifact for
// one Attempt. Sections mirror the plan:
//
//	Task → Execution → C++ engine metrics → worker metrics →
//	system metrics → PerformanceReceiptV1
//
// JSON field order is deliberate and stable: identity and workload
// first (what was run), then the measured sections, then the derived
// KPIs — so receipts diff cleanly across runs.
type PerformanceReceiptV1 struct {
	Version int `json:"version"`

	Identity PerformanceIdentity `json:"identity"`
	Workload WorkloadProfile     `json:"workload"`

	Timing     TimingMetrics     `json:"timing"`
	Process    ProcessMetrics    `json:"process"`
	CPU        CPUMetrics        `json:"cpu"`
	IO         IOMetrics         `json:"io"`
	Media      MediaMetrics      `json:"media"`
	Memory     MemoryMetrics     `json:"memory"`
	Scheduling SchedulingMetrics `json:"scheduling"`

	// FramePipeline carries the §25 producer-consumer health metrics of
	// the in-process encode pipeline (Decoder → FramePool → Render
	// Producer → BoundedQueue → Encoder Consumer → Muxer). Only jobs that
	// genuinely encode (zoom/resize/text/compositing) route through it;
	// copy-only and legacy paths leave the section all-zero (the zero
	// value reads as "not measured", never a measured zero).
	FramePipeline FramePipelineMetrics `json:"frame_pipeline,omitempty"`

	// Phases carries the canonical exclusive-measurement phase list.
	// The assembler derives it from the existing sidecar phase timing;
	// legacy sidecars that predate detailed events leave it nil.
	Phases []PhaseTiming `json:"phases,omitempty"`

	// Segments carries the per-timeline-segment C++ sidecar timings,
	// including the started/finished offsets and parallelism fields.
	// The assembler maps them from RenderMetrics.Segments; legacy
	// sidecars leave the slice nil.
	Segments []SegmentTiming `json:"segments,omitempty"`

	Derived DerivedMetrics `json:"derived"`
}

// NewPerformanceReceiptV1 returns a zero-valued V1 receipt with Version
// stamped to PerformanceReceiptVersionV1. The assembler populates the
// sections; callers should never hand-build a receipt by literal.
func NewPerformanceReceiptV1() *PerformanceReceiptV1 {
	return &PerformanceReceiptV1{Version: PerformanceReceiptVersionV1}
}

// ToJSON marshals the receipt as a stable, indented diagnostic document
// (the eventual performance_receipt.json artifact). MarshalIndent is
// intentional: this document is read by humans and diffs across runs.
func (r *PerformanceReceiptV1) ToJSON() ([]byte, error) {
	if r == nil {
		return nil, errors.New("performance: cannot marshal a nil receipt")
	}
	return json.MarshalIndent(r, "", "  ")
}

// PerformanceIdentity identifies which job, attempt, worker, release and
// machine produced the receipt. Without this section two receipts cannot
// be compared as the same benchmark.
type PerformanceIdentity struct {
	JobID     string `json:"job_id,omitempty"`
	AttemptID string `json:"attempt_id,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	WorkerID  string `json:"worker_id,omitempty"`

	Release       string `json:"release,omitempty"`
	GitCommit     string `json:"git_commit,omitempty"`
	EngineVersion string `json:"engine_version,omitempty"`
	EngineSHA256  string `json:"engine_sha256,omitempty"`

	Kernel       string `json:"kernel,omitempty"`
	Architecture string `json:"architecture,omitempty"`
	CPUModel     string `json:"cpu_model,omitempty"`

	// BenchmarkFixtureID links the receipt to the immutable fixture
	// (BenchmarkFixtureRegistry) it was produced from, when any.
	BenchmarkFixtureID string `json:"benchmark_fixture_id,omitempty"`
}

// WorkloadProfile describes what the attempt actually ran. Two receipts
// with different workloads must never be compared as if they were the
// same benchmark (5 min / 10 clips vs 10 min / 80 clips).
//
// The profile is built from the CompiledRenderPlanV2 via
// performance.WorkloadFromCompiledRenderPlan — the render plan is the
// single owner of clip count and expected duration (Fact Owner:
// render_plan). It is NEVER reconstructed from telemetry (no timeline
// items fallback).
type WorkloadProfile struct {
	JobType string `json:"job_type,omitempty"`

	// DurationUS is the intended media duration of the job in
	// microseconds (the unit used by the render plan timelines).
	DurationUS int64 `json:"duration_us,omitempty"`
	ClipCount  int   `json:"clip_count,omitempty"`
	AssetCount int   `json:"asset_count,omitempty"`

	CopyOnly       bool `json:"copy_only,omitempty"`
	FinalAudioCopy bool `json:"final_audio_copy,omitempty"`

	VideoCodec string  `json:"video_codec,omitempty"`
	AudioCodec string  `json:"audio_codec,omitempty"`
	Width      int     `json:"width,omitempty"`
	Height     int     `json:"height,omitempty"`
	FPS        float64 `json:"fps,omitempty"`
}

// TimingMetrics is the wall-clock budget of the attempt. WallMs is the
// master accounting clock; the phase fields explain as much of it as the
// available timers allow.
type TimingMetrics struct {
	WallMs          int64 `json:"wall_ms"`
	ResolveMs       int64 `json:"resolve_ms"`
	ValidateMs      int64 `json:"validate_ms"`
	CompileMs       int64 `json:"compile_ms"`
	RenderMs        int64 `json:"render_ms"`
	PipelineTotalMs int64 `json:"pipeline_total_ms"`
	PlanMarshalMs   int64 `json:"plan_marshal_ms"`
	PlanWriteMs     int64 `json:"plan_write_ms"`
	ProcessStartMs  int64 `json:"process_start_ms"`
	ProcessWaitMs   int64 `json:"process_wait_ms"`
	NativeTotalMs   int64 `json:"native_total_ms"`
	ExecutorTotalMs int64 `json:"executor_total_ms"`
	ArtifactTotalMs int64 `json:"artifact_total_ms"`
}

// ProcessMetrics counts the processes spawned for the attempt.
//
// EngineSpawnCount and ExternalProcessCount are DISJOINT and must never
// be summed: EngineSpawnCount counts the Go worker's own C++ engine
// subprocess, while ExternalProcessCount counts the external tool
// processes (ffmpeg/ffprobe/shell/curl) spawned BY the engine. The
// Phase-1 target for the copy-only path is
//
//	ffmpeg_exec_count = 0, ffprobe_exec_count = 0, shell_exec_count = 0
//
// which makes ProcessMetrics an architectural invariant, not just a
// counter.
//
// EngineSpawnCount is the OBSERVED spawn fact (cmd.Start() success,
// reported by the process runner and mirrored by the canonical
// worker.engine.spawn event) — it is never derived from a timing value
// like ProcessStartMs.
type ProcessMetrics struct {
	EngineSpawnCount int64 `json:"engine_spawn_count"`
	EngineSpawnMs    int64 `json:"engine_spawn_ms"`

	ExternalProcessCount int64 `json:"external_process_count"`

	FfmpegExecCount  int64 `json:"ffmpeg_exec_count"`
	FfprobeExecCount int64 `json:"ffprobe_exec_count"`
	ShellExecCount   int64 `json:"shell_exec_count"`
	CurlExecCount    int64 `json:"curl_exec_count"`

	// Engine-declared spawn ledger (sidecar process_counters block): the
	// external tool processes the C++ engine ITSELF spawned, broken down
	// by kind. These are DISJOINT from ExternalProcessCount / *_exec_count
	// above (the Go-side /proc sampler's view) and must never be summed
	// with them. The Phase-1 copy-only invariant
	// engine_external_spawn_count == 0 is readable straight from this
	// field.
	EngineExternalSpawnCount int64 `json:"engine_external_spawn_count"`
	EngineFfmpegSpawnCount   int64 `json:"engine_ffmpeg_spawn_count"`
	EngineFfprobeSpawnCount  int64 `json:"engine_ffprobe_spawn_count"`
	EngineShellSpawnCount    int64 `json:"engine_shell_spawn_count"`
	EngineCurlSpawnCount     int64 `json:"engine_curl_spawn_count"`

	ChildWaitMs int64 `json:"child_wait_ms"`
}

// CPUMetrics separates the attempt's CPU consumption from its wall
// clock. WallMs mirrors Timing.WallMs (the master accounting clock) so
// the section is self-contained; CPUUserMs/CPUSystemMs are measured
// from the utime/stime clock ticks of every process in the engine tree
// (sampled from /proc while the engine ran, engine included), converted
// to milliseconds. CPUTotalMs = user + system.
//
// CPUWallRatio = cpu_total_ms / wall_ms: >1 means the workload is
// CPU-parallel across cores, <<1 means the attempt is not CPU-bound.
// A zero value means "not derived", never a measured zero.
//
// Provenance note: this section's CPUWallRatio is the /proc sampler's
// view (user+system ticks of the engine tree). Derived.CPUWallRatio is
// the SAME concept but consumes the authoritative cgroup v2 CPU fact
// (attempt_telemetry) when the attempt session collected it — the two
// may differ by sub-percent noise and must not be treated as a bug.
type CPUMetrics struct {
	WallMs       int64   `json:"wall_ms"`
	CPUUserMs    int64   `json:"cpu_user_ms"`
	CPUSystemMs  int64   `json:"cpu_system_ms"`
	CPUTotalMs   int64   `json:"cpu_total_ms"`
	CPUWallRatio float64 `json:"cpu_wall_ratio"`

	// EngineCPUUserMs/EngineCPUSystemMs are the engine process's OWN
	// getrusage facts (sidecar process_counters block) — the
	// engine-declared subset of the tree counters above. Zero on engines
	// that predate the block.
	EngineCPUUserMs   int64 `json:"engine_cpu_user_ms"`
	EngineCPUSystemMs int64 `json:"engine_cpu_system_ms"`
}

// IOMetrics counts real bytes moved by the attempt. TotalBytesRead and
// TotalBytesWritten are measured from /proc/<pid>/io over the whole
// engine process tree (logical rchar/wchar bytes) and are the
// amplification denominators. The per-kind counters come from the
// engine's own accounting (segment source sizes, temp bytes, mux phase
// bytes, final artifact size) or are zero until engine-side
// instrumentation exists — see the assembler for the exact provenance
// of each field.
type IOMetrics struct {
	TotalBytesRead    int64 `json:"total_bytes_read"`
	TotalBytesWritten int64 `json:"total_bytes_written"`

	// AssetBytesRead is the engine-declared size of every asset it bound
	// or staged (sum of segments[].source_bytes); a file-size proxy for
	// bytes touched from assets, not a syscall counter.
	AssetBytesRead int64 `json:"asset_bytes_read"`
	// AssetBytesCopied comes from the engine's io_counters sidecar block
	// (asset materialization copies); 0 on older engines. The Phase-1
	// copy-only target is exactly 0 copies.
	AssetBytesCopied  int64 `json:"asset_bytes_copied"`
	TempBytesWritten  int64 `json:"temp_bytes_written"`
	MuxBytesRead      int64 `json:"mux_bytes_read"`
	MuxBytesWritten   int64 `json:"mux_bytes_written"`
	FinalBytesWritten int64 `json:"final_bytes_written"`

	// FileCopyCount/FileCopyBytes come from the engine's io_counters
	// sidecar block; 0 on older engines. The Phase-1 copy-only target is
	// 0 copies.
	FileCopyCount int64 `json:"file_copy_count"`
	FileCopyBytes int64 `json:"file_copy_bytes"`

	// InputOpenCount/InputReopenCount come from the engine's io_counters
	// sidecar block; 0 on older engines. Reopens are re-opens of a path
	// already opened by the same render (e.g. the copy-only muxer's
	// stream-discovery open followed by the readPackets reopen).
	InputOpenCount   int64 `json:"input_open_count"`
	InputReopenCount int64 `json:"input_reopen_count"`
}

// MediaMetrics mirrors the C++ engine sidecar counters already mapped by
// the worker (frames, encode passes, temp bytes, concat mode, …).
//
// MediaMetrics.TempBytes and IOMetrics.TempBytesWritten both project the
// engine's sidecar temp_bytes accounting: Media keeps the sidecar
// mirror, IO exposes the same value as the projection consumed by the
// amplification KPIs.
type MediaMetrics struct {
	Frames           int64   `json:"frames"`
	FramesDecoded    int64   `json:"frames_decoded"`
	FramesComposited int64   `json:"frames_composited"`
	EncodePasses     int64   `json:"encode_passes"`
	TempBytes        int64   `json:"temp_bytes"`
	OutputBytes      int64   `json:"output_bytes"`
	DurationSec      float64 `json:"duration_sec"`
	ConcatMode       string  `json:"concat_mode,omitempty"`
	FPS              float64 `json:"fps"`
	SpeedX           float64 `json:"speed_x"`
	Bitrate          float64 `json:"bitrate"`
	DupFrames        int64   `json:"dup_frames"`
	DropFrames       int64   `json:"drop_frames"`
}

// MemoryMetrics summarizes the engine process tree's RSS, sampled from
// /proc/<pid>/stat while the engine ran (engine + descendants).
// PeakRSSBytes is the high-water mark of the tree's resident set;
// CurrentRSSBytes is the tree RSS at the last sample before the monitor
// stopped (the engine may have exited by then, so it can sit below the
// peak). Zero when no native render occurred.
type MemoryMetrics struct {
	PeakRSSBytes    int64 `json:"peak_rss_bytes"`
	CurrentRSSBytes int64 `json:"current_rss_bytes"`
}

// OffCPUReason describes WHY the attempt was off CPU when a deep
// profile (DETAILED/KERNEL mode) is available; the standard receipt
// leaves OffCPUMs zero and reason empty.
type OffCPUReason string

// Canonical OffCPUReason values.
const (
	OffCPUReasonProcessWait OffCPUReason = "process_wait"
	OffCPUReasonDiskIO      OffCPUReason = "disk_io"
	OffCPUReasonNetworkIO   OffCPUReason = "network_io"
	OffCPUReasonQueueWait   OffCPUReason = "queue_wait"
	OffCPUReasonLockWait    OffCPUReason = "lock_wait"
	OffCPUReasonSleep       OffCPUReason = "sleep"
	OffCPUReasonUnknown     OffCPUReason = "unknown"
)

// SchedulingMetrics separates off-CPU time from futex contention. The
// intent is to never treat "futex = contention" automatically: waiting
// for the media engine is process_wait, not lock contention.
type SchedulingMetrics struct {
	QueueWaitMS  int64        `json:"queue_wait_ms"`
	OffCPUMs     int64        `json:"off_cpu_ms"`
	OffCPUReason OffCPUReason `json:"off_cpu_reason,omitempty"`

	// Engine-declared scheduling/VM facts (sidecar process_counters
	// block): the engine process's own voluntary/involuntary context
	// switches and minor/major page faults via getrusage. Zero on engines
	// that predate the block or when no native render occurred.
	VoluntaryContextSwitches   int64 `json:"voluntary_context_switches"`
	InvoluntaryContextSwitches int64 `json:"involuntary_context_switches"`
	MinorPageFaults            int64 `json:"minor_page_faults"`
	MajorPageFaults            int64 `json:"major_page_faults"`
}

// FramePipelineMetrics (§25) reports how the producer-consumer encode
// pipeline spent its time when the job went through the in-process
// --render-frames path:
//
//	Decoder → FramePool → Render Producer → BoundedQueue → Encoder
//	Consumer → Muxer
//
// ProducerBusyMS is the render producer actively scaling frames;
// ProducerWaitMS is the producer blocked on a hand-off queue (waiting for
// the decoder on an empty render queue, or for the encoder queue to drain
// — backpressure). ConsumerBusyMS is the encoder consumer sending frames
// and writing packets; ConsumerWaitMS is the consumer blocked on an EMPTY
// encoder queue (encoder starvation). QueueDepthAvg is the time-weighted
// average depth of the encoder hand-off queue, QueueDepthMax its
// high-water mark (<= pool capacity by construction), QueueEmptyMS the
// consumer's empty-queue wait and QueueFullMS the producer's full-queue
// wait. The ratios are dimensionless floats in [0, 1] derived by the
// engine:
//
//	producer_stall_ratio     = producer_wait / (busy + wait)
//	encoder_starvation_ratio = consumer_wait / (busy + wait)
//	backpressure_ratio       = queue_full / (producer busy + wait)
//
// A zero value means the stage never ran / was not measured, never a
// measured zero.
type FramePipelineMetrics struct {
	ProducerBusyMS         int64   `json:"producer_busy_ms"`
	ProducerWaitMS         int64   `json:"producer_wait_ms"`
	ConsumerBusyMS         int64   `json:"consumer_busy_ms"`
	ConsumerWaitMS         int64   `json:"consumer_wait_ms"`
	QueueDepthAvg          int64   `json:"queue_depth_avg"`
	QueueDepthMax          int64   `json:"queue_depth_max"`
	QueueEmptyMS           int64   `json:"queue_empty_ms"`
	QueueFullMS            int64   `json:"queue_full_ms"`
	ProducerStallRatio     float64 `json:"producer_stall_ratio"`
	EncoderStarvationRatio float64 `json:"encoder_starvation_ratio"`
	BackpressureRatio      float64 `json:"backpressure_ratio"`
}

// PhaseTiming is one timed phase observation of the attempt. The assembler
// fills it from the sidecar phase timing; Derive sums ONLY the rows
// stamped TimingExclusive into accounted_ratio (catalog
// accounted_ratio_rule: never span parents or span children, whose
// parallel instances would double-count against the wall clock).
//
// TimingMode is the canonical catalog timing role of the phase event,
// stamped by the assembler from the shared catalog (event lookup, with a
// phase-taxonomy fallback for attempt-scoped events that predate a
// catalog key). Rows the catalog cannot classify carry an empty mode and
// are quarantined from accounted_ratio — an unclassified phase is never
// treated as exclusive.
type PhaseTiming struct {
	Name        string `json:"name"`
	DurationMS  int64  `json:"duration_ms"`
	CPUMs       int64  `json:"cpu_ms,omitempty"`
	QueueWaitMS int64  `json:"queue_wait_ms,omitempty"`
	BytesIn     int64  `json:"bytes_in,omitempty"`
	BytesOut    int64  `json:"bytes_out,omitempty"`
	FramesIn    int64  `json:"frames_in,omitempty"`
	FramesOut   int64  `json:"frames_out,omitempty"`

	// TimingMode is the canonical catalog timing role (exclusive |
	// span_parent | span_child); omitted when the event cannot be
	// classified by the shared catalog. ONLY TimingExclusive rows feed
	// accounted_ratio.
	TimingMode telemetry.TimingMode `json:"timing_mode,omitempty"`
}

// SegmentTiming mirrors one row of the C++ sidecar segments[] array. It
// is the per-segment counterpart of the exclusive PhaseTiming list:
// phase rows explain the wall clock, segment rows explain what each
// timeline segment did (and how parallel it ran).
type SegmentTiming struct {
	SegmentIndex     int     `json:"segment_index"`
	SceneID          string  `json:"scene_id,omitempty"`
	SourceType       string  `json:"source_type,omitempty"`
	DurationMS       float64 `json:"duration_ms"`
	AssetDownloadMS  float64 `json:"asset_download_ms,omitempty"`
	FfmpegEncodeMS   float64 `json:"ffmpeg_encode_ms,omitempty"`
	SourceBytes      int64   `json:"source_bytes,omitempty"`
	OutputBytes      int64   `json:"output_bytes,omitempty"`
	FramesEncoded    int64   `json:"frames_encoded,omitempty"`
	FramesDecoded    int64   `json:"frames_decoded,omitempty"`
	FramesComposited int64   `json:"frames_composited,omitempty"`
	FfmpegSpeedX     float64 `json:"ffmpeg_speed_x,omitempty"`
	Codec            string  `json:"codec,omitempty"`
	Status           string  `json:"status,omitempty"`
	ErrorCode        string  `json:"error_code,omitempty"`

	// Parallelism telemetry (migration 098).
	StartedOffsetMS  float64 `json:"started_offset_ms,omitempty"`
	FinishedOffsetMS float64 `json:"finished_offset_ms,omitempty"`
	WorkerSlot       int     `json:"worker_slot,omitempty"`
	CPUThreads       int     `json:"cpu_threads,omitempty"`
	ParallelGroup    string  `json:"parallel_group,omitempty"`
}

// DerivedMetrics carries the computed KPIs that make the receipt a
// benchmark instead of a log dump. Every field is computed by the SINGLE
// DerivedMetricsCalculator — Derive(RawMetrics) in derive.go — from raw
// observed facts; nobody hand-assembles this section and no other package
// recomputes these ratios (RAW and DERIVED are strictly separated). A zero
// value means "not derived / fact not measured", never a measured zero.
//
// All ratios are safe against missing denominators: a zero denominator
// yields 0, never +Inf or NaN.
type DerivedMetrics struct {
	// UnaccountedMS is wall_ms minus the sum of the exclusive measured
	// phases (catalog accounted_ratio_rule: only timing_mode=exclusive
	// durations are summed, never span children). Target:
	// accounted_ratio > 0.95 before hunting for single-digit-second wins.
	UnaccountedMS  int64   `json:"unaccounted_ms"`
	AccountedRatio float64 `json:"accounted_ratio"`

	// ReadAmplification / WriteAmplification = total bytes read/written
	// (process-tree /proc/<pid>/io) divided by the final output bytes.
	// copy-only Phase 1 targets write_amplification close to 1.x.
	ReadAmplification  float64 `json:"read_amplification"`
	WriteAmplification float64 `json:"write_amplification"`

	// ProcessesPerClip = external process count (process_runner) / clip
	// count (render_plan). Phase 1 target for copy-only: 0.
	ProcessesPerClip float64 `json:"processes_per_clip"`

	// UsefulWorkRatio = useful pipeline time / wall clock time. It is a
	// directional observation, not a mathematically exact split; the
	// useful-work raw input is caller-known (AssemblyContext).
	UsefulWorkRatio float64 `json:"useful_work_ratio"`

	// CPUWallRatio = cpu_total_ms / wall_ms. >1 means the workload is
	// CPU-parallel across cores; <<1 means the attempt is not CPU-bound.
	// cpu_total_ms is the raw attempt-telemetry CPU fact — never inferred
	// from a wall clock.
	CPUWallRatio float64 `json:"cpu_wall_ratio"`
}
