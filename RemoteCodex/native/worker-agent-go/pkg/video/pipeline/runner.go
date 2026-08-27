package pipeline

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/plan"
)

// ProgressSnapshot is the canonical operator-visible progress emitted by the
// native renderer. It is carried on the execution context so a shared Runner
// remains safe when multiple tasks render concurrently.
type ArtifactWriteProgress struct {
	Artifact           string
	Path               string
	HighWatermarkBytes int64
	SafeOffsetBytes    int64
	Finalized          bool
}

type ProgressSnapshot struct {
	Percent           int32
	Scene             int32
	TotalScenes       int32
	Segment           int32
	TotalSegments     int32
	SegmentCompleted  bool
	Phase             string
	FramesEncoded     int64
	FramesDecoded     int64
	FramesComposited  int64
	FfmpegSpeedX      float64
	ElapsedMS         int64
	CumulativeMetrics map[string]float64
}

// ProgressFunc is the legacy operator-visible callback contract. Keep this
// signature source-compatible for existing clients; detailed progress uses
// DetailedProgressFunc below.
type ProgressFunc func(percent, scene, total int, stage string)

// DetailedProgressFunc receives the canonical incremental Attempt snapshot.
// The worker stores it in ActiveTaskExecution.Progress; it is not a second
// progress tracker.
type DetailedProgressFunc func(ProgressSnapshot)
type ArtifactWriteProgressFunc func(ArtifactWriteProgress)

type progressContextKey struct{}
type detailedProgressContextKey struct{}
type artifactWriteProgressContextKey struct{}

// WithProgressCallback associates a legacy task-local progress sink with ctx.
func WithProgressCallback(ctx context.Context, fn ProgressFunc) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, progressContextKey{}, fn)
}

// WithDetailedProgressCallback associates the canonical detailed progress
// sink with ctx without changing the legacy callback API.
func WithDetailedProgressCallback(ctx context.Context, fn DetailedProgressFunc) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, detailedProgressContextKey{}, fn)
}

// ProgressCallback returns the legacy task-local progress sink, if any.
func ProgressCallback(ctx context.Context) ProgressFunc {
	if ctx == nil {
		return nil
	}
	fn, _ := ctx.Value(progressContextKey{}).(ProgressFunc)
	return fn
}

// DetailedProgressCallback returns the canonical detailed progress sink.
func WithArtifactWriteCallback(ctx context.Context, fn ArtifactWriteProgressFunc) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, artifactWriteProgressContextKey{}, fn)
}

func ArtifactWriteCallback(ctx context.Context) ArtifactWriteProgressFunc {
	if ctx == nil {
		return nil
	}
	fn, _ := ctx.Value(artifactWriteProgressContextKey{}).(ArtifactWriteProgressFunc)
	return fn
}

func DetailedProgressCallback(ctx context.Context) DetailedProgressFunc {
	if ctx == nil {
		return nil
	}
	fn, _ := ctx.Value(detailedProgressContextKey{}).(DetailedProgressFunc)
	return fn
}

// SegmentTiming mirrors the C++ SegmentTiming struct emitted inside the
// sidecar segments[] array. One row per timeline segment.
type SegmentTiming struct {
	SegmentIndex     int
	SceneWorkerIndex int
	SceneID          string
	SourceType       string
	DurationMS       float64
	AssetDownloadMS  float64
	FfmpegEncodeMS   float64
	SourceBytes      int64
	OutputBytes      int64
	FramesEncoded    int64
	FramesDecoded    int64
	FramesComposited int64
	FfmpegSpeedX     float64
	Codec            string
	Preset           string
	FfmpegThreads    int
	Status           string
	ErrorCode        string
	ErrorMessage     string
	SourceURLHash    string
	CacheKey         string
	InputDurationMS  float64
	OutputDurationMS float64
	MetadataJSON     string

	// Parallelism telemetry (migration 098).
	StartedOffsetMS  float64
	FinishedOffsetMS float64
	WorkerSlot       int
	CPUThreads       int
	ParallelGroup    string
}

// DetailedPhaseTiming is the parser-neutral representation of one C++
// sidecar phases[] event. It deliberately lives in pipeline (rather than
// taskrunner) so the native client can expose parsed telemetry without an
// import cycle. The executor converts it to taskrunner.DetailedPhaseTiming
// at the worker contract boundary.
type DetailedPhaseTiming struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	EventIndex       int64
	StartedAt        time.Time
	CompletedAt      time.Time
	DurationMS       int64
	Status           string
	ErrorCode        string
	ErrorMessage     string
	BytesIn          int64
	BytesOut         int64
	Frames           int64
	MetadataJSON     string
	SegmentIndex     int32
	TrackKind        string
	TrackIndex       int32
	StartedOffsetMS  float64
	FinishedOffsetMS float64
	CPUMS            float64
	QueueWaitMS      float64
	FramesIn         int64
	FramesOut        int64
}

// RenderClient is the interface for executing a RenderPlan.
// Implemented by the native C++ render client.
type RenderClient interface {
	Render(ctx context.Context, p *plan.RenderPlan) error
	RenderWithMetrics(ctx context.Context, p *plan.RenderPlan) (RenderMetrics, error)
}

// CompiledPlanV2Renderer is the narrow optional native surface used by the
// packet-copy executor. Keeping it separate preserves existing test clients
// and prevents legacy V1 pipeline callers from depending on V2.
type CompiledPlanV2Renderer interface {
	RenderCompiledPlanV2(context.Context, []byte, string) (RenderMetrics, error)
}

// RenderMetrics captures the native engine sidecar + subprocess wall-clock
// counters. The zero value is safe — executors that don't use native
// rendering return it unpopulated.
type RenderMetrics struct {
	Frames           int64
	FramesDecoded    int64
	FramesComposited int64
	Fps              float64
	SpeedX           float64
	EncodePasses     int64
	TempBytes        int64
	OutputDurable    bool
	// Native output identity is trusted only when SHA256Valid is true.
	SHA256           string
	SHA256Valid      bool
	OutputSizeBytes  int64
	BackwardSeekSeen bool
	DurationSec      float64
	ConcatMode       string
	TotalSize        int64
	OutTimeMs        int64
	Bitrate          float64
	DupFrames        int64
	DropFrames       int64
	PlanMarshalMs    int64
	PlanWriteMs      int64
	ProcessStartMs   int64
	ProcessWaitMs    int64
	TotalMs          int64
	// Process lifecycle counters. EngineSpawnCount is mapped from the
	// EXPLICIT spawn fact reported by runEngineProcess (cmd.Start()
	// succeeded) — never inferred from a timing value. The exec counts
	// cover the external tool processes the engine spawned in its own
	// process group, sampled from /proc by the native client while the
	// engine ran. They are zero when no native render occurred.
	EngineSpawnCount     int64
	EngineSpawnMs        int64
	ExternalProcessCount int64
	FfmpegExecCount      int64
	FfprobeExecCount     int64
	ShellExecCount       int64
	CurlExecCount        int64
	ChildWaitMs          int64 // Tree I/O totals measured from /proc/<pid>/io over the engine
	// process tree (engine + descendants): logical bytes read/written
	// through read()/write() including page-cache hits, and the
	// block-layer storage bytes after the page cache. Zero when no
	// native render occurred.
	TotalBytesRead      int64
	TotalBytesWritten   int64
	StorageBytesRead    int64
	StorageBytesWritten int64
	// CPU/RSS counters over the engine process tree, sampled from
	// /proc/<pid>/stat while the engine ran (engine + descendants):
	// CPUUserMs/CPUSystemMs are the summed utime/stime clock ticks
	// converted to milliseconds; PeakRSSBytes is the high-water mark of
	// the tree's resident set; CurrentRSSBytes is the last observed tree
	// RSS. Zero when no native render occurred.
	CPUUserMs       int64
	CPUSystemMs     int64
	PeakRSSBytes    int64
	CurrentRSSBytes int64
	// Engine-side real I/O counters reported by the C++ engine in the
	// sidecar io_counters block. Zero when the engine predates the block.
	FileCopyCount    int64
	FileCopyBytes    int64
	AssetBytesCopied int64
	InputOpenCount   int64
	InputReopenCount int64
	// OutputBackwardSeekCount/Bytes report how many times the C++ packet
	// output sink moved the write position below its hashed prefix (each
	// invalidates the opportunistic SHA) and the total rewound bytes.
	OutputBackwardSeekCount int64
	OutputBackwardSeekBytes int64
	// Engine-declared process counters reported by the C++ engine in the
	// sidecar process_counters block. They are DISJOINT facts from the
	// /proc sampler above: the engine counts the external tool processes
	// IT spawned (external/ffmpeg/ffprobe/shell/curl), the sampler counts
	// what it observed. EngineCPUUserMs/EngineCPUSystemMs are the engine
	// process's own getrusage; the context-switch and page-fault fields
	// are the engine's own scheduling/VM facts. Zero when the engine
	// predates the block. The Phase-1 copy-only invariant is
	// EngineExternalSpawnCount == 0.
	EngineExternalSpawnCount         int64
	EngineFfmpegSpawnCount           int64
	EngineFfprobeSpawnCount          int64
	EngineShellSpawnCount            int64
	EngineCurlSpawnCount             int64
	EngineCPUUserMs                  int64
	EngineCPUSystemMs                int64
	EngineVoluntaryContextSwitches   int64
	EngineInvoluntaryContextSwitches int64
	EngineMinorPageFaults            int64
	EngineMajorPageFaults            int64
	// Native output and durability timings are emitted by the C++ sidecar.
	FirstPacketReadMS  int64
	FirstOutputWriteMS int64
	FileFsyncMS        int64
	DirectoryFsyncMS   int64
	OutputRenameMS     int64
	// PhaseMS carries the per-phase engine timings from the C++ sidecar
	// (engine.asset_download, engine.segment_build, engine.concat, …).
	// Nil when no sidecar was read.
	PhaseMS map[string]float64
	// Segments carries the per-segment C++ sidecar timings, including
	// started/finished offsets and all parallelism fields.
	Segments []SegmentTiming
	// DetailedPhases carries the optional C++ sidecar phases[] stream.
	// It is nil for legacy sidecars that predate detailed events.
	DetailedPhases []DetailedPhaseTiming
	// Observability carries optional category summaries (audio, subtitles,
	// I/O, quality, retry, and wasted work) from newer sidecars. Legacy
	// sidecars leave it nil and remain fully compatible.
	Observability map[string]interface{}
	// FramePipeline carries the §25 producer-consumer health metrics of
	// the in-process encode pipeline (--render-frames sidecar block).
	// Zero when no encode-path job ran or the engine predates the block.
	FramePipeline FramePipelineMetrics
}

// FramePipelineMetrics is the §25 producer-consumer health report of the
// in-process encode pipeline (Decoder → FramePool → Render Producer →
// BoundedQueue → Encoder Consumer → Muxer), projected from the sidecar
// frame_pipeline block emitted by --render-frames.
//
// ProducerWaitMS = producer blocked on an empty render queue (decoder
// behind) plus a full encoder queue (backpressure); ConsumerWaitMS = the
// consumer blocked on an EMPTY encoder queue (encoder starvation).
// QueueEmptyMS/QueueFullMS are the same waits projected onto the queue;
// QueueDepthAvg is the time-weighted average depth. The ratios are
// dimensionless floats in [0, 1]; a zero value means not measured.
type FramePipelineMetrics struct {
	ProducerBusyMS         int64
	ProducerWaitMS         int64
	ConsumerBusyMS         int64
	ConsumerWaitMS         int64
	QueueDepthAvg          int64
	QueueDepthMax          int64
	QueueEmptyMS           int64
	QueueFullMS            int64
	ProducerStallRatio     float64
	EncoderStarvationRatio float64
	BackpressureRatio      float64
}

// RenderClient exposes the underlying render client so callers outside
// pkg/video (currently only pkg/bootstrap, for the engine self-test)
// can drive a single Render without rebuilding the renderer. The
// returned value is the SAME pointer the constructor was given;
// mutating it via the returned interface affects subsequent Run()
// calls — callers MUST treat it as read-only.
//
// This is a pure accessor — no side-effects, no logging, no caching.
// The intent is to keep pkg/bootstrap decoupled from the pipeline
// constructor's identity-check obligations while still letting the
// self-render step drive the canonical worker-side renderer.
func (r *Runner) RenderClient() RenderClient {
	if r == nil {
		return nil
	}
	return r.renderClient
}

// RenderCompiledPlanV2 delegates to the native zero-spawn V2 path. A runner
// without that capability fails closed; it never falls back to FFmpeg.
func (r *Runner) RenderCompiledPlanV2(ctx context.Context, planJSON []byte, outputPath string) (RenderMetrics, error) {
	if r == nil || r.renderClient == nil {
		return RenderMetrics{}, fmt.Errorf("pipeline: compiled V2 renderer is not configured")
	}
	renderer, ok := r.renderClient.(CompiledPlanV2Renderer)
	if !ok {
		return RenderMetrics{}, fmt.Errorf("pipeline: compiled V2 renderer is not supported")
	}
	return renderer.RenderCompiledPlanV2(ctx, planJSON, outputPath)
}

// Runner orchestrates: resolve compiler → validate → compile → render.
type Runner struct {
	registry     *Registry
	renderClient RenderClient
	logger       *logger.Logger
}

// NewRunner creates a pipeline runner with the given registry and render client.
func NewRunner(registry *Registry, client RenderClient, log *logger.Logger) *Runner {
	return &Runner{
		registry:     registry,
		renderClient: client,
		logger:       log,
	}
}

// RunMetrics aggregates the per-phase wall-clock timings and the
// native engine sidecar counters from a single pipeline.Runner.Run/
// RunWithMetrics invocation. Zero values are valid when the phase was
// skipped or the metric is unavailable.
type RunMetrics struct {
	ResolveMs  int64
	ValidateMs int64
	CompileMs  int64
	RenderMs   int64
	TotalMs    int64

	TimelineItems int
	AudioTracks   int

	// Native engine metrics (zero when no native rendering occurred)
	RenderMetrics
}

// RunWithMetrics executes the full pipeline and returns phase-level
// timings plus the native engine sidecar counters. Run() delegates to
// this method so existing callers are source-compatible.
func (r *Runner) RunWithMetrics(ctx context.Context, pipelineID string, jobID string, input map[string]interface{}, outputPath string) (m RunMetrics, err error) {
	start := time.Now()
	defer func() {
		// Preserve the total wall-clock duration on every early return,
		// including resolve/validate/compile/render failures.
		m.TotalMs = time.Since(start).Milliseconds()
	}()

	// Phase: resolve compiler
	resolveStart := time.Now()
	compiler, err := r.registry.Resolve(pipelineID)
	if err != nil {
		return m, fmt.Errorf("pipeline: resolve: %w", err)
	}
	m.ResolveMs = time.Since(resolveStart).Milliseconds()

	// Phase: validate
	r.logger.Info("[PIPELINE] Validating %s for job %s", pipelineID, jobID)
	validateStart := time.Now()
	if err := compiler.Validate(input); err != nil {
		return m, fmt.Errorf("pipeline: validate %s: %w", pipelineID, err)
	}
	m.ValidateMs = time.Since(validateStart).Milliseconds()

	// Phase: compile
	r.logger.Info("[PIPELINE] Compiling %s for job %s", pipelineID, jobID)
	compileStart := time.Now()
	p, err := compiler.Compile(ctx, jobID, input, outputPath)
	if err != nil {
		return m, fmt.Errorf("pipeline: compile %s: %w", pipelineID, err)
	}
	m.CompileMs = time.Since(compileStart).Milliseconds()

	if len(p.Timeline) == 0 {
		return m, fmt.Errorf("pipeline: compile %s produced empty timeline", pipelineID)
	}
	m.TimelineItems = len(p.Timeline)
	m.AudioTracks = len(p.AudioTracks)

	// Phase: render via native client (with metrics)
	r.logger.Info("[PIPELINE] Rendering %s for job %s (%d timeline items)", pipelineID, jobID, len(p.Timeline))
	renderStart := time.Now()
	nativeMetrics, renderErr := r.renderClient.RenderWithMetrics(ctx, p)
	m.RenderMs = time.Since(renderStart).Milliseconds()
	// Preserve native sidecar metrics before inspecting renderErr. Failed
	// and cancelled renders can still have completed phases/segments that
	// must remain available to waste and retry diagnostics.
	m.RenderMetrics = nativeMetrics
	if renderErr != nil {
		return m, fmt.Errorf("pipeline: render %s: %w", pipelineID, renderErr)
	}

	r.logger.Info("[PIPELINE] Completed %s for job %s", pipelineID, jobID)
	m.TotalMs = time.Since(start).Milliseconds()
	return m, nil
}

// Run executes the full pipeline for a job.
// It resolves the compiler, validates input, compiles the plan, and renders.
func (r *Runner) Run(ctx context.Context, pipelineID string, jobID string, input map[string]interface{}, outputPath string) error {
	_, err := r.RunWithMetrics(ctx, pipelineID, jobID, input, outputPath)
	return err
}
