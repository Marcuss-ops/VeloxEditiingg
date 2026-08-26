package telemetry
type PerformanceReport struct {
	// ── Summary ────────────────────────────────────────────────────
	MediaDurationSeconds float64
	WallClockSeconds     float64
	RealTimeFactor       float64 // wall / media (lower is better)
	ThroughputX          float64 // media / wall (higher is better)

	// ── Input ──────────────────────────────────────────────────────
	SceneCount       int
	SourceDurationMs int64
	OutputDurationMs int64
	TotalInputBytes  int64

	// ── Cache ──────────────────────────────────────────────────────
	CacheHitCount  int64
	CacheMissCount int64
	CacheHitRatio  float64
	CacheHitBytes  int64
	CacheMissBytes int64

	// ── Phase breakdown (ordered, non-zero phases only) ────────────
	Phases []PhaseBreakdown

	// ── Segment stats ──────────────────────────────────────────────
	SegmentsTotal     int32
	SegmentsPacketCopy int32
	SegmentsReencoded  int32
	SegmentsComposited int32
	PacketCopyBytes    int64
	ReencodedBytes     int64
	PacketCopyRatio    float64

	// ── GPU ────────────────────────────────────────────────────────
	GPUUtilAvgPct    float64
	GPUUtilPeakPct   float64
	NVDECUtilAvgPct   float64
	NVDECUtilPeakPct  float64
	NVENCUtilAvgPct   float64
	NVENCUtilPeakPct  float64
	VRAMUsedAvgBytes  int64
	VRAMUsedPeakBytes int64
	GPUIdleMs         int64

	// ── GPU transfers ──────────────────────────────────────────────
	FramesDownloadedGPU int64
	FramesUploadedGPU   int64
	GPUToCPUBytes       int64
	CPUToGPUBytes       int64

	// ── CPU ────────────────────────────────────────────────────────
	CPUPercentAvg  float64
	CPUPercentPeak float64
	CPUTotalMs     int64
	PeakRSSBytes   int64

	// ── Output ─────────────────────────────────────────────────────
	OutputBytes    int64
	OutputSHA256   string
	FfprobeValid   bool
	DurationDiffSec float64

	// ── Audio ──────────────────────────────────────────────────────
	AudioCopyMs   int64
	AudioEncodeMs int64
	AudioPacketsCopied  int64
	AudioPacketsEncoded int64
	AudioInputBytes     int64
	AudioOutputBytes    int64

	// ── Per-scene breakdown ────────────────────────────────────────
	TopSlowestScenes []SceneReport

	// ── Critical path ──────────────────────────────────────────────
	CriticalPathComponent string
	CriticalPathMs        int64
	CriticalPathPercent   float64

	// ── Process spawn ──────────────────────────────────────────────
	FFmpegExecCount   int64
	FFprobeExecCount  int64
	ProcessSpawnCount int64
	FFmpegProcessMs   int64
	FFprobeProcessMs  int64
	ProcessStartupMs  int64

	// ── Download / Disk ────────────────────────────────────────────
	DriveDownloadMs      int64
	BlobstoreDownloadMs  int64
	LocalCacheReadMs     int64
	AssetDownloadWaitMs  int64
	DownloadMbpsAvg      float64
	UploadMbpsAvg        float64
	DriveUploadMbps      float64
	ArtifactDownloadMbps float64
	DiskReadMs           int64
	DiskWriteMs          int64
	OutputWriteMs        int64
	TempWriteMs          int64
	FinalReadMs          int64
}

// PhaseBreakdown is one phase's contribution to the job total.
type PhaseBreakdown struct {
	Name       string
	Label      string
	DurationMs int64
	Percent    float64
	Count      int64
	BytesIn    int64
	BytesOut   int64
	FramesIn   int64
	FramesOut  int64
}

// SceneReport is one scene's aggregated metrics.
type SceneReport struct {
	SceneID          string
	TotalMs          int64
	SourceDurationMs int64
	OutputDurationMs int64
	FramesDecoded    int64
	FramesEncoded    int64
	RenderSpeed      float64
	InputBytes       int64
	OutputBytes      int64
}

// ── Builder ────────────────────────────────────────────────────────────────

// BuildPerformanceReport constructs a report from the job timer, raw metrics,
