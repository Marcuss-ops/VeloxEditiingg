package native

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// engine_sidecar.go owns the C++ engine sidecar format: the JSON
// schema emitted at <outputPath>.progress.json by the C++ engine.
// Fields are a subset of the emitted JSON needed for operator-visible
// telemetry; unrecognised keys are silently ignored by json.Decode.
//
// Official Attempt journal path for phases[]:
// readEngineSidecar -> mapEngineSidecar -> pipeline.RenderMetrics.DetailedPhases
// -> TaskRunner's importExecutorDetailedPhases -> EventRecorder.ImportCXX.
// No receipt, heartbeat, Prometheus, or TaskResult sink imports C++ events
// directly; they all project the recorder after that boundary.

// engineSidecar mirrors the C++ <output>.progress.json sidecar written
// by RenderEngine::emitSidecar.
type engineSidecar struct {
	Frames           int64              `json:"frames"`
	FramesDecoded    int64              `json:"frames_decoded"`
	FramesComposited int64              `json:"frames_composited"`
	Fps              float64            `json:"fps"`
	SpeedX           float64            `json:"speed_x"`
	EncodePasses     int64              `json:"encode_passes"`
	TempBytes        int64              `json:"temp_bytes"`
	OutputDurable    bool               `json:"output_durable"`
	SHA256           string             `json:"sha256"`
	SHA256Valid      bool               `json:"sha256_valid"`
	OutputSizeBytes  int64              `json:"output_size_bytes"`
	BackwardSeekSeen bool               `json:"backward_seek_seen"`
	DurationSec      float64            `json:"duration_seconds"`
	ConcatMode       string             `json:"concat_mode"`
	TotalSize        int64              `json:"total_size"`
	OutTimeUs        int64              `json:"out_time_us"`
	OutTimeMs        int64              `json:"out_time_ms"`
	Bitrate          float64            `json:"bitrate"`
	DupFrames        int64              `json:"dup_frames"`
	DropFrames       int64              `json:"drop_frames"`
	PhaseMS          map[string]float64 `json:"phase_ms,omitempty"`
	// Observability contains optional category summaries emitted by newer
	// engines. It is intentionally untyped at this compatibility boundary:
	// counters, ratios, booleans, and error classifications must all survive
	// sidecar -> RenderMetrics without changing the legacy JSON contract.
	Observability map[string]interface{} `json:"observability,omitempty"`
	Segments      []segmentTiming        `json:"segments,omitempty"`
	Phases        []detailedPhaseTiming  `json:"phases,omitempty"`
	// IOCounters carries the engine-side real I/O counters (file copies,
	// asset materialization bytes, avformat input opens) emitted by newer
	// engines. Legacy sidecars leave it nil.
	IOCounters *engineIOCounters `json:"io_counters,omitempty"`
	// ProcessCounters carries the engine-declared spawn ledger and the
	// engine process's getrusage usage (context switches, page faults,
	// engine CPU). Legacy sidecars leave it nil.
	ProcessCounters *engineProcessCounters `json:"process_counters,omitempty"`
	// FramePipeline carries the §25 producer-consumer health metrics of
	// the in-process encode pipeline (--render-frames). Only emitted by
	// engines with the frame_pipeline sidecar block; legacy sidecars
	// leave it nil.
	FramePipeline *engineFramePipeline `json:"frame_pipeline,omitempty"`
}

// engineFramePipeline mirrors the sidecar frame_pipeline block written by
// cmdRenderFrames for the Phase-3 in-process AVFrame encode pipeline:
//
//	Decoder → FramePool → Render Producer → BoundedQueue → Encoder
//	Consumer → Muxer
//
// producer_busy_ms/wait_ms and consumer_busy_ms/wait_ms are wall-clock
// milliseconds; queue_depth_avg is the time-weighted average depth of the
// encoder hand-off queue and queue_depth_max its high-water mark;
// queue_empty_ms/full_ms are the encoder-starvation and backpressure wait
// totals. The ratios are dimensionless floats in [0, 1].
type engineFramePipeline struct {
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

// engineIOCounters mirrors the sidecar io_counters block written by
// RenderEngine::sidecarJson. All values are zero on engines that predate
// the block.
type engineIOCounters struct {
	FirstPacketReadMS  int64 `json:"first_packet_read_ms"`
	FirstOutputWriteMS int64 `json:"first_output_write_ms"`
	FileFsyncMS        int64 `json:"file_fsync_ms"`
	DirectoryFsyncMS   int64 `json:"directory_fsync_ms"`
	OutputRenameMS     int64 `json:"output_rename_ms"`
	FileCopyCount      int64 `json:"file_copy_count"`
	FileCopyBytes      int64 `json:"file_copy_bytes"`
	AssetBytesCopied   int64 `json:"asset_bytes_copied"`
	InputOpenCount     int64 `json:"input_open_count"`
	InputReopenCount   int64 `json:"input_reopen_count"`
}

// engineProcessCounters mirrors the sidecar process_counters block
// written by RenderEngine::sidecarJson: the engine's OWN ledger of
// external tool spawns (disjoint from the Go-side /proc sampler — the
// engine counts what it launched) and the engine process's getrusage
// usage (CPU user/system ms, voluntary/involuntary context switches,
// minor/major page faults). All values are zero on engines that predate
// the block. The Phase-1 copy-only invariant external_spawn_count == 0
// is readable straight from this block.
type engineProcessCounters struct {
	ExternalSpawnCount int64 `json:"external_spawn_count"`
	FfmpegSpawnCount   int64 `json:"ffmpeg_spawn_count"`
	FfprobeSpawnCount  int64 `json:"ffprobe_spawn_count"`
	ShellSpawnCount    int64 `json:"shell_spawn_count"`
	CurlSpawnCount     int64 `json:"curl_spawn_count"`

	CPUUserMs                  int64 `json:"cpu_user_ms"`
	CPUSystemMs                int64 `json:"cpu_system_ms"`
	VoluntaryContextSwitches   int64 `json:"voluntary_context_switches"`
	InvoluntaryContextSwitches int64 `json:"involuntary_context_switches"`
	MinorPageFaults            int64 `json:"minor_page_faults"`
	MajorPageFaults            int64 `json:"major_page_faults"`
}

// segmentTiming mirrors the C++ SegmentTiming struct emitted inside
// the sidecar segments[] array.
// detailedPhaseTiming mirrors one event from the C++ phases[] sidecar
// array. All fields are optional at the wire boundary so older sidecars
// remain valid and newer fields can be adopted independently.
type detailedPhaseTiming struct {
	Origin           string          `json:"origin"`
	Scope            string          `json:"scope"`
	Component        string          `json:"component"`
	Action           string          `json:"action"`
	Phase            string          `json:"phase"`
	EventType        string          `json:"event_type"`
	EventName        string          `json:"event_name"`
	EventIndex       int64           `json:"event_index"`
	StartedAt        time.Time       `json:"started_at"`
	CompletedAt      time.Time       `json:"completed_at"`
	DurationMS       int64           `json:"duration_ms"`
	Status           string          `json:"status"`
	ErrorCode        string          `json:"error_code"`
	ErrorMessage     string          `json:"error_message"`
	BytesIn          int64           `json:"bytes_in"`
	BytesOut         int64           `json:"bytes_out"`
	Frames           int64           `json:"frames"`
	Metadata         json.RawMessage `json:"metadata"`
	MetadataJSON     string          `json:"metadata_json"`
	SegmentIndex     int32           `json:"segment_index"`
	TrackKind        string          `json:"track_kind"`
	TrackIndex       int32           `json:"track_index"`
	StartedOffsetMS  float64         `json:"started_offset_ms"`
	FinishedOffsetMS float64         `json:"finished_offset_ms"`
	CPUMS            float64         `json:"cpu_ms"`
	QueueWaitMS      float64         `json:"queue_wait_ms"`
	FramesIn         int64           `json:"frames_in"`
	FramesOut        int64           `json:"frames_out"`
}

type segmentTiming struct {
	Index            int64   `json:"index"`
	WorkerIndex      int64   `json:"worker_index"`
	SceneID          string  `json:"scene_id"`
	SourceType       string  `json:"source_type"`
	TotalMs          float64 `json:"total_ms"`
	AssetDownloadMs  float64 `json:"asset_download_ms"`
	FfmpegEncodeMs   float64 `json:"ffmpeg_encode_ms"`
	SourceBytes      int64   `json:"source_bytes"`
	OutputBytes      int64   `json:"output_bytes"`
	FramesEncoded    int64   `json:"frames_encoded"`
	FramesDecoded    int64   `json:"frames_decoded"`
	FramesComposited int64   `json:"frames_composited"`
	FfmpegSpeedX     float64 `json:"ffmpeg_speed_x"`
	Codec            string  `json:"codec"`
	Preset           string  `json:"preset"`
	FfmpegThreads    int64   `json:"ffmpeg_threads"`
	Status           string  `json:"status"`
	ErrorCode        string  `json:"error_code"`
	ErrorMessage     string  `json:"error_message"`
	SourceURLHash    string  `json:"source_url_hash"`
	CacheKey         string  `json:"cache_key"`
	InputDurationMs  float64 `json:"input_duration_ms"`
	OutputDurationMs float64 `json:"output_duration_ms"`
	MetadataJSON     string  `json:"metadata_json"`

	// Parallelism telemetry (migration 098).
	StartedOffsetMs  float64 `json:"started_offset_ms"`
	FinishedOffsetMs float64 `json:"finished_offset_ms"`
	WorkerSlot       int64   `json:"worker_slot"`
	CpuThreads       int64   `json:"cpu_threads"`
	ParallelGroup    string  `json:"parallel_group"`
}

// readEngineSidecar reads and parses the C++ sidecar at
// <outputPath>.progress.json. Returns a zero-value EngineSidecar if
// the file does not exist or cannot be parsed — callers treat missing
// sidecar as a non-fatal condition.
func readEngineSidecar(outputPath string) (engineSidecar, error) {
	var sc engineSidecar
	sidecarPath := outputPath + ".progress.json"
	f, err := os.Open(sidecarPath)
	if err != nil {
		return sc, fmt.Errorf("open sidecar %s: %w", sidecarPath, err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&sc); err != nil {
		return sc, fmt.Errorf("decode sidecar %s: %w", sidecarPath, err)
	}
	return sc, nil
}
