package native

import (
	"encoding/json"
	"fmt"
	"os"
)

// engine_sidecar.go owns the C++ engine sidecar format: the JSON
// schema emitted at <outputPath>.progress.json by the C++ engine.
// Fields are a subset of the emitted JSON needed for operator-visible
// telemetry; unrecognised keys are silently ignored by json.Decode.

// engineSidecar mirrors the C++ <output>.progress.json sidecar written
// by RenderEngine::emitSidecar.
type engineSidecar struct {
	Frames       int64              `json:"frames"`
	Fps          float64            `json:"fps"`
	SpeedX       float64            `json:"speed_x"`
	EncodePasses int64              `json:"encode_passes"`
	TempBytes    int64              `json:"temp_bytes"`
	DurationSec  float64            `json:"duration_seconds"`
	ConcatMode   string             `json:"concat_mode"`
	TotalSize    int64              `json:"total_size"`
	OutTimeUs    int64              `json:"out_time_us"`
	OutTimeMs    int64              `json:"out_time_ms"`
	Bitrate      float64            `json:"bitrate"`
	DupFrames    int64              `json:"dup_frames"`
	DropFrames   int64              `json:"drop_frames"`
	PhaseMS      map[string]float64 `json:"phase_ms,omitempty"`
	Segments     []segmentTiming    `json:"segments,omitempty"`
}

// segmentTiming mirrors the C++ SegmentTiming struct emitted inside
// the sidecar segments[] array.
type segmentTiming struct {
	Index            int64   `json:"index"`
	WorkerIndex      int64   `json:"worker_index"`
	SourceType       string  `json:"source_type"`
	TotalMs          float64 `json:"total_ms"`
	AssetDownloadMs  float64 `json:"asset_download_ms"`
	FfmpegEncodeMs   float64 `json:"ffmpeg_encode_ms"`
	SourceBytes      int64   `json:"source_bytes"`
	OutputBytes      int64   `json:"output_bytes"`
	FramesEncoded    int64   `json:"frames_encoded"`
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
