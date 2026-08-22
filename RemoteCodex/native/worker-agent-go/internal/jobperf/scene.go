package jobperf

import "sort"

// SceneMetrics is the per-scene performance breakdown. One row per
// timeline scene; the report derives TOP SLOWEST SCENES from it.
type SceneMetrics struct {
	SceneID          string  `json:"scene_id"`
	SourceDurationMS float64 `json:"source_duration_ms"`
	OutputDurationMS float64 `json:"output_duration_ms"`

	DownloadMS  float64 `json:"download_ms"`
	DecodeMS    float64 `json:"decode_ms"`
	SubtitleMS  float64 `json:"subtitle_ms"`
	WatermarkMS float64 `json:"watermark_ms"`
	BlurMS      float64 `json:"blur_ms"`
	EncodeMS    float64 `json:"encode_ms"`

	TotalMS float64 `json:"total_ms"`

	InputBytes  int64 `json:"input_bytes"`
	OutputBytes int64 `json:"output_bytes"`

	FramesDecoded    int64   `json:"frames_decoded"`
	FramesEncoded    int64   `json:"frames_encoded"`
	FramesComposited int64   `json:"frames_composited"`
	Fps              float64 `json:"fps"`
	SpeedX           float64 `json:"render_speed"`

	PacketCopy bool   `json:"packet_copy"`
	Status     string `json:"status,omitempty"`
}

// TopSlowestScenes returns up to n scenes ordered by TotalMS descending.
func TopSlowestScenes(scenes []SceneMetrics, n int) []SceneMetrics {
	if len(scenes) == 0 || n <= 0 {
		return nil
	}
	out := append([]SceneMetrics(nil), scenes...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].TotalMS > out[j].TotalMS })
	if len(out) > n {
		out = out[:n]
	}
	return out
}
