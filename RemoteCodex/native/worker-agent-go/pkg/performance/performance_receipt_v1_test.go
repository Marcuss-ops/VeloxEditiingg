package performance

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// fullReceipt returns a receipt with every section populated, used as
// the round-trip fixture.
func fullReceipt() *PerformanceReceiptV1 {
	return &PerformanceReceiptV1{
		Version: PerformanceReceiptVersionV1,
		Identity: PerformanceIdentity{
			JobID:              "job_123",
			AttemptID:          "attempt_456",
			TaskID:             "task_789",
			WorkerID:           "worker_42",
			Release:            "2026.08.1",
			GitCommit:          "fbbf7c1",
			EngineVersion:      "1.2.3",
			EngineSHA256:       "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
			Kernel:             "6.8.0",
			Architecture:       "amd64",
			CPUModel:           "Intel(R) Xeon(R) Gold 6230",
			BenchmarkFixtureID: "COPY_ONLY_CANONICAL_5M_V1",
		},
		Workload: WorkloadProfile{
			JobType:        "process_video",
			DurationUS:     300_000_000,
			ClipCount:      25,
			AssetCount:     26,
			CopyOnly:       true,
			FinalAudioCopy: true,
			VideoCodec:     "h264",
			AudioCodec:     "aac",
			Width:          1920,
			Height:         1080,
			FPS:            30,
		},
		Timing: TimingMetrics{
			WallMs:          19340,
			ResolveMs:       120,
			ValidateMs:      40,
			CompileMs:       210,
			RenderMs:        18910,
			PipelineTotalMs: 19310,
			PlanMarshalMs:   55,
			PlanWriteMs:     12,
			ProcessStartMs:  85,
			ProcessWaitMs:   18700,
			NativeTotalMs:   18800,
			ExecutorTotalMs: 19340,
			ArtifactTotalMs: 210,
		},
		Process: ProcessMetrics{
			EngineSpawnCount:     1,
			EngineSpawnMs:        85,
			ExternalProcessCount: 64,
			FfmpegExecCount:      32,
			FfprobeExecCount:     25,
			ShellExecCount:       6,
			CurlExecCount:        1,
			ChildWaitMs:          18700,
		},
		IO: IOMetrics{
			TotalBytesRead:    1_800_000_000,
			TotalBytesWritten: 1_200_000_000,
			AssetBytesRead:    1_500_000_000,
			AssetBytesCopied:  1_200_000_000,
			TempBytesWritten:  900_000_000,
			MuxBytesRead:      1_100_000_000,
			MuxBytesWritten:   300_000_000,
			FinalBytesWritten: 300_000_000,
			FileCopyCount:     40,
			FileCopyBytes:     1_200_000_000,
			InputOpenCount:    30,
			InputReopenCount:  7,
		},
		Media: MediaMetrics{
			Frames:           5000,
			FramesDecoded:    0,
			FramesComposited: 0,
			EncodePasses:     0,
			TempBytes:        0,
			OutputBytes:      300_000_000,
			DurationSec:      300,
			ConcatMode:       "copy",
			FPS:              30,
			SpeedX:           42.5,
			Bitrate:          8_000_000,
			DupFrames:        0,
			DropFrames:       0,
		},
		Memory: MemoryMetrics{
			PeakRSSBytes:    512_000_000,
			CurrentRSSBytes: 480_000_000,
		},
		Scheduling: SchedulingMetrics{
			QueueWaitMS:  1200,
			OffCPUMs:     14_500,
			OffCPUReason: OffCPUReasonProcessWait,
		},
		Phases: []PhaseTiming{
			{Name: "media.open", DurationMS: 400, CPUMs: 150, BytesIn: 1_800_000_000, BytesOut: 0},
			{Name: "mux.trailer", DurationMS: 30, CPUMs: 25, BytesIn: 0, BytesOut: 300_000_000},
		},
		Segments: []SegmentTiming{
			{SegmentIndex: 0, SceneID: "scene_1", DurationMS: 12000, SourceBytes: 60_000_000, OutputBytes: 60_000_000, Status: "ok"},
			{SegmentIndex: 1, SceneID: "scene_2", DurationMS: 10000, SourceBytes: 60_000_000, OutputBytes: 60_000_000, Status: "ok"},
		},
		Derived: DerivedMetrics{
			UnaccountedMS:      430,
			AccountedRatio:     0.978,
			ReadAmplification:  6.0,
			WriteAmplification: 4.0,
			ProcessesPerClip:   2.56,
			UsefulWorkRatio:    0.35,
			CPUWallRatio:       0.155,
		},
	}
}

func TestNewPerformanceReceiptV1_StampsVersion(t *testing.T) {
	r := NewPerformanceReceiptV1()
	require.NotNil(t, r)
	require.Equal(t, PerformanceReceiptVersionV1, r.Version)
}

func TestPerformanceReceiptV1_JSONRoundTrip(t *testing.T) {
	want := fullReceipt()

	data, err := json.Marshal(want)
	require.NoError(t, err)

	var got PerformanceReceiptV1
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, *want, got, "receipt must survive a JSON round trip without field loss")
}

func TestPerformanceReceiptV1_JSONKeys(t *testing.T) {
	// Parity discipline (same spirit as the payload_contract_parity
	// tests): the marshaled document must use the canonical snake_case
	// keys that the plan and future storage/compare tooling read.
	data, err := json.Marshal(fullReceipt())
	require.NoError(t, err)

	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &doc))

	require.Equal(t, `1`, string(doc["version"]), "version key must be present")

	for section, keys := range map[string][]string{
		"identity": {
			"job_id", "attempt_id", "task_id", "worker_id",
			"release", "git_commit", "engine_version", "engine_sha256",
			"kernel", "architecture", "cpu_model", "benchmark_fixture_id",
		},
		"workload": {
			"job_type", "duration_us", "clip_count", "asset_count",
			"copy_only", "final_audio_copy", "video_codec", "audio_codec",
			"width", "height", "fps",
		},
		"timing": {
			"wall_ms", "resolve_ms", "validate_ms", "compile_ms", "render_ms",
			"pipeline_total_ms", "plan_marshal_ms", "plan_write_ms",
			"process_start_ms", "process_wait_ms", "native_total_ms",
			"executor_total_ms", "artifact_total_ms",
		},
		"process": {
			"engine_spawn_count", "engine_spawn_ms", "external_process_count",
			"ffmpeg_exec_count", "ffprobe_exec_count", "shell_exec_count",
			"curl_exec_count", "child_wait_ms",
		},
		"io": {
			"total_bytes_read", "total_bytes_written",
			"asset_bytes_read", "asset_bytes_copied", "temp_bytes_written",
			"mux_bytes_read", "mux_bytes_written", "final_bytes_written",
			"file_copy_count", "file_copy_bytes",
			"input_open_count", "input_reopen_count",
		},
		"media": {
			"frames", "frames_decoded", "frames_composited", "encode_passes",
			"temp_bytes", "output_bytes", "duration_sec", "concat_mode",
			"fps", "speed_x", "bitrate", "dup_frames", "drop_frames",
		},
		"memory":     {"peak_rss_bytes", "current_rss_bytes"},
		"scheduling": {"queue_wait_ms", "off_cpu_ms", "off_cpu_reason"},
		"derived": {
			"unaccounted_ms", "accounted_ratio",
			"read_amplification", "write_amplification",
			"processes_per_clip", "useful_work_ratio", "cpu_wall_ratio",
		},
	} {
		sectionData, ok := doc[section]
		require.True(t, ok, "section %q must be present in the marshaled receipt", section)
		var sectionDoc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(sectionData, &sectionDoc))
		for _, key := range keys {
			_, ok := sectionDoc[key]
			require.True(t, ok, "section %q must expose key %q", section, key)
		}
	}

	var phases []PhaseTiming
	require.NoError(t, json.Unmarshal(doc["phases"], &phases))
	require.Len(t, phases, 2, "phases must marshal as the canonical phase objects")
	require.Equal(t, "media.open", phases[0].Name)
	require.Equal(t, int64(400), phases[0].DurationMS)

	var segments []SegmentTiming
	require.NoError(t, json.Unmarshal(doc["segments"], &segments))
	require.Len(t, segments, 2, "segments must marshal as the canonical segment objects")
	require.Equal(t, "scene_1", segments[0].SceneID)
	require.Equal(t, 12000.0, segments[0].DurationMS)
}

func TestPerformanceReceiptV1_ZeroValueMarshals(t *testing.T) {
	var zero PerformanceReceiptV1
	data, err := json.Marshal(&zero)
	require.NoError(t, err)
	require.Contains(t, string(data), `"version":0`)
	require.Contains(t, string(data), `"identity"`)
	require.Contains(t, string(data), `"derived"`)
}

func TestPerformanceReceiptV1_ToJSON(t *testing.T) {
	data, err := NewPerformanceReceiptV1().ToJSON()
	require.NoError(t, err)
	require.True(t, json.Valid(data), "ToJSON must produce valid JSON")
}

func TestPerformanceReceiptV1_ToJSON_Nil(t *testing.T) {
	var nilReceipt *PerformanceReceiptV1
	_, err := nilReceipt.ToJSON()
	require.Error(t, err, "nil receipt must not silently marshal to null")
}
