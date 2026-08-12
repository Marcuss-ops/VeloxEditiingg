package performance

// benchmark_renderer_test.go exercises the production NativeRenderer
// against a FAKE zero-spawn engine: a shell script that parses the
// plan's output_path, writes a deterministic artifact and emits a
// copy-only-compliant sidecar (zero frames decoded/encoded, packet_copy
// concat mode, zero external spawns). The real engine e2e lives in the
// worker smoke; these tests pin the renderer contract.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeEngineScript is a minimal zero-spawn engine: it reads the plan
// (argv: --render --plan <path>), writes the artifact and a sidecar
// that satisfies every copy-only invariant. It is written with PURE
// shell builtins (read/printf/parameter expansion — no cat/grep/sed/
// python3) so the /proc sampler observes ZERO external execs, exactly
// like the real in-process libavformat packet path.
const fakeEngineScript = `#!/bin/sh
set -e
PLAN="$3"
OUT=""
while IFS= read -r line; do
  case "$line" in
    *'"output_path"'*)
      line="${line#*'"output_path"'}"
      line="${line#*:}"
      line="${line#*\"}"
      OUT="${line%%\"*}"
      break
      ;;
  esac
done < "$PLAN"
[ -n "$OUT" ] || exit 1
printf 'fake-mp4-content' > "$OUT"
printf '%s' '{"frames":0,"frames_decoded":0,"frames_composited":0,"fps":0,"speed_x":0,"encode_passes":0,"temp_bytes":0,"output_durable":true,"duration_seconds":300,"concat_mode":"packet_copy","total_size":17,"out_time_us":0,"out_time_ms":0,"bitrate":0,"dup_frames":0,"drop_frames":0,"phase_ms":{"packet_mux_ms":42},"io_counters":{"file_copy_count":0,"file_copy_bytes":0,"asset_bytes_copied":0,"input_open_count":24,"input_reopen_count":0},"process_counters":{"external_spawn_count":0,"ffmpeg_spawn_count":0,"ffprobe_spawn_count":0,"shell_spawn_count":0,"curl_spawn_count":0,"cpu_user_ms":0,"cpu_system_ms":0,"voluntary_context_switches":0,"involuntary_context_switches":0,"minor_page_faults":0,"major_page_faults":0},"phases":[{"origin":"engine","scope":"attempt","component":"engine","action":"packet_mux","phase":"composite","event_type":"phase","event_name":"packet_mux","duration_ms":42,"status":"completed","bytes_in":0,"bytes_out":17,"frames_in":0,"frames_out":0}],"segments":[]}' > "$OUT.progress.json"
exit 0
`

func writeFakeEngine(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "velox_video_engine")
	require.NoError(t, os.WriteFile(path, []byte(fakeEngineScript), 0o755))
	return path
}

func TestNativeRenderer_Render_Success(t *testing.T) {
	engineBin := writeFakeEngine(t)
	trackDir := writeTestTrack(t, testManifest())
	workDir := t.TempDir()

	renderer, err := NewNativeRenderer(NativeRendererConfig{
		TrackDir: trackDir, WorkDir: workDir, BinaryPath: engineBin,
		WorkerID: "test-worker", GitCommit: "abc123",
	})
	require.NoError(t, err)
	require.NotEqual(t, "", renderer.engineSHA256)

	fixture, ok := NewBenchmarkFixtureRegistry().Fixture(FixtureCopyOnlyCanonical5MV1)
	require.True(t, ok)

	result, err := renderer.Render(context.Background(), fixture)
	require.NoError(t, err)
	require.NotNil(t, result.Receipt)

	receipt := result.Receipt
	// Identity + workload.
	require.Equal(t, string(FixtureCopyOnlyCanonical5MV1), receipt.Identity.BenchmarkFixtureID)
	require.Equal(t, "test-worker", receipt.Identity.WorkerID)
	require.Equal(t, "abc123", receipt.Identity.GitCommit)
	require.NotEqual(t, "", receipt.Identity.EngineSHA256)
	require.Equal(t, 24, receipt.Workload.ClipCount)
	require.Equal(t, int64(300_000_000), receipt.Workload.DurationUS)
	require.True(t, receipt.Workload.CopyOnly)
	require.True(t, receipt.Workload.FinalAudioCopy)

	// Zero-spawn media contract: nothing decoded/encoded, packet_copy.
	require.Equal(t, "packet_copy", receipt.Media.ConcatMode)
	require.Equal(t, int64(0), receipt.Media.FramesDecoded)
	require.Equal(t, int64(0), receipt.Media.EncodePasses)
	require.Equal(t, int64(0), receipt.Process.ExternalProcessCount)
	require.Equal(t, int64(1), receipt.Process.EngineSpawnCount)
	require.Equal(t, int64(0), receipt.Process.EngineExternalSpawnCount)
	require.Equal(t, int64(17), receipt.IO.FinalBytesWritten)

	// Evidence: deterministic artifact + zero temp files.
	require.NotEqual(t, "", result.ArtifactSHA256)
	require.Equal(t, 0, result.Evidence.TempSegmentFiles)
	require.Empty(t, result.Evidence.TempFiles)

	// The tier-1 deterministic gate must pass with zero violations.
	violations := CheckFixtureGate(fixture, receipt, result.Evidence)
	require.Empty(t, violations)
}

func TestNativeRenderer_Render_GateCatchesTempFiles(t *testing.T) {
	trackDir := writeTestTrack(t, testManifest())
	workDir := t.TempDir()

	// A fake engine that leaves a temp segment file behind must fail the
	// zero-spawn invariant — the sweep + gate catch it. The leaky line
	// appends the leftover INSIDE the render run dir (dirname of $OUT).
	leakyScript := strings.Replace(fakeEngineScript, `printf 'fake-mp4-content' > "$OUT"`, `printf 'fake-mp4-content' > "$OUT"
printf 'leftover-segment' > "${OUT%/out.mp4}/seg_001.ts"`, 1)
	leakyBin := filepath.Join(t.TempDir(), "velox_video_engine")
	require.NoError(t, os.WriteFile(leakyBin, []byte(leakyScript), 0o755))

	renderer, err := NewNativeRenderer(NativeRendererConfig{TrackDir: trackDir, WorkDir: workDir, BinaryPath: leakyBin})
	require.NoError(t, err)
	fixture, _ := NewBenchmarkFixtureRegistry().Fixture(FixtureCopyOnlyCanonical5MV1)

	result, err := renderer.Render(context.Background(), fixture)
	require.NoError(t, err)
	require.Equal(t, 1, result.Evidence.TempSegmentFiles)
	violations := CheckFixtureGate(fixture, result.Receipt, result.Evidence)
	require.NotEmpty(t, violations)
}

func TestNativeRenderer_FailsClosed(t *testing.T) {
	t.Run("missing track manifest", func(t *testing.T) {
		_, err := NewNativeRenderer(NativeRendererConfig{TrackDir: t.TempDir(), BinaryPath: writeFakeEngine(t)})
		require.Error(t, err)
	})
	t.Run("missing engine binary", func(t *testing.T) {
		trackDir := writeTestTrack(t, testManifest())
		_, err := NewNativeRenderer(NativeRendererConfig{TrackDir: trackDir, BinaryPath: filepath.Join(t.TempDir(), "nope")})
		require.Error(t, err)
	})
	t.Run("empty track dir", func(t *testing.T) {
		_, err := NewNativeRenderer(NativeRendererConfig{})
		require.Error(t, err)
	})
	t.Run("non-canonical fixture rejected fast", func(t *testing.T) {
		trackDir := writeTestTrack(t, testManifest())
		renderer, err := NewNativeRenderer(NativeRendererConfig{TrackDir: trackDir, BinaryPath: writeFakeEngine(t)})
		require.NoError(t, err)
		other, _ := NewBenchmarkFixtureRegistry().Fixture(FixtureCopy5MLow)
		_, err = renderer.Render(context.Background(), other)
		require.Error(t, err)
		require.Contains(t, err.Error(), "only the canonical fixture")
	})
	t.Run("manifest spec mismatch", func(t *testing.T) {
		manifest := testManifest()
		manifest.SpecSHA256 = strings.Repeat("ff", 32) // not the pinned digest
		trackDir := writeTestTrack(t, manifest)
		renderer, err := NewNativeRenderer(NativeRendererConfig{TrackDir: trackDir, BinaryPath: writeFakeEngine(t)})
		require.NoError(t, err)
		fixture, _ := NewBenchmarkFixtureRegistry().Fixture(FixtureCopyOnlyCanonical5MV1)
		_, err = renderer.Render(context.Background(), fixture)
		require.Error(t, err)
		require.Contains(t, err.Error(), "spec digest")
	})
}
