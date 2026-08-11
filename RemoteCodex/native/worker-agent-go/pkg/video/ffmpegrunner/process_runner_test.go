package ffmpegrunner

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeStub writes an executable shell script that stands in for the
// ffmpeg binary. ProcessRunner runs whatever Binary points at, so a stub
// keeps tests hermetic (no real ffmpeg required on the host).
func writeStub(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stub.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	return path
}

func runStub(t *testing.T, body string, args ...string) (FFmpegResult, error) {
	t.Helper()
	runner := &ProcessRunner{Binary: writeStub(t, body)}
	return runner.Run(context.Background(), FFmpegRequest{Operation: OperationCompose, Args: args})
}

func TestProcessRunner_Success(t *testing.T) {
	result, err := runStub(t, "exit 0", "-i", "in.mp4", "-y", "out.mp4")
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", result.ExitCode)
	}
	if result.TerminatedBySignal {
		t.Error("TerminatedBySignal = true, want false")
	}
	if result.CommandFingerprint == "" {
		t.Error("CommandFingerprint must be populated")
	}
	if result.Parameters.InputCount != 1 {
		t.Errorf("Parameters.InputCount = %d, want 1", result.Parameters.InputCount)
	}
}

func TestProcessRunner_NonZeroExit(t *testing.T) {
	result, err := runStub(t, "exit 3")
	if err == nil {
		t.Fatal("Run = nil, want error for non-zero exit")
	}
	if !strings.Contains(err.Error(), "exit status 3") {
		t.Errorf("error = %q, want to mention exit status 3", err.Error())
	}
	if result.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", result.ExitCode)
	}
}

func TestProcessRunner_StartFailure(t *testing.T) {
	runner := &ProcessRunner{Binary: "/definitely/not/a/real/binary"}
	result, err := runner.Run(context.Background(), FFmpegRequest{Operation: OperationEncode, Args: []string{"-y", "out.mp4"}})
	if err == nil {
		t.Fatal("Run = nil, want start error")
	}
	if result.ExitCode != -1 {
		t.Errorf("ExitCode = %d, want -1 (start failure sentinel)", result.ExitCode)
	}
	if result.CommandFingerprint == "" {
		t.Error("fingerprint must be populated even on start failure")
	}
}

func TestProcessRunner_WallTimeMeasured(t *testing.T) {
	result, err := runStub(t, "sleep 0.15")
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.ProcessWallMS < 100 {
		t.Errorf("ProcessWallMS = %d, want >= 100 for a 150ms stub", result.ProcessWallMS)
	}
	if result.ProcessSpawnMS < 0 {
		t.Errorf("ProcessSpawnMS = %d, want >= 0", result.ProcessSpawnMS)
	}
}

func TestProcessRunner_PreCanceledContextFailsFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before start
	runner := &ProcessRunner{Binary: writeStub(t, "sleep 5")}
	result, err := runner.Run(ctx, FFmpegRequest{Operation: OperationEncode, Args: []string{"-y", "out.mp4"}})
	if err == nil {
		t.Fatal("Run = nil, want context-cancellation error")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("error = %q, want context canceled", err.Error())
	}
	if result.ExitCode != -1 {
		t.Errorf("ExitCode = %d, want -1 (process never started)", result.ExitCode)
	}
}

func TestProcessRunner_CancellationMidRunKillsProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &ProcessRunner{Binary: writeStub(t, "sleep 30")}
	done := make(chan struct{})
	var result FFmpegResult
	var err error
	go func() {
		result, err = runner.Run(ctx, FFmpegRequest{Operation: OperationEncode, Args: []string{"-y", "out.mp4"}})
		close(done)
	}()
	time.Sleep(150 * time.Millisecond) // let the child actually start
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after mid-run cancellation")
	}
	if err == nil {
		t.Fatal("Run = nil, want error after mid-run cancellation")
	}
	if !result.TerminatedBySignal {
		t.Error("TerminatedBySignal = false, want true (SIGKILL by CommandContext)")
	}
	if result.ExitCode != -1 {
		t.Errorf("ExitCode = %d, want -1 (killed by signal)", result.ExitCode)
	}
}

func TestProcessRunner_StderrCaptured(t *testing.T) {
	var stderr bytes.Buffer
	runner := &ProcessRunner{
		Binary: writeStub(t, "echo diagnostic >&2"),
		Stderr: &stderr,
	}
	_, err := runner.Run(context.Background(), FFmpegRequest{Operation: OperationAudioMix, Args: nil})
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if !strings.Contains(stderr.String(), "diagnostic") {
		t.Errorf("stderr = %q, want captured diagnostic line", stderr.String())
	}
}

func TestProcessRunner_CPUTimesAndFingerprintPresent(t *testing.T) {
	result, err := runStub(t, "i=0; while [ $i -lt 5000 ]; do i=$((i+1)); done; exit 0")
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.UserCPUMs < 0 || result.SystemCPUMs < 0 {
		t.Errorf("CPU times must be non-negative, got user=%d sys=%d", result.UserCPUMs, result.SystemCPUMs)
	}
	if result.PeakRSSBytes < 0 {
		t.Errorf("PeakRSSBytes = %d, want >= 0", result.PeakRSSBytes)
	}
	want := Fingerprint(FFmpegRequest{Operation: OperationCompose, Args: nil})
	if result.CommandFingerprint != want {
		t.Errorf("fingerprint = %q, want %q (computed once at Run entry)", result.CommandFingerprint, want)
	}
}

func TestProcessRunner_EnvForwarded(t *testing.T) {
	// Stub echoes an env var we inject through the runner.
	runner := &ProcessRunner{
		Binary: writeStub(t, "test \"$VELOX_STUB_TOKEN\" = \"s3cr3t\" && exit 0 || exit 9"),
		Env:    []string{"VELOX_STUB_TOKEN=s3cr3t"},
	}
	result, err := runner.Run(context.Background(), FFmpegRequest{Operation: OperationCompose, Args: nil})
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0 (env must reach the child)", result.ExitCode)
	}
}

func TestMergeEnv_OverrideWinsByKey(t *testing.T) {
	parent := []string{"PATH=/usr/bin", "VELOX_DUP=inherited", "KEEP=yes"}
	override := []string{"VELOX_DUP=override", "VELOX_NEW=added"}
	got := mergeEnv(parent, override)
	seen := map[string]string{}
	for _, entry := range got {
		eq := strings.IndexByte(entry, '=')
		if eq > 0 {
			seen[entry[:eq]] = entry[eq+1:]
		}
	}
	if seen["VELOX_DUP"] != "override" {
		t.Errorf("VELOX_DUP = %q, want override (inherited value must be shadowed)", seen["VELOX_DUP"])
	}
	if seen["VELOX_NEW"] != "added" {
		t.Errorf("VELOX_NEW = %q, want added", seen["VELOX_NEW"])
	}
	if seen["KEEP"] != "yes" || seen["PATH"] != "/usr/bin" {
		t.Errorf("unrelated parent entries lost: %v", got)
	}
}

func TestProcessRunner_EnvOverrideWins(t *testing.T) {
	// The parent environment is inherited with the same key present; the
	// override must win (dedupe by key, not naive append).
	t.Setenv("VELOX_STUB_TOKEN", "inherited")
	runner := &ProcessRunner{
		Binary: writeStub(t, "test \"$VELOX_STUB_TOKEN\" = \"override\" && exit 0 || exit 9"),
		Env:    []string{"VELOX_STUB_TOKEN=override"},
	}
	result, err := runner.Run(context.Background(), FFmpegRequest{Operation: OperationCompose, Args: nil})
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0 (override must shadow the inherited value)", result.ExitCode)
	}
}

func TestProcessRunner_PhaseTimingBreakdown(t *testing.T) {
	// Stub writes its first progress line immediately, then does ~200ms of
	// work, then writes the last line and exits. Expected decomposition:
	//   first_output_ms small (immediate first byte)
	//   processing_ms  ≥ 150ms (the work window)
	//   exit_wait_ms   ≥ 0 (reap only)
	//   first_output + processing + exit_wait ≈ process_wall_ms
	result, err := runStub(t, "printf 'start\\n'; sleep 0.2; printf 'end\\n'")
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.FirstOutputMS < 0 || result.FirstOutputMS > 1000 {
		t.Errorf("FirstOutputMS = %d, want small non-negative", result.FirstOutputMS)
	}
	if result.ProcessingMS < 150 {
		t.Errorf("ProcessingMS = %d, want >= 150 (the sleep window)", result.ProcessingMS)
	}
	if result.ExitWaitMS < 0 {
		t.Errorf("ExitWaitMS = %d, want >= 0", result.ExitWaitMS)
	}
	sum := result.FirstOutputMS + result.ProcessingMS + result.ExitWaitMS
	if delta := sum - result.ProcessWallMS; delta > 100 || delta < -100 {
		t.Errorf("first_output+processing+exit_wait = %d vs process_wall_ms = %d (delta %d), want within ±100", sum, result.ProcessWallMS, delta)
	}
	if result.Operation != OperationCompose {
		t.Errorf("Operation = %q, want compose (stamped from request)", result.Operation)
	}
}

func TestProcessRunner_SilentProcessHasNoPhaseTrio(t *testing.T) {
	// No output at all: the phase trio stays zero (documented) while wall
	// and exit code remain complete. A short sleep keeps wall measurable
	// (a sub-millisecond process legitimately rounds wall to 0ms).
	result, err := runStub(t, "sleep 0.05; exit 0")
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.FirstOutputMS != 0 || result.ProcessingMS != 0 {
		t.Errorf("phase trio = first_output=%d processing=%d, want 0/0 for a silent process", result.FirstOutputMS, result.ProcessingMS)
	}
	if result.ProcessWallMS < 30 {
		t.Errorf("ProcessWallMS = %d, want >= 30 for a 50ms silent process", result.ProcessWallMS)
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", result.ExitCode)
	}
}

func TestProcessRunner_FirstOutputPrecedesProcessingEnd(t *testing.T) {
	// The first byte arrives after a 50ms setup delay, then 100ms of work:
	// first_output_ms must be measurable and strictly precede the work
	// window, which must fit inside the wall window. (An instant first byte
	// truncates to 0ms at ms granularity, so the stub delays it — real
	// ffmpeg spends tens-to-hundreds of ms in setup before first output.)
	result, err := runStub(t, "sleep 0.05; printf 'ping\\n'; sleep 0.1; printf 'pong\\n'")
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if result.FirstOutputMS < 30 {
		t.Errorf("FirstOutputMS = %d, want >= 30 (the 50ms setup delay)", result.FirstOutputMS)
	}
	if result.ProcessingMS < 80 {
		t.Errorf("ProcessingMS = %d, want >= 80 (the 100ms work window)", result.ProcessingMS)
	}
	if result.FirstOutputMS >= result.ProcessWallMS {
		t.Errorf("FirstOutputMS = %d must be < ProcessWallMS = %d", result.FirstOutputMS, result.ProcessWallMS)
	}
	if result.FirstOutputMS >= result.ProcessingMS {
		t.Errorf("FirstOutputMS = %d must precede ProcessingMS = %d", result.FirstOutputMS, result.ProcessingMS)
	}
}

func TestNewProcessRunner_DefaultBinary(t *testing.T) {
	r := NewProcessRunner()
	if r.binary() != "ffmpeg" {
		t.Errorf("binary() = %q, want default ffmpeg", r.binary())
	}
	if r.ioPoll() != 50*time.Millisecond {
		t.Errorf("ioPoll() = %v, want default 50ms", r.ioPoll())
	}
}
