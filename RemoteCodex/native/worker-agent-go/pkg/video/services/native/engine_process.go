package native

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// engine_process.go owns the subprocess lifecycle of the C++ engine:
// launch with the crash-safety backstop (Setpgid + Pdeathsig), stream
// wiring, and the SIGTERM-grace-then-SIGKILL policy on context
// cancellation. It is intentionally independent from the sidecar
// parser and the metrics mapping so the process-tree termination path
// can be exercised in isolation against a fake engine binary that
// never emits a sidecar.

// runEngineProcess launches velox_video_engine --render --plan and
// returns (engineStarted, processStartMs, processWaitMs, stderr, stdout,
// telemetry, err).
//
// engineStarted is the EXPLICIT spawn fact: true exactly when cmd.Start()
// succeeded, observed at the process-runner boundary. It is never derived
// from a timing value (ProcessStartMs > 0) — the caller maps this fact onto
// EngineSpawnCount and emits the canonical worker.engine.spawn event.
//
// err semantics:
//   - ctx.Err() (context.Canceled or DeadlineExceeded) when the caller
//     cancelled via the context — metrics fields below are populated
//     up through ProcessStartMs; ProcessWaitMs is zero
//   - raw exec.ExitError-equivalent from cmd.Wait() when the engine
//     failed — ProcessStartMs AND ProcessWaitMs are populated; the
//     caller is responsible for wrapping the error with stderr/stdout
//
// telemetry reports the external processes the engine spawned in its
// own process group and the tree's byte counters, sampled from /proc
// while it ran. It is populated on every exit path once the engine was
// started (the monitor is stopped by the deferred cleanup below before
// this function returns).
//
// SAFETY-CRITICAL: Setpgid + Pdeathsig + 10s SIGTERM grace + SIGKILL
// hard-kill + <-done reaping are preserved verbatim from the original
// render_client.go. Do not modify these.
func runEngineProcess(ctx context.Context, binaryPath, planPath string, onProgress DetailedProgressFunc, legacyProgress ProgressFunc) (engineStarted bool, processStartMs int64, processWaitMs int64, stderrBuf strings.Builder, stdoutBuf strings.Builder, telemetry ProcessTelemetry, err error) {
	args := []string{"--render", "--plan", planPath}
	if chrononBackendEnabled() {
		args = []string{"render-plan", "--input", planPath}
	}
	cmd := exec.Command(binaryPath, args...)
	// The worker path enables exact decoded-frame telemetry in the native
	// engine. The CLI keeps the probe opt-in so standalone renders are not
	// burdened with showinfo diagnostics unless explicitly requested.
	cmd.Env = append(os.Environ(), "VELOX_FFMPEG_DECODE_TELEMETRY=1")
	// Every Attempt owns an isolated process group. Pdeathsig is the
	// crash-safety backstop: if the worker agent is SIGKILLed, the
	// native engine receives SIGKILL from the kernel without
	// relying on Go cleanup. The engine's descendants (FFmpeg
	// included) inherit this process group.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return false, 0, 0, stderrBuf, stdoutBuf, telemetry, fmt.Errorf("stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return false, 0, 0, stderrBuf, stdoutBuf, telemetry, fmt.Errorf("stderr pipe: %w", err)
	}

	processStart := time.Now()
	if err := cmd.Start(); err != nil {
		return false, 0, 0, stderrBuf, stdoutBuf, telemetry, fmt.Errorf("start engine: %w", err)
	}
	// The spawn is now an observed fact — record it before any timing.
	engineStarted = true
	processStartMs = time.Since(processStart).Milliseconds()

	// Start the external-process sampler as soon as the engine PID is
	// known. The engine owns its process group (Setpgid below), so the
	// /proc group scan sees the whole ffmpeg/ffprobe/shell/curl tree it
	// spawns. The deferred cleanup stops the monitor on EVERY exit path
	// (success, cancellation, subprocess failure) and collects the final
	// counts into the named return value before this function returns.
	var monitorStop chan struct{}
	var monitorDone chan struct{}
	if cmd.Process != nil {
		monitorStop = make(chan struct{})
		monitorDone = make(chan struct{})
		go func() {
			defer close(monitorDone)
			// With Setpgid the engine's pgrp equals its PID, so the same
			// value is both the group to sample and the PID to exclude.
			telemetry = monitorProcessGroup(cmd.Process.Pid, cmd.Process.Pid, externalProcessSampleInterval, monitorStop)
		}()
		defer func() {
			close(monitorStop)
			<-monitorDone
		}()
	}

	progressDone := streamEngineOutput(stdoutPipe, stderrPipe, ctx, onProgress, legacyProgress, &stderrBuf, &stdoutBuf)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	waitStart := time.Now()
	select {
	case <-ctx.Done():
		if cmd.Process != nil {
			pgid := cmd.Process.Pid
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
				// Reap the process after the hard kill. Without this
				// wait the worker can return while the process group is
				// still winding down, and the native process remains
				// observable as a zombie.
				<-done
			}
		}
		<-progressDone
		return engineStarted, processStartMs, 0, stderrBuf, stdoutBuf, telemetry, ctx.Err()
	case execErr := <-done:
		<-progressDone
		processWaitMs = time.Since(waitStart).Milliseconds()
		return engineStarted, processStartMs, processWaitMs, stderrBuf, stdoutBuf, telemetry, execErr
	}
}
