package native

import (
	"context"
	"fmt"
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
// returns (processStartMs, processWaitMs, stderr, stdout, err).
//
// err semantics:
//   - ctx.Err() (context.Canceled or DeadlineExceeded) when the caller
//     cancelled via the context — metrics fields below are populated
//     up through ProcessStartMs; ProcessWaitMs is zero
//   - raw exec.ExitError-equivalent from cmd.Wait() when the engine
//     failed — ProcessStartMs AND ProcessWaitMs are populated; the
//     caller is responsible for wrapping the error with stderr/stdout
//
// SAFETY-CRITICAL: Setpgid + Pdeathsig + 10s SIGTERM grace + SIGKILL
// hard-kill + <-done reaping are preserved verbatim from the original
// render_client.go. Do not modify these.
func runEngineProcess(ctx context.Context, binaryPath, planPath string, onProgress ProgressFunc) (processStartMs int64, processWaitMs int64, stderrBuf strings.Builder, stdoutBuf strings.Builder, err error) {
	cmd := exec.Command(binaryPath, "--render", "--plan", planPath)
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
		return 0, 0, stderrBuf, stdoutBuf, fmt.Errorf("stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return 0, 0, stderrBuf, stdoutBuf, fmt.Errorf("stderr pipe: %w", err)
	}

	processStart := time.Now()
	if err := cmd.Start(); err != nil {
		return 0, 0, stderrBuf, stdoutBuf, fmt.Errorf("start engine: %w", err)
	}
	processStartMs = time.Since(processStart).Milliseconds()

	progressDone := streamEngineOutput(stdoutPipe, stderrPipe, ctx, onProgress, &stderrBuf, &stdoutBuf)

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
		return processStartMs, 0, stderrBuf, stdoutBuf, ctx.Err()
	case execErr := <-done:
		<-progressDone
		processWaitMs = time.Since(waitStart).Milliseconds()
		return processStartMs, processWaitMs, stderrBuf, stdoutBuf, execErr
	}
}
