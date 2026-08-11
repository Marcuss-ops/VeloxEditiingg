package ffmpegrunner

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ProcessRunner is the canonical exec-based FFmpegRunner. Every field
// is optional and has a safe default; it is safe for concurrent use
// (each Run owns one process).
type ProcessRunner struct {
	// Binary is the ffmpeg executable (default "ffmpeg", resolved via
	// PATH by exec). Tests point it at a stub binary.
	Binary string
	// Stdout / Stderr receive the child's streams (default os.Stdout /
	// os.Stderr).
	Stdout io.Writer
	Stderr io.Writer
	// Env appends extra environment entries to the child (KEY=VALUE).
	Env []string
	// ioPollInterval tunes the /proc/<pid>/io sampling loop; zero uses
	// the default 50ms.
	ioPollInterval time.Duration
}

// NewProcessRunner returns the default runner.
func NewProcessRunner() *ProcessRunner {
	return &ProcessRunner{}
}

func (r *ProcessRunner) binary() string {
	if strings.TrimSpace(r.Binary) == "" {
		return "ffmpeg"
	}
	return r.Binary
}

func (r *ProcessRunner) stdout() io.Writer {
	if r.Stdout != nil {
		return r.Stdout
	}
	return os.Stdout
}

func (r *ProcessRunner) stderr() io.Writer {
	if r.Stderr != nil {
		return r.Stderr
	}
	return os.Stderr
}

func (r *ProcessRunner) ioPoll() time.Duration {
	if r.ioPollInterval > 0 {
		return r.ioPollInterval
	}
	return 50 * time.Millisecond
}

// streamTiming is the phase window observed on ONE output stream: the
// first byte the child wrote and the moment its write side closed (EOF).
type streamTiming struct {
	firstByte time.Time
	eof       time.Time
}

// streamTimeout bounds how long Run waits for a child output stream to
// close after the process exits. A grandchild that outlives the direct
// child (e.g. a shell wrapper that backgrounded a worker) would otherwise
// hold the pipe open forever; the old writer-based path hung on Wait for
// the same reason. We wait the deadline, then proceed without that stream's
// timing (fields stay zero for it).
const streamTimeout = 2 * time.Second

// Run executes one ffmpeg process and returns its profiling result.
// On failure the result still carries the profiling data (exit code,
// CPU, I/O observed so far) alongside the error.
//
// stdout/stderr flow through parent-owned pipes (fd passthrough) so the
// first output byte and pipe EOF are observed directly: no second read of
// the output is ever needed, and the phase trio first_output_ms +
// processing_ms + exit_wait_ms decomposes process_wall_ms without
// post-hoc file I/O.
func (r *ProcessRunner) Run(ctx context.Context, req FFmpegRequest) (FFmpegResult, error) {
	result := FFmpegResult{
		Operation:          req.Operation,
		CommandFingerprint: Fingerprint(req),
		Parameters:         Sanitize(req),
		ExitCode:           -1, // -1 marks "not started / start failure"
	}
	cmd := exec.CommandContext(ctx, r.binary(), req.Args...)
	if len(r.Env) > 0 {
		cmd.Env = mergeEnv(os.Environ(), r.Env)
	}
	// Run the child in its own process group and cancel by killing the WHOLE
	// group: ffmpeg helpers (shell wrappers, decoders, spawned filters) must
	// not outlive their parent on cancellation. Killing only the direct child
	// would orphan grandchildren holding our stdout/stderr pipes, hanging the
	// caller on Wait.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	// Parent-owned pipes for stdout/stderr. cmd gets the *os.File write
	// ends (fd passthrough — no hidden exec copy goroutines); our readers
	// tee the streams into the configured writers while recording the
	// first-byte and EOF moments.
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return result, fmt.Errorf("ffmpeg stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		stdoutR.Close()
		stdoutW.Close()
		return result, fmt.Errorf("ffmpeg stderr pipe: %w", err)
	}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	streams := make(chan streamTiming, 2)
	tee := func(reader *os.File, dst io.Writer) {
		timing := streamTiming{}
		var buf [32 * 1024]byte
		forward := true
		for {
			n, readErr := reader.Read(buf[:])
			if n > 0 {
				if timing.firstByte.IsZero() {
					timing.firstByte = time.Now()
				}
				if forward {
					if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
						// Keep draining but stop forwarding: a broken sink must
						// not leave the child blocked on a full pipe, which
						// would hang cmd.Wait() forever.
						forward = false
					}
				}
			}
			if readErr != nil {
				break // EOF or stream error: the child closed this stream
			}
		}
		timing.eof = time.Now()
		streams <- timing
	}
	go tee(stdoutR, r.stdout())
	go tee(stderrR, r.stderr())

	spawnStart := time.Now()
	if err := cmd.Start(); err != nil {
		// Close every end so the tee goroutines exit promptly.
		stdoutW.Close()
		stderrW.Close()
		stdoutR.Close()
		stderrR.Close()
		return result, fmt.Errorf("ffmpeg start: %w", err)
	}
	result.ProcessSpawnMS = time.Since(spawnStart).Milliseconds()
	// Reference for first_output_ms is the instant BEFORE Start(): a fast
	// child can write its first byte before Start() returns, so any
	// reference taken after Start() would make first_output_ms negative
	// (truncated to 0). The tiny overlap with spawn_ms (the Start() call
	// itself, ~tens of µs) is part of setup and acceptable.
	startTime := spawnStart
	// Close our write ends NOW: the tees then observe EOF at the exact
	// moment the child exits (its fd copies close with the process).
	stdoutW.Close()
	stderrW.Close()

	// Best-effort storage-layer I/O sampling: /proc/<pid>/io is only
	// readable while the process exists, so a goroutine keeps the last
	// cumulative sample until Wait reaps the child. The goroutine exits
	// within one poll tick after close(stop) — it never leaks, but it may
	// briefly outlive Run; the final burst of I/O right before exit is
	// intentionally best-effort.
	var ioMu sync.Mutex
	var lastRead, lastWrite int64
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(r.ioPoll())
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				read, written, ok := readProcIO(cmd.Process.Pid)
				if !ok {
					continue
				}
				ioMu.Lock()
				lastRead, lastWrite = read, written
				ioMu.Unlock()
			}
		}
	}()

	wallStart := time.Now()
	waitErr := cmd.Wait()
	close(stop)
	waitEnd := time.Now()
	result.ProcessWallMS = waitEnd.Sub(wallStart).Milliseconds()
	ioMu.Lock()
	result.ReadBytes, result.WriteBytes = lastRead, lastWrite
	ioMu.Unlock()

	// Collect the stream windows. Both tees finish right after the child
	// exits (EOF); the bounded wait only matters for pathological
	// grandchildren that hold a pipe open — when it fires, the affected
	// timing is zeroed and StreamTimedOut is set so consumers do not read
	// the zeros as "the process produced no output".
	outTiming, outTimedOut := waitStream(streams)
	errTiming, errTimedOut := waitStream(streams)
	result.StreamTimedOut = outTimedOut || errTimedOut
	// First output byte = the EARLIEST across the two streams; EOF = the
	// LATEST (the work window ends when the last stream closes).
	firstByte := outTiming.firstByte
	if firstByte.IsZero() || (!errTiming.firstByte.IsZero() && errTiming.firstByte.Before(firstByte)) {
		firstByte = errTiming.firstByte
	}
	eof := outTiming.eof
	if errTiming.eof.After(eof) {
		eof = errTiming.eof
	}
	if !firstByte.IsZero() {
		result.FirstOutputMS = firstByte.Sub(startTime).Milliseconds()
		if !eof.IsZero() {
			result.ProcessingMS = eof.Sub(firstByte).Milliseconds()
		}
	}
	if !eof.IsZero() {
		result.ExitWaitMS = waitEnd.Sub(eof).Milliseconds()
		if result.ExitWaitMS < 0 {
			result.ExitWaitMS = 0 // EOF observed after Wait returned (scheduling)
		}
	}

	if ru, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		result.UserCPUMs = timevalToMS(&ru.Utime)
		result.SystemCPUMs = timevalToMS(&ru.Stime)
		// Linux ru_maxrss is in kilobytes; convert to bytes.
		if ru.Maxrss > 0 {
			result.PeakRSSBytes = ru.Maxrss * 1024
		}
	}
	result.ExitCode = cmd.ProcessState.ExitCode()
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
		result.TerminatedBySignal = ws.Signaled()
	}

	if waitErr != nil {
		return result, fmt.Errorf("ffmpeg run: %w", waitErr)
	}
	return result, nil
}

// waitStream returns the next stream window, bounded by streamTimeout.
// A stream that never closes yields a zero window (its phase fields stay
// zero) instead of hanging Run forever; the second return reports whether
// the bound was hit so callers can mark the result with StreamTimedOut.
func waitStream(ch <-chan streamTiming) (streamTiming, bool) {
	select {
	case timing := <-ch:
		return timing, false
	case <-time.After(streamTimeout):
		return streamTiming{}, true
	}
}

func timevalToMS(tv *syscall.Timeval) int64 {
	if tv == nil {
		return 0
	}
	return tv.Sec*1000 + tv.Usec/1000
}

// mergeEnv returns the parent environment with r.Env applied. Entries are
// deduplicated by key so an override actually wins: glibc getenv() scans
// the environ array from the start and returns the FIRST match, so a naive
// append would let the inherited value shadow the requested override.
func mergeEnv(parent, override []string) []string {
	if len(override) == 0 {
		return parent
	}
	keys := make(map[string]struct{}, len(override))
	for _, entry := range override {
		if eq := strings.IndexByte(entry, '='); eq > 0 {
			keys[entry[:eq]] = struct{}{}
		}
	}
	out := make([]string, 0, len(parent)+len(override))
	for _, entry := range parent {
		eq := strings.IndexByte(entry, '=')
		if eq > 0 {
			if _, shadowed := keys[entry[:eq]]; shadowed {
				continue
			}
		}
		out = append(out, entry)
	}
	return append(out, override...)
}

// readProcIO reads the storage-layer I/O counters of a live process
// from /proc/<pid>/io (Linux). Returns ok=false when the entry is
// unavailable (non-Linux, process already reaped, permission).
func readProcIO(pid int) (readBytes, writeBytes int64, ok bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/io")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "read_bytes:"):
			if value, perr := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "read_bytes:")), 10, 64); perr == nil {
				readBytes = value
			}
		case strings.HasPrefix(line, "write_bytes:"):
			if value, perr := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:")), 10, 64); perr == nil {
				writeBytes = value
			}
		}
	}
	return readBytes, writeBytes, true
}
