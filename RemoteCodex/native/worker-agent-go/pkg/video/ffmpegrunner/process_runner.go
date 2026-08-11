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

// Run executes one ffmpeg process and returns its profiling result.
// On failure the result still carries the profiling data (exit code,
// CPU, I/O observed so far) alongside the error.
func (r *ProcessRunner) Run(ctx context.Context, req FFmpegRequest) (FFmpegResult, error) {
	result := FFmpegResult{
		CommandFingerprint: Fingerprint(req),
		Parameters:         Sanitize(req),
		ExitCode:           -1, // -1 marks "not started / start failure"
	}
	cmd := exec.CommandContext(ctx, r.binary(), req.Args...)
	cmd.Stdout = r.stdout()
	cmd.Stderr = r.stderr()
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

	spawnStart := time.Now()
	if err := cmd.Start(); err != nil {
		return result, fmt.Errorf("ffmpeg start: %w", err)
	}
	result.ProcessSpawnMS = time.Since(spawnStart).Milliseconds()

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
	result.ProcessWallMS = time.Since(wallStart).Milliseconds()
	ioMu.Lock()
	result.ReadBytes, result.WriteBytes = lastRead, lastWrite
	ioMu.Unlock()

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
