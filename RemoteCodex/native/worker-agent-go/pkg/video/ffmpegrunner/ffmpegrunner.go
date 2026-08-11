// Package ffmpegrunner is the canonical FFmpeg process runner with
// centralized profiling (Phase B1).
//
// Every Go-side FFmpeg render operation — audio mix, segment compose,
// final encode/mux — MUST execute through the single FFmpegRunner
// interface instead of spawning `exec.Command` at each call site. The
// runner measures, exactly once per process, the phase-B profiling
// contract:
//
//	process_spawn_ms    time from Run() entry to successful Start()
//	first_output_ms     Start() → first byte on stdout/stderr
//	processing_ms       first output → last byte drained (work window)
//	exit_wait_ms         last byte → Wait() returning (reap/shutdown)
//	process_wall_ms     wall time of the executed process
//	user_cpu_ms         child user CPU (getrusage RUSAGE_CHILDREN delta)
//	system_cpu_ms       child system CPU
//	peak_rss_bytes      child peak resident set (ProcessState rusage)
//	read_bytes          storage-layer read bytes (/proc/<pid>/io, best-effort)
//	write_bytes         storage-layer write bytes (/proc/<pid>/io, best-effort)
//	exit_code           process exit code
//	terminated_by_signal whether the process died from a signal
//	command_fingerprint SHA-256 over the canonical argument vector
//	parameters          SANITIZED parameters (codecs, filter names,
//	                    input count) — NEVER raw paths, tokens, or secrets
//
// FFmpegResult is the only surface callers may consume. The raw
// argument vector is deliberately NOT part of the result: paths and
// tokens never leave this package in result form.
package ffmpegrunner

import "context"

// OperationType classifies the ffmpeg phase so the sanitized profile
// and fingerprints are comparable across attempts of the same phase.
type OperationType string

const (
	// OperationAudioMix is the voiceover/music/sfx mixing pass
	// (AudioRecorder surface).
	OperationAudioMix OperationType = "audio_mix"
	// OperationCompose is the per-segment video composition pass
	// (SegmentRecorder surface).
	OperationCompose OperationType = "compose"
	// OperationEncode is the final mux/encode pass (MuxRecorder
	// surface).
	OperationEncode OperationType = "encode"
)

// SanitizedParameters is the safe, comparable projection of an ffmpeg
// invocation. It contains no paths, URLs, tokens, or raw argument
// strings.
type SanitizedParameters struct {
	// Codecs are the codec names declared via -c / -c:a / -c:v
	// (e.g. libx264, aac, pcm_s16le), deduplicated and sorted.
	Codecs []string `json:"codecs,omitempty"`
	// Filters are the filter names found in -filter_complex / -vf /
	// -af (e.g. amix, volume, concat, setpts, scale, pad, ass),
	// deduplicated and sorted. Path-like values (ass=/path) yield
	// only the filter name.
	Filters []string `json:"filters,omitempty"`
	// InputCount is the number of -i inputs (never their paths).
	InputCount int `json:"input_count"`
}

// FFmpegRequest is one ffmpeg invocation. Args is the argument vector
// AFTER the binary name (identical to exec.Command(binary, args...)).
type FFmpegRequest struct {
	Operation OperationType
	Args      []string
}

// FFmpegResult is the complete profiling contract of one ffmpeg
// process. All numeric fields are best-effort where the platform does
// not expose them (they stay zero); the fingerprint and sanitized
// parameters are always populated.
//
// The wall clock of one process decomposes as:
//
//	spawn_ms            Run() entry → Start() success
//	first_output_ms     Start() → first byte on stdout/stderr
//	                    (0 when the process produced no output)
//	processing_ms       first output → last byte drained (the actual
//	                    work window; 0 when no output was observed)
//	exit_wait_ms        last byte → Wait() returning (reap/shutdown;
//	                    clamped at 0)
//
// process_wall_ms is the time Wait() blocked (spawn-to-exit minus the
// sub-millisecond setup between Start and Wait), so
// first_output_ms + processing_ms + exit_wait_ms ≈ process_wall_ms for
// processes that produced output. For silent invocations only spawn_ms
// and process_wall_ms are meaningful and the phase trio stays zero.
type FFmpegResult struct {
	// Operation is the phase this process belonged to (audio_mix /
	// compose / encode). The runner stamps it from the request so results
	// are self-describing and aggregation can group by phase.
	Operation          OperationType `json:"operation"`
	ProcessSpawnMS     int64         `json:"process_spawn_ms"`
	FirstOutputMS      int64         `json:"first_output_ms"`
	ProcessingMS       int64         `json:"processing_ms"`
	ExitWaitMS         int64         `json:"exit_wait_ms"`
	ProcessWallMS      int64         `json:"process_wall_ms"`
	UserCPUMs          int64         `json:"user_cpu_ms"`
	SystemCPUMs        int64         `json:"system_cpu_ms"`
	PeakRSSBytes       int64         `json:"peak_rss_bytes"`
	ReadBytes          int64         `json:"read_bytes"`
	WriteBytes         int64         `json:"write_bytes"`
	ExitCode           int           `json:"exit_code"`
	TerminatedBySignal bool          `json:"terminated_by_signal"`
	// StreamTimedOut is true when an output stream did not close within
	// streamTimeout after the process exited (a grandchild held the pipe
	// open). In that case the phase trio is not zero because the process
	// was silent, but because the window could not be observed — consumers
	// must not read the zeros as "no output produced".
	StreamTimedOut     bool                `json:"stream_timed_out,omitempty"`
	CommandFingerprint string              `json:"command_fingerprint"`
	Parameters         SanitizedParameters `json:"parameters"`
}

// FFmpegRunner executes one ffmpeg process and returns its profiling
// result. Implementations must be safe for concurrent use and must
// never expose the raw argument vector through FFmpegResult.
type FFmpegRunner interface {
	Run(ctx context.Context, req FFmpegRequest) (FFmpegResult, error)
}
