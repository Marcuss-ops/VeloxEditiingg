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
type FFmpegResult struct {
	ProcessSpawnMS     int64               `json:"process_spawn_ms"`
	ProcessWallMS      int64               `json:"process_wall_ms"`
	UserCPUMs          int64               `json:"user_cpu_ms"`
	SystemCPUMs        int64               `json:"system_cpu_ms"`
	PeakRSSBytes       int64               `json:"peak_rss_bytes"`
	ReadBytes          int64               `json:"read_bytes"`
	WriteBytes         int64               `json:"write_bytes"`
	ExitCode           int                 `json:"exit_code"`
	TerminatedBySignal bool                `json:"terminated_by_signal"`
	CommandFingerprint string              `json:"command_fingerprint"`
	Parameters         SanitizedParameters `json:"parameters"`
}

// FFmpegRunner executes one ffmpeg process and returns its profiling
// result. Implementations must be safe for concurrent use and must
// never expose the raw argument vector through FFmpegResult.
type FFmpegRunner interface {
	Run(ctx context.Context, req FFmpegRequest) (FFmpegResult, error)
}
