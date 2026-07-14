// Package taskattempts — canonical error codes for structured error
// classification (Scorecard v2 / Step 13).
//
// Workers emit these canonical codes in their TaskResult error_code
// field. The master uses them as the `reason` label on the
// velox_error_classification_total Prometheus counter and as the
// grouping key for SQL failure-reason dashboards.
//
// Codes are a CLOSED enum — free-form strings are rejected at the
// worker boundary and MUST NOT land in dashboards or alerts (same
// discipline as compute-outcome labels).
package taskattempts

// CanonicalErrorCode is a closed-enum error code emitted by workers.
type CanonicalErrorCode string

const (
	// Asset / download failures.
	ErrAssetDownloadFailed CanonicalErrorCode = "ASSET_DOWNLOAD_FAILED"
	ErrAssetCacheCorrupt   CanonicalErrorCode = "ASSET_CACHE_CORRUPT"

	// FFmpeg / encoding failures.
	ErrFFmpegExitNonZero CanonicalErrorCode = "FFMPEG_EXIT_NONZERO"
	ErrFFmpegTimeout     CanonicalErrorCode = "FFMPEG_TIMEOUT"
	ErrFFmpegOOM         CanonicalErrorCode = "FFMPEG_OOM"

	// Output failures.
	ErrOutputMissing    CanonicalErrorCode = "OUTPUT_MISSING"
	ErrOutputCorrupt    CanonicalErrorCode = "OUTPUT_CORRUPT"
	ErrOutputValidation CanonicalErrorCode = "OUTPUT_VALIDATION_FAILED"

	// Resource exhaustion.
	ErrInsufficientDisk         CanonicalErrorCode = "INSUFFICIENT_DISK"
	ErrOutOfMemory              CanonicalErrorCode = "OUT_OF_MEMORY"
	ErrFileDescriptorsExhausted CanonicalErrorCode = "FILE_DESCRIPTORS_EXHAUSTED"

	// Pipeline / plan failures.
	ErrInvalidRenderPlan CanonicalErrorCode = "INVALID_RENDER_PLAN"
	ErrInvalidSceneSpec  CanonicalErrorCode = "INVALID_SCENE_SPEC"
	ErrUnsupportedCodec  CanonicalErrorCode = "UNSUPPORTED_CODEC"

	// Upload / network failures.
	ErrUploadFailed       CanonicalErrorCode = "UPLOAD_FAILED"
	ErrUploadTimeout      CanonicalErrorCode = "UPLOAD_TIMEOUT"
	ErrNetworkUnreachable CanonicalErrorCode = "NETWORK_UNREACHABLE"

	// Worker / master lifecycle.
	ErrWorkerLost    CanonicalErrorCode = "WORKER_LOST"
	ErrLeaseExpired  CanonicalErrorCode = "LEASE_EXPIRED"
	ErrTaskTimeout   CanonicalErrorCode = "TASK_TIMEOUT"
	ErrTaskCancelled CanonicalErrorCode = "TASK_CANCELLED"

	// Unknown / catch-all.
	ErrUnknown CanonicalErrorCode = "UNKNOWN"
)

// CanonicalErrorComponents are the well-known component names
// emitted alongside error codes for component-level grouping.
var CanonicalErrorComponents = []string{
	"asset_download",
	"ffmpeg",
	"pipeline",
	"upload",
	"worker",
	"master",
	"unknown",
}

// CanonicalErrorPhases are well-known phase names emitted when
// an error is attributable to a specific rendering phase.
var CanonicalErrorPhases = []string{
	"cache_lookup",
	"download",
	"decode",
	"compile",
	"render",
	"concat",
	"mux_audio",
	"encode",
	"upload",
	"finalize",
	"unknown",
}
