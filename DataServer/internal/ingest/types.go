package ingest

import (
	"time"

	"velox-server/internal/renderfingerprint"
	"velox-server/internal/taskattempts"
)

// types.go owns the ingest service's typed input/output surface:
// IngestCommand (the audit-mandated TaskResult identity tuple plus the
// declaration fields), DeclaredArtifact and IngestResult. The service
// itself lives in service.go.

// IngestCommand is the typed input for TaskReportIngestionService.IngestTaskResult.
// Mirrors the audit-mandated TaskResult identity tuple (PR-03) plus the
// declaration fields. Output artifacts are worker-claimed descriptors;
// this service persists them so the artifact upload pipeline can later
// verify the bytes uploaded match these declarations.
type IngestCommand struct {
	TaskID    string
	AttemptID string
	LeaseID   string
	WorkerID  string
	JobID     string // optional but required for the Job roll-up step (4)

	// Executor identity is master-owned. The handler may populate these
	// values from the canonical task row, but canonicalizePhaseTimingIdentity
	// resolves and overwrites them again before persistence.
	ExecutorID      string
	ExecutorVersion int

	// AttemptNumber is the canonical attempt number stamped at Claim time
	// (PR-2 / fix/canonical-attempt-identity). Authoritatively-derived
	// ValidateIdentityTuple strict-compares the wire attempt_number against
	// the canonical task_attempts.attempt_number for the matched tuple.
	AttemptNumber int32

	// Status is "succeeded" or "failed". The handler maps any other value
	// to "failed" defensively.
	Status string

	// Error fields. Populated when Status == "failed"; ignored otherwise.
	ErrorCode    string
	ErrorDetail  string
	FailureClass string
	// RenderFingerprint is supplied by a trusted compiler/worker adapter and
	// persisted atomically with the terminal attempt report.
	RenderFingerprint *renderfingerprint.Fingerprint

	// OutputArtifacts is the worker's map of declared artifacts. Each
	// entry is converted to OutputArtifact via metadata JSON; declared_path
	// and declared_sha256 are worker-supplied hints (NOT authoritative;
	// the artifact upload pipeline's FinalizeVerified recomputes both).
	OutputArtifacts []DeclaredArtifact

	// Scorecard v1 / F1 — typed execution metrics hoisted from the
	// pb.TaskExecutionMetrics wire payload by the gRPC handler via
	// executionMetricsToAttemptMetrics (handler_jobs_metrics.go).
	// Persisted by IngestTaskResult under the per-task mutex immediately
	// after the atomic close-write so the typed metrics commit together
	// with the terminal status flip — guaranteeing serializable scorecard
	// ingest with NO observable window where a task is SUCCEEDED on
	// task_attempts but missing on task_attempt_metrics.
	TypedMetrics taskattempts.AttemptMetrics
	CacheStats   taskattempts.AttemptCacheStats
	CostBasis    taskattempts.AttemptCostBasis

	// Scorecard v2 / Step 8: software versioning from the worker report.
	GitSHA            string
	WorkerVersion     string
	EngineVersion     string
	FFmpegVersion     string
	ConfigHash        string
	DockerImageDigest string
	// Scorecard v2 / Step 15: tracing correlation from gRPC metadata.
	TraceID string
	SpanID  string
	// Step 16: raw worker report payload (JSON) for audit and replay.
	RawReportJSON       string
	RawReportReceivedAt time.Time
	// PerformanceReport metadata supplied by the worker for idempotency
	// and conflict detection in task_attempt_reports.
	ReportSchemaVersion    int32
	ReportVersion          int32
	ReportHash             string
	TelemetrySchemaVersion int32
	// Scorecard v2 / Step 17: per-segment C++ sidecar timings.
	SegmentTimings []taskattempts.SegmentTiming
	// Scorecard v2 / Step 18: partial phase metrics captured when an
	// attempt fails before all phases complete.
	PartialPhaseMetrics []taskattempts.PhaseTimingDetailed
	// PhaseTimings is the complete append-only event timeline. It is
	// persisted atomically with the terminal result; the legacy partial
	// field remains a fallback for older workers. Identity fields inside
	// these entries are always overwritten from master-owned task/attempt
	// rows before persistence.
	PhaseTimings []taskattempts.PhaseTimingDetailed
}

// DeclaredArtifact is one worker-claimed artifact. Mirrors the proto
// TaskResult.OutputArtifacts[].Item Struct shape.
type DeclaredArtifact struct {
	ArtifactID   string
	ArtifactType string
	Path         string // worker-supplied hint; not authoritative
	Size         int64
	SHA256       string // worker-supplied hint; verified by master during upload
	Metadata     map[string]interface{}
}

// IngestResult reports what IngestTaskResult did. Counters let callers
// (handler, observability) emit structured logs without re-querying
// the database.
//
// fix/atomic-ingestion: ArtifactsSkips is always 0 — duplicate detection
// now happens inside IngestTaskResultAtomic's SQL transaction (UNIQUE
// constraint skip), so the ingest service no longer distinguishes new
// vs duplicate declarations.
type IngestResult struct {
	TaskID          string
	AttemptID       string
	JobID           string
	AttemptClosed   bool // true iff the atomic actually flipped an attempt
	ArtifactsNew    int  // number of artifact declarations sent (all registered or skipped as duplicates)
	ArtifactsSkips  int  // always 0 under atomic ingestion; kept for API compatibility
	JobTransitioned bool // true iff Ingest transitioned the Job to AWAITING_ARTIFACT or FAILED
	JobNewStatus    string
}
