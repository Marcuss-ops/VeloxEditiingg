package executor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"velox-shared/taskcontract"
)

// ── Task description (worker-side import of shared TaskSpec) ────────────
//
// TaskSpec is re-exported from velox-shared/taskcontract so master and
// worker share a single definition. executor.TaskSpec is a type alias —
// existing call-sites remain unchanged.

// TaskSpec is the canonical task specification, re-exported from the
// shared contract package.
type TaskSpec = taskcontract.TaskSpec

// ── Enums ────────────────────────────────────────────────────────────────────

// ResourceClass identifies the dominant kind of resource a task type uses.
// Drives worker-side capability matching (PR-3.5 / PR-3.8).
type ResourceClass string

const (
	// ResourceCPU: CPU-bound; no dedicated GPU required.
	ResourceCPU ResourceClass = "cpu"
	// ResourceGPU: requires a GPU; CPU-only workers reject.
	ResourceGPU ResourceClass = "gpu"
	// ResourceMixed: benefits from a GPU but tolerates CPU fallback when
	// the GPU is busy or absent.
	ResourceMixed ResourceClass = "mixed"
	// ResourceIO: dominated by network or disk; small CPU footprint.
	ResourceIO ResourceClass = "io"
)

// Valid returns true iff r is one of the canonical initial set.
func (r ResourceClass) Valid() bool {
	switch r {
	case ResourceCPU, ResourceGPU, ResourceMixed, ResourceIO:
		return true
	}
	return false
}

// TemporalMode describes how an executor moves through time.
type TemporalMode string

const (
	// TemporalFrameLocal: each frame is independent (no temporal context).
	TemporalFrameLocal TemporalMode = "frame_local"
	// TemporalWindowed: short-range context only (audio mix, recent frames).
	TemporalWindowed TemporalMode = "windowed"
	// TemporalStateful: long-range state (overlays, watermark).
	TemporalStateful TemporalMode = "stateful"
	// TemporalGlobal: full-document (cacheable, deterministic).
	TemporalGlobal TemporalMode = "global"
)

// Valid returns true iff t is one of the canonical initial set.
func (t TemporalMode) Valid() bool {
	switch t {
	case TemporalFrameLocal, TemporalWindowed, TemporalStateful, TemporalGlobal:
		return true
	}
	return false
}

// ── Descriptor ───────────────────────────────────────────────────────────────

// Descriptor is the static, immutable description of one executor.
// Registered in bootstrap; PR-3.5 derives worker hello from this.
type Descriptor struct {
	ID            string        `json:"id"`
	Version       int           `json:"version"`
	InputTypes    []string      `json:"input_types,omitempty"`
	OutputTypes   []string      `json:"output_types,omitempty"`
	ResourceClass ResourceClass `json:"resource_class"`
	Deterministic bool          `json:"deterministic"`
	Cacheable     bool          `json:"cacheable"`
	TemporalMode  TemporalMode  `json:"temporal_mode"`
	SupportsAlpha bool          `json:"supports_alpha"`
}

// Validate ensures the descriptor is internally consistent and suitable
// for registration. Errors wrap ErrInvalidDescriptor so callers can
// match with errors.Is.
func (d *Descriptor) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: Descriptor is nil", ErrInvalidDescriptor)
	}
	if strings.TrimSpace(d.ID) == "" {
		return fmt.Errorf("%w: Descriptor.ID is required", ErrInvalidDescriptor)
	}
	if strings.ContainsRune(d.ID, '@') {
		return fmt.Errorf("%w: Descriptor.ID %q must not contain '@'", ErrInvalidDescriptor, d.ID)
	}
	if d.Version <= 0 {
		return fmt.Errorf("%w: Descriptor %q has non-positive version %d", ErrInvalidDescriptor, d.ID, d.Version)
	}
	if !d.ResourceClass.Valid() {
		return fmt.Errorf("%w: Descriptor %q has unknown ResourceClass %q", ErrInvalidDescriptor, d.ID, d.ResourceClass)
	}
	if !d.TemporalMode.Valid() {
		return fmt.Errorf("%w: Descriptor %q has unknown TemporalMode %q", ErrInvalidDescriptor, d.ID, d.TemporalMode)
	}
	return nil
}

// Key returns the canonical (id, version) tuple as a string. The "@"
// separator is reserved; Descriptor.Validate rejects "@" in ID.
func (d Descriptor) Key() string {
	return fmt.Sprintf("%s@%d", d.ID, d.Version)
}

// ── ExecutionContext sub-interfaces (concrete impl is PR-3.2) ────────────────
//
// ExecutionContext is built PER task in worker-agent-go/internal/taskrunner.
// These sub-interfaces exist so executors don't take a hidden dependency on
// any concrete backend (OTel vs log-based, S3 vs GCS, content-addressed vs
// flat). Each method here is a SHAPE only; implementations land in PR-3.2.

// ArtifactAccess is the canonical reader/writer for blob artifacts published
// by a worker. Hashes are content-addressed.
type ArtifactAccess interface {
	Get(ctx context.Context, hash string) ([]byte, error)
	Put(ctx context.Context, hash string, data []byte) error
}

// LocalCache is the persistent, content-addressed local cache (PR-3.7).
// Get returns (data, found, err); hash verification is the caller's job.
type LocalCache interface {
	Get(ctx context.Context, hash string) (data []byte, found bool, err error)
	Put(ctx context.Context, hash string, data []byte) error
}

// Telemetry is a scoped span recorder. PR-3.2 supplies the OTel-backed
// default; tests can pass a noop.
type Telemetry interface {
	Record(name string, fields map[string]interface{}) error
}

// ResourceLimits reflects the worker's per-task envelope. Sampled by
// the resource sampler (PR-3.6); consumed here so executors can fail
// fast when over budget.
type ResourceLimits interface {
	CPU() int
	MemoryMB() int64
	DiskFreeGB() int64
	MaxConcurrent() int
}

// Clock abstracts time so executors can be tested deterministically.
type Clock interface {
	Now() time.Time
}

// Logger is a scoped logger; the worker agent supplies one with the
// executor ID already attached as a field.
type Logger interface {
	Info(msg string, fields map[string]interface{})
	Warn(msg string, fields map[string]interface{})
	Error(msg string, err error, fields map[string]interface{})
}

// ExecutionContext is the bounded, owned environment handed to each
// Executor.Execute call. It is rebuilt per task — no global mutable
// state leaks across calls (PR-3 invariant #7: every reusable
// resource must enter a canonical registry).
//
// DONE/ERR together signal cancellation (lease loss, ctx cancel,
// hard timeout). Executors MUST check Done() in their inner loops.
type ExecutionContext interface {
	Artifacts() ArtifactAccess
	LocalCache() LocalCache
	Telemetry() Telemetry
	Resources() ResourceLimits
	Clock() Clock
	Logger() Logger
	Done() <-chan struct{}
	Err() error
}

// ── Result ────────────────────────────────────────────────────────────────────

// ExecutionResult is the structured outcome of one Execute call.
// The taskrunner converts this into the canonical TaskExecutionReport
// reported back to the master (PR-1 metrics catalogue).
type ExecutionResult struct {
	// Status is exactly "succeeded" or "failed". Anything else is treated
	// as a malformed result by the taskrunner.
	Status string `json:"status"`
	// Outputs lists the canonical artifact references produced by this task.
	Outputs []ArtifactRef `json:"outputs,omitempty"`
	// Metrics holds executor-defined measurements.
	Metrics map[string]interface{} `json:"metrics,omitempty"`
	// ErrorCode is a stable error code when Status == "failed"
	// (e.g. "validation_failed", "executor_panic_contained", "lease_lost").
	ErrorCode string `json:"error_code,omitempty"`
	// ErrorDetail is a human-readable detail when Status == "failed".
	ErrorDetail string `json:"error_detail,omitempty"`
	// StartedAt / CompletedAt bracket the actual execution work.
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

// ArtifactRef is the canonical pointer to one published artifact.
// fix/artifact-metadata: ArtifactID is now separate from Hash (sha256).
// SizeBytes carries the real byte count of the produced file.
// Hash is mandatory for succeeded tasks — validated by dispatchTaskRunner.
type ArtifactRef struct {
	Type       string `json:"type"`
	Hash       string `json:"hash"`
	ArtifactID string `json:"artifact_id,omitempty"`
	URI        string `json:"uri,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
}

// ── Executor ──────────────────────────────────────────────────────────────────

// Executor is the canonical contract every executable task type
// implements in the worker agent. Implementations register in worker
// bootstrap; the taskrunner (PR-3.3) resolves and runs them.
type Executor interface {
	// Descriptor returns the static, immutable description. Must be
	// idempotent and safe to call after Register.
	Descriptor() Descriptor
	// Validate runs task-type-specific pre-flight checks. Called by the
	// taskrunner BEFORE resource acquisition (PR-3.3 invariant:
	// validate first, acquire later).
	Validate(spec TaskSpec) error
	// Execute performs the canonical work. Must respect cancellation
	// (execCtx.Done()) and never mutate job/task lifecycle directly
	// (PR-3 invariant: task lifecycle is master-owned).
	Execute(ctx context.Context, execCtx ExecutionContext, spec TaskSpec) (ExecutionResult, error)
}
