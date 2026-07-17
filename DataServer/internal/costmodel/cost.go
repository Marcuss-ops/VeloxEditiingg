// Package costmodel derives a per-worker placement decision (eligible +
// lower-is-better score + structured explanation) from a
// WorkerProfile and a JobRequirements.
//
// The model consumes the four canonical fields exposed on
// executor.Descriptor (ResourceClass + TemporalMode +
// Deterministic + Cacheable) plus transient worker state (drain,
// offline, capacity). WorkerProfiles are built from heartbeat
// capabilities maps in worker_profile.go.
//
// Module boundaries: this package is duplicated in
// RemoteCodex/native/worker-agent-go/internal/costmodel. See the
// "Cost model pos: Duplicata in due" choice. Both implementations
// stay in lock-step and are verified against each other by the
// parity test in the worker-side mirror.
package costmodel
