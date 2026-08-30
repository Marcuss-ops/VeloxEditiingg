// Package routing provides canonical types for internal routing metadata
// that flows through the forwarding pipeline (creatorflow → enqueue → worker).
//
// The magic-string payload keys (_internal_forwarding_key, _internal_pipeline_id,
// _internal_executor_id, _internal_executor_version) are replaced by typed
// constants and structs defined here. Every producer and consumer of these
// keys MUST use this package instead of propagating bare strings.
package routing

import (
	"fmt"
	"strings"
)

// ForwardingKey is the canonical key that links a remote creator job to a
// Velox Job. Format: "source_provider:source_job_id:target_executor_id".
type ForwardingKey string

// PipelineID identifies the creator pipeline that generated a job.
type PipelineID string

// ExecutorRef identifies the worker-side executor and its version.
type ExecutorRef struct {
	ID      string
	Version int
}

// InternalRoutingMetadata bundles all routing fields carried through the
// forwarding pipeline. Callers use FromPayload / InjectIntoPayload to
// read and write without touching raw string keys.
type InternalRoutingMetadata struct {
	ForwardingKey ForwardingKey
	PipelineID    PipelineID
	Executor      ExecutorRef
}

// Payload keys — the single source of truth for map[string]interface{}
// access patterns. Every file that previously used bare "_internal_*"
// strings MUST import these constants instead.
const (
	KeyForwardingKey   = "_internal_forwarding_key"
	KeyPipelineID      = "_internal_pipeline_id"
	KeyExecutorID      = "_internal_executor_id"
	KeyExecutorVersion = "_internal_executor_version"
)

// FromPayload extracts InternalRoutingMetadata from a raw payload map.
// Returns zero-value metadata when none of the keys are present.
func FromPayload(m map[string]interface{}) InternalRoutingMetadata {
	if m == nil {
		return InternalRoutingMetadata{}
	}
	var meta InternalRoutingMetadata
	if v, ok := m[KeyForwardingKey].(string); ok {
		meta.ForwardingKey = ForwardingKey(strings.TrimSpace(v))
	}
	if v, ok := m[KeyPipelineID].(string); ok {
		meta.PipelineID = PipelineID(strings.TrimSpace(v))
	}
	// Creator/external payloads use the public wire spelling. Accept it at
	// this boundary and canonicalize it to the internal routing key so the
	// resolver cannot drop an explicitly selected renderer pipeline.
	if meta.PipelineID == "" {
		if v, ok := m["pipeline_id"].(string); ok {
			meta.PipelineID = PipelineID(strings.TrimSpace(v))
		}
	}
	if v, ok := m[KeyExecutorID].(string); ok {
		meta.Executor.ID = strings.TrimSpace(v)
	}
	if meta.Executor.ID == "" {
		if v, ok := m["executor_id"].(string); ok {
			meta.Executor.ID = strings.TrimSpace(v)
		}
	}
	if v, ok := m[KeyExecutorVersion].(float64); ok {
		meta.Executor.Version = int(v)
	} else if v, ok := m[KeyExecutorVersion].(int); ok {
		meta.Executor.Version = v
	}
	return meta
}

// InjectIntoPayload writes all non-zero routing fields into the target map.
func (m InternalRoutingMetadata) InjectIntoPayload(target map[string]interface{}) {
	if target == nil {
		return
	}
	if m.ForwardingKey != "" {
		target[KeyForwardingKey] = string(m.ForwardingKey)
	}
	if m.PipelineID != "" {
		target[KeyPipelineID] = string(m.PipelineID)
	}
	if m.Executor.ID != "" {
		target[KeyExecutorID] = m.Executor.ID
	}
	if m.Executor.Version > 0 {
		target[KeyExecutorVersion] = m.Executor.Version
	}
}

// FormatForwardingKey builds a ForwardingKey from its components.
//
// The components MUST NOT contain a literal `:` (the separator) or `%`
// (the escape character), since producing a working round-trip with
// Parse requires both to be encoded. Callers that pass user-supplied
// strings through (POST /api/v1/creator/jobs, POST /api/v1/jobs)
// MUST validate the input first; see the
// handlers/server/pipeline/idempotency_validation helper which rejects
// `:` and other control characters at the HTTP boundary.
//
// Components containing `:` or `%` ARE encoded here as a defense-in-
// depth measure so historical callers can still emit safe keys without
// doing pre-validation, even though the safer contract is for the
// input layer to reject them outright. Clean ASCII components leave
// the encoded form byte-identical to the pre-encoding string, so
// existing DB rows (forwarding_key column) remain readable.
func FormatForwardingKey(provider, sourceJobID, executorID string) ForwardingKey {
	return ForwardingKey(fmt.Sprintf("%s:%s:%s",
		escapeForwardingKeyComponent(provider),
		escapeForwardingKeyComponent(sourceJobID),
		escapeForwardingKeyComponent(executorID),
	))
}

// ParseForwardingKey splits a ForwardingKey back into its components.
//
// The split honours any `%3A` (encoded `:`) and `%25` (encoded `%`)
// escapes emitted by FormatForwardingKey. A pre-existing ForwardingKey
// produced without escaping (legacy data) is still split cleanly when
// it contains none of `:` or `%`; if it contains an unmangled `:`
// inside what is supposed to be a single component, the result will
// silently be wrong — callers upgrading must run a one-time migration
// to rewrite historical rows. New producers MUST go through
// FormatForwardingKey which encodes consistently.
func (k ForwardingKey) Parse() (provider, sourceJobID, executorID string) {
	parts := strings.SplitN(string(k), ":", 3)
	if len(parts) >= 1 {
		provider = unescapeForwardingKeyComponent(parts[0])
	}
	if len(parts) >= 2 {
		sourceJobID = unescapeForwardingKeyComponent(parts[1])
	}
	if len(parts) >= 3 {
		executorID = unescapeForwardingKeyComponent(parts[2])
	}
	return
}

// escapeForwardingKeyComponent escapes the bytes that would break the
// writer/reader symmetry of ForwardingKey.Parse: a literal `:` would
// shift the column of every component, and a literal `%` would mangle
// any escape sequence that follows. `%` is escaped first so the
// escaped form of `%` itself does not collide with `%3A`.
//
// All other bytes pass through unchanged. The function is the single
// mirror of unescapeForwardingKeyComponent; changing one without the
// other is a writer/reader asymmetry bug.
func escapeForwardingKeyComponent(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, ":", "%3A")
	return s
}

// unescapeForwardingKeyComponent is the inverse of
// escapeForwardingKeyComponent. We unescape `%25` first so that an
// escaped `%` does not collide with the prefix of `%3A` in the second
// pass. A malformed input (a `%` not followed by `25` or `3A`) is
// preserved as-is rather than silently dropped, so future operators
// can spot the bug rather than mis-split a forwarding key invisible.
func unescapeForwardingKeyComponent(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "%25", "%")
	s = strings.ReplaceAll(s, "%3A", ":")
	return s
}

// InjectIntoPayload writes this ForwardingKey into the target map under
// KeyForwardingKey, if and only if k is non-empty.
//
// Round-out of the ForwardingKey method surface: alongside (k).String
// and (k).Parse, (k).InjectIntoPayload is the symmetric write-side
// helper for callers that hold ONLY a ForwardingKey value (rather than
// the broader InternalRoutingMetadata, whose InjectIntoPayload writes
// all non-zero routing fields atomically).
//
// A nil target is a no-op (matching InternalRoutingMetadata.InjectIntoPayload)
// so callers can use the method unconditionally on a freshly-allocated map.
// An empty receiver is likewise a no-op — the caller did not produce a
// forwarding key, so we do not want to write a zero-value entry that would
// later be confused with a real key by routing.FromPayload.
func (k ForwardingKey) InjectIntoPayload(target map[string]interface{}) {
	if target == nil || k == "" {
		return
	}
	target[KeyForwardingKey] = string(k)
}

// String returns the string representation of the ForwardingKey.
func (k ForwardingKey) String() string { return string(k) }

// String returns the string representation of the PipelineID.
func (p PipelineID) String() string { return string(p) }
