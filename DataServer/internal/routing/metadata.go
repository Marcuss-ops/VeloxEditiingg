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
	KeyForwardingKey     = "_internal_forwarding_key"
	KeyPipelineID        = "_internal_pipeline_id"
	KeyExecutorID        = "_internal_executor_id"
	KeyExecutorVersion   = "_internal_executor_version"
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
	if v, ok := m[KeyExecutorID].(string); ok {
		meta.Executor.ID = strings.TrimSpace(v)
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
func FormatForwardingKey(provider, sourceJobID, executorID string) ForwardingKey {
	return ForwardingKey(fmt.Sprintf("%s:%s:%s", provider, sourceJobID, executorID))
}

// ParseForwardingKey splits a ForwardingKey back into its components.
func (k ForwardingKey) Parse() (provider, sourceJobID, executorID string) {
	parts := strings.SplitN(string(k), ":", 3)
	if len(parts) >= 1 {
		provider = parts[0]
	}
	if len(parts) >= 2 {
		sourceJobID = parts[1]
	}
	if len(parts) >= 3 {
		executorID = parts[2]
	}
	return
}

// String returns the string representation of the ForwardingKey.
func (k ForwardingKey) String() string { return string(k) }

// String returns the string representation of the PipelineID.
func (p PipelineID) String() string { return string(p) }
