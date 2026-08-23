// Package taskcontract provides the canonical TaskSpec type shared between
// master (velox-server) and worker (velox-worker-agent). A single source of
// truth eliminates the duplicate struct definitions and divergent validation
// that existed when each side maintained its own copy.
//
// All task-spec identity fields (Version, JobID, ExecutorID, Payload),
// validation rules, and hashing live here. Both sides import this package
// and re-export via a type alias so existing call-sites remain unchanged.
package taskcontract

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"velox-shared/contract/assembly"
)

// SpecVersion is the current canonical task spec version.
const SpecVersion = 1

// ErrInvalidSpec is the sentinel returned by Validate when a TaskSpec
// is structurally incorrect.
var ErrInvalidSpec = fmt.Errorf("taskcontract: invalid spec")

// TaskSpec is the typed, validated, versioned representation of the work
// a task must perform. It is serialized for transport and storage but
// must be validated before persistence. A deterministic spec_hash is
// stored alongside for integrity verification.
type TaskSpec struct {
	Version    int                    `json:"version"`
	JobID      string                 `json:"job_id"`
	ExecutorID string                 `json:"executor_id"`
	Payload    map[string]interface{} `json:"payload,omitempty"`
	// DeliveryPlan is control-plane routing data. It is persisted into
	// job_delivery_plans and must never be sent to the renderer in Payload.
	// The map retains the canonical delivery_plan/legacy alias envelope so
	// the shared parser remains the single shape owner.
	DeliveryPlan map[string]interface{} `json:"delivery_plan,omitempty"`
	// PublicationSpecs are control-plane publication intents. They are
	// retained with the canonical spec but are never included in Payload.
	PublicationSpecs     []map[string]interface{} `json:"publication_specs,omitempty"`
	RequiredCapabilities []string                 `json:"required_capabilities,omitempty"`
	// Assembly is control-plane preparation metadata. It is persisted with
	// the task spec but is never included in Payload sent to the renderer.
	Assembly *assembly.AssemblyJobV1 `json:"assembly,omitempty"`
}

// Validate checks the task spec for structural correctness.
// Both master and worker MUST pass this check before persisting or
// executing a spec. Executor implementations may add stricter checks
// via their own Validate method.
func (s *TaskSpec) Validate() error {
	if s == nil {
		return fmt.Errorf("%w: TaskSpec is nil", ErrInvalidSpec)
	}
	if s.Version <= 0 {
		return fmt.Errorf("%w: version must be > 0 (got %d)", ErrInvalidSpec, s.Version)
	}
	if s.JobID == "" {
		return fmt.Errorf("%w: job_id is required", ErrInvalidSpec)
	}
	if s.ExecutorID == "" {
		return fmt.Errorf("%w: executor_id is required", ErrInvalidSpec)
	}
	if s.Assembly != nil {
		if err := s.Assembly.Validate(); err != nil {
			return fmt.Errorf("%w: assembly: %v", ErrInvalidSpec, err)
		}
	}
	return nil
}

// SpecHash returns a deterministic SHA-256 hex digest of the canonical
// JSON serialization of the spec. Two specs with identical content produce
// the same hash regardless of field ordering (encoding/json sorts map keys).
func (s *TaskSpec) SpecHash() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("spec hash marshal: %w", err)
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum), nil
}

// MustSpecHash is like SpecHash but panics on error. Safe for use after
// successful Validate.
func (s *TaskSpec) MustSpecHash() string {
	h, err := s.SpecHash()
	if err != nil {
		panic("taskcontract: MustSpecHash called on invalid spec: " + err.Error())
	}
	return h
}

// RenderPlanID extracts a render plan ID from the spec payload.
// Returns empty string if not present.
func (s *TaskSpec) RenderPlanID() string {
	if s == nil || s.Payload == nil {
		return ""
	}
	if v, ok := s.Payload["render_plan_id"].(string); ok {
		return v
	}
	return ""
}
