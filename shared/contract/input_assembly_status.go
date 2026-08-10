package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// InputAssemblyStatus describes the producer-side handoff state of a payload.
// It must not be confused with JobStatus: InputAssemblyCompleted means the
// request envelope is fully assembled, not that rendering or delivery is done.
//
// The type owns the legacy wire spelling at this boundary: pending was
// historically emitted as uppercase PENDING, while completed and failure
// values remain lowercase. This keeps old readers compatible without making
// a generic string status the in-memory representation.
type InputAssemblyStatus string

const (
	InputAssemblyPending               InputAssemblyStatus = "pending"
	InputAssemblyCompleted             InputAssemblyStatus = "completed"
	InputAssemblyCompletedWithWarnings InputAssemblyStatus = "completed_with_warnings"
	InputAssemblyFailed                InputAssemblyStatus = "failed"
)

// Valid reports whether s is a known input-assembly status.
func (s InputAssemblyStatus) Valid() bool {
	switch s {
	case InputAssemblyPending, InputAssemblyCompleted, InputAssemblyCompletedWithWarnings, InputAssemblyFailed:
		return true
	default:
		return false
	}
}

// WireValue returns the established JSON spelling for the input-assembly
// status. PENDING is retained for compatibility with existing payloads.
func (s InputAssemblyStatus) WireValue() string {
	if s == InputAssemblyPending {
		return "PENDING"
	}
	return string(s)
}

// MarshalJSON preserves the historical payload spelling while keeping the
// domain type explicit in Go. Invalid lifecycle values are rejected rather
// than being emitted through the overloaded status key.
func (s InputAssemblyStatus) MarshalJSON() ([]byte, error) {
	if !s.Valid() {
		return nil, fmt.Errorf("contract: invalid input assembly status %q", s)
	}
	return json.Marshal(s.WireValue())
}

// UnmarshalJSON accepts both legacy uppercase and canonical lowercase
// spellings. Unknown values are retained for lossless compatibility reads;
// callers must use Valid or ParseInputAssemblyStatus before treating them as
// an input-assembly state.
func (s *InputAssemblyStatus) UnmarshalJSON(data []byte) error {
	if s == nil {
		return nil
	}
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, ok := ParseInputAssemblyStatus(raw)
	if !ok {
		return fmt.Errorf("contract: invalid input assembly status %q", raw)
	}
	*s = parsed
	return nil
}

// ParseInputAssemblyStatus converts the case-insensitive payload spelling at
// the wire boundary into the distinct input-assembly domain type. The legacy
// canonical payload uses both "PENDING" and lowercase "completed" depending
// on its producer, so parsing is deliberately tolerant while the returned
// value is always canonical lowercase.
func ParseInputAssemblyStatus(raw string) (InputAssemblyStatus, bool) {
	status := InputAssemblyStatus(strings.ToLower(strings.TrimSpace(raw)))
	if !status.Valid() {
		return "", false
	}
	return status, true
}

// InputAssemblyStatus returns the payload's input-handoff status without
// conflating it with JobStatus. Invalid or lifecycle values return the zero
// value, so callers cannot accidentally treat SUCCEEDED/PUBLISHED as a
// producer-side completion.
func (p *JobPayloadV2) InputAssemblyStatus() InputAssemblyStatus {
	if p == nil {
		return ""
	}
	if !p.Status.Valid() {
		return ""
	}
	return p.Status
}

// SetInputAssemblyStatus writes the canonical wire spelling for an
// input-assembly state while preserving the existing payload key and JSON
// contract.
func (p *JobPayloadV2) SetInputAssemblyStatus(status InputAssemblyStatus) bool {
	if p == nil || !status.Valid() {
		return false
	}
	p.Status = status
	return true
}
