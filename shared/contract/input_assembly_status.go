package contract

import "strings"

// InputAssemblyStatus describes the producer-side handoff state of a payload.
// It must not be confused with JobStatus: InputAssemblyCompleted means the
// request envelope is fully assembled, not that rendering or delivery is done.
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
// conflating it with JobStatus. JobPayloadV2.Status remains a string because
// it is an established overloaded wire field; callers that need semantics
// should use this parser/accessor rather than casting it to another domain.
func (p *JobPayloadV2) InputAssemblyStatus() InputAssemblyStatus {
	if p == nil {
		return ""
	}
	status, ok := ParseInputAssemblyStatus(p.Status)
	if !ok {
		return ""
	}
	return status
}

// SetInputAssemblyStatus writes the canonical wire spelling for an
// input-assembly state while preserving the existing payload key and JSON
// contract.
func (p *JobPayloadV2) SetInputAssemblyStatus(status InputAssemblyStatus) bool {
	if p == nil || !status.Valid() {
		return false
	}
	// Preserve the historical uppercase default on the wire. The completed
	// handoff values remain lowercase because that is the established creator
	// payload contract.
	if status == InputAssemblyPending {
		p.Status = "PENDING"
	} else {
		p.Status = string(status)
	}
	return true
}
