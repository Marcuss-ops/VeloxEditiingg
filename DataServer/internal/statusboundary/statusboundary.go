// Package statusboundary contains explicit status-domain adapters at
// canonical payload boundaries. It deliberately has no parser for a generic
// overloaded "status" field: callers must name the domain they expect.
package statusboundary

import (
	"strings"

	"velox-server/internal/deliveries"
	"velox-server/internal/jobs"
	"velox-server/internal/publicationstate"
	"velox-shared/contract"
)

// Domains is the typed view of statuses that may accompany a canonical
// payload. A nil field means that domain is not present at this boundary;
// absence is different from an invalid status and avoids guessing from the
// overloaded wire key.
type Domains struct {
	InputAssembly *contract.InputAssemblyStatus
	Job           *jobs.JobStatus
	Delivery      *deliveries.DeliveryStatus
	Publication   *publicationstate.PublicationStatus
}

// ParseInputAssembly parses only producer-side handoff statuses.
func ParseInputAssembly(raw string) (contract.InputAssemblyStatus, bool) {
	return contract.ParseInputAssemblyStatus(raw)
}

// ParseJob parses only canonical Velox job lifecycle statuses.
func ParseJob(raw string) (jobs.JobStatus, bool) {
	status := jobs.JobStatus(strings.ToUpper(strings.TrimSpace(raw)))
	if !status.Valid() {
		return "", false
	}
	return status, true
}

// ParseDelivery parses only canonical delivery lifecycle statuses.
func ParseDelivery(raw string) (deliveries.DeliveryStatus, bool) {
	status := deliveries.DeliveryStatus(strings.ToUpper(strings.TrimSpace(raw)))
	if !status.Valid() {
		return "", false
	}
	return status, true
}

// ParsePublication parses only canonical publication lifecycle statuses.
func ParsePublication(raw string) (publicationstate.PublicationStatus, bool) {
	status := publicationstate.PublicationStatus(strings.ToUpper(strings.TrimSpace(raw)))
	if !status.Valid() {
		return "", false
	}
	return status, true
}
