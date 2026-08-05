// Package identity — worker_id.go
//
// Canonical typed worker identity. Every identity-bearing surface in
// Velox — lease, job assignment, deployment record, smoke run,
// heartbeat, cache ownership, admin API, audit — MUST carry a
// WorkerID rather than a bare string, so an inventory hostname,
// container name, IP address, worker_name or display name can never be
// mistaken for an identity at compile time.
//
// The wire contract (gRPC/protobuf, JSON APIs, SQLite TEXT columns)
// stays plain string: convert at the boundary with ParseWorkerID /
// String(). The typed type exists to make the application model
// unambiguous, not to change serialized formats.
package identity

import (
	"fmt"
	"strings"
)

// WorkerID is the canonical worker identity. Hostname, IP, container
// name and systemd unit are operational ATTRIBUTES of a worker; only
// the WorkerID may be used as an identity key.
type WorkerID string

// ParseWorkerID converts a wire-boundary string into a WorkerID. The
// value is trimmed; no other normalization is applied — callers that
// need the canonical IP-derived form call Normalized() explicitly.
func ParseWorkerID(s string) WorkerID {
	return WorkerID(strings.TrimSpace(s))
}

// String returns the underlying string. Use at wire/storage boundaries.
func (id WorkerID) String() string { return string(id) }

// IsEmpty reports whether the ID is the empty string.
func (id WorkerID) IsEmpty() bool { return id == "" }

// Normalized returns the canonical normalized form (see
// NormalizeWorkerID). IDs that are empty or match no legacy pattern are
// returned unchanged.
func (id WorkerID) Normalized() WorkerID {
	return WorkerID(NormalizeWorkerID(id.String()))
}

// IsValid reports whether the ID matches the canonical worker_id shape
// (RW-PROD-001 A4). Call after Normalized() when the caller expects the
// canonical IP-derived form.
func (id WorkerID) IsValid() bool { return IsValidWorkerID(id.String()) }

// Validate returns a descriptive error for empty or malformed IDs.
func (id WorkerID) Validate() error {
	if id == "" {
		return fmt.Errorf("identity: empty worker id")
	}
	if !id.IsValid() {
		return fmt.Errorf("identity: invalid worker id %q (must match ^[a-z][a-z0-9_-]{2,62}$)", id.String())
	}
	return nil
}

// NewWorkerID generates a fresh canonical "worker-xxxxxxxx" ID.
func NewWorkerID() WorkerID { return WorkerID(GenerateWorkerID()) }

// WorkerIdentity is the canonical worker identity record.
//
// ID is the ONLY field usable as an identity (lease, job assignment,
// deployment record, smoke run, heartbeat, cache ownership, admin API,
// audit). HostID (e.g. machine-id / inventory host) and NodeName (e.g.
// Kubernetes node / systemd unit) are operational attributes: they
// describe WHERE a worker runs, never WHAT it is.
type WorkerIdentity struct {
	ID       WorkerID
	HostID   string
	NodeName string
}

// NewWorkerIdentity builds a canonical WorkerIdentity from a WorkerID
// plus its operational attributes.
func NewWorkerIdentity(id WorkerID, hostID, nodeName string) WorkerIdentity {
	return WorkerIdentity{ID: id, HostID: hostID, NodeName: nodeName}
}

// Validate checks that the identity carries a non-empty, valid WorkerID.
// HostID / NodeName are informational and never validated.
func (wi WorkerIdentity) Validate() error {
	if wi.ID.IsEmpty() {
		return fmt.Errorf("identity: WorkerIdentity.ID is empty")
	}
	return wi.ID.Validate()
}
