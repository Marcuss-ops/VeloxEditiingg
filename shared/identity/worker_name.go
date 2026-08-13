// Package identity — worker_name.go
//
// Typed operator-facing worker display name. WorkerName is the MUTABLE
// presentation metadata of a worker, deliberately distinct from WorkerID:
// it never participates in leases, job assignment, cache ownership, audit
// identity or any other identity key. It exists so a display name can never
// be mistaken for (or silently substituted into) the immutable worker_id at
// compile time, and so the single validation rule lives in exactly one place.
//
// The wire contract (gRPC/protobuf, JSON APIs, SQLite TEXT columns) stays a
// plain string: convert at the boundary with ParseWorkerName / String().
package identity

import (
	"fmt"
	"strings"
)

// MaxWorkerNameLength is the upper bound for an operator-facing worker name.
// It matches the shared validation.MaxIdentifierLength convention so names
// and identifiers share one length ceiling.
const MaxWorkerNameLength = 128

// WorkerName is the operator-facing display name of a worker. Only the
// WorkerID may be used as an identity; WorkerName is presentation-only.
type WorkerName string

// ParseWorkerName converts a wire-boundary string into a WorkerName. The
// value is trimmed; no other normalization is applied. Callers that must
// reject malformed names call Validate() — the single validation SSOT.
func ParseWorkerName(s string) WorkerName {
	return WorkerName(strings.TrimSpace(s))
}

// String returns the underlying string. Use at wire/storage boundaries.
func (n WorkerName) String() string { return string(n) }

// IsEmpty reports whether the name is empty after trimming.
func (n WorkerName) IsEmpty() bool { return n == "" }

// Validate reports an error for a name that cannot be safely rendered in
// dashboards, logs or JSON payloads: empty, longer than MaxWorkerNameLength,
// or containing control characters (which would enable log/HTML injection or
// corrupt a single-line admin surface). This is the ONLY place that decides
// whether a worker name is well-formed; callers must not re-implement the
// rule.
func (n WorkerName) Validate() error {
	if n.IsEmpty() {
		return fmt.Errorf("identity: empty worker name")
	}
	runes := 0
	for _, r := range string(n) {
		runes++
		if runes > MaxWorkerNameLength {
			return fmt.Errorf("identity: worker name exceeds %d characters", MaxWorkerNameLength)
		}
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("identity: worker name %q contains control characters", n.String())
		}
	}
	return nil
}
