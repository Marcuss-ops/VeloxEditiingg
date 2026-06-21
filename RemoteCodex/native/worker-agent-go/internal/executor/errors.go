// Package executor is the canonical, worker-side catalog of executable
// task types. The Registry is the SINGLE source of truth for which
// executors a worker hosts. Every descriptor registered here surfaces in
// the worker's hello capability payload (PR-3.5).
//
// Errors are exposed as named sentinels so callers can branch with
// errors.Is. This package deliberately avoids free-form fmt.Errorf strings
// for conditions that a caller is expected to handle explicitly; every
// expected failure mode is a sentinel.
package executor

import "errors"

// Sentinel errors returned by Registry and friends. Match with errors.Is.
var (
	// ErrExecutorNotFound: Registry.Resolve was called for an (id, version)
	// the registry does not know about. The taskrunner converts this into
	// a stable "unsupported_executor" error code (PR-3.3 invariant).
	ErrExecutorNotFound = errors.New("executor: not found")

	// ErrExecutorExists: Registry.Register was called for an (id, version)
	// already present. Bootstrap surfaces this so duplicates are caught
	// BEFORE the worker hello is published.
	ErrExecutorExists = errors.New("executor: already registered")

	// ErrInvalidDescriptor: a Descriptor failed its invariants (empty or
	// malformed ID, version <= 0, unknown ResourceClass or TemporalMode,
	// "@" in ID). Wrapped with %w so callers can match the sentinel.
	ErrInvalidDescriptor = errors.New("executor: invalid descriptor")
)
