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

import (
	"errors"

	"velox-shared/taskcontract"
)

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

	// ErrInvalidDescriptor: a Descriptor failed its invariants — empty or
	// malformed ID, version <= 0, unknown ResourceClass or TemporalMode,
	// "@" in ID — OR a TaskSpec failed its struct invariants (job_id or
	// executor_id missing, version <= 0, nil spec). The TaskSpec.Validate
	// method in the shared velox-shared/taskcontract package wraps each
	// violation with taskcontract.ErrInvalidSpec via fmt.Errorf("%w: ...").
	//
	// This sentinel is an ALIAS of taskcontract.ErrInvalidSpec (same Go
	// value, same pointer) so errors.Is semantics round-trip across the
	// shared-contract → executor-package boundary. Mirrors the existing
	// `type TaskSpec = taskcontract.TaskSpec` alias convention in
	// executor/types.go: canonical source lives in shared; executor-side
	// name preserved for backwards compatibility with legacy call sites
	// and registry_test.go's pre-cutover assertions.
	ErrInvalidDescriptor = taskcontract.ErrInvalidSpec
)
