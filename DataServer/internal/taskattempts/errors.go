package taskattempts

import "errors"

// ErrAttemptNotFound is returned when an attempt ID does not match any row.
var ErrAttemptNotFound = errors.New("taskattempts: attempt not found")

// ErrDuplicateReport is returned when a final report is submitted for an
// already-terminal attempt (idempotent rejection).
var ErrDuplicateReport = errors.New("taskattempts: duplicate final report")

// ErrStaleReport is returned when a worker/lease CAS tuple does not match.
var ErrStaleReport = errors.New("taskattempts: stale report (worker or lease mismatch)")

// ErrActiveAttemptExists is returned when creating a new attempt while a
// non-terminal attempt already exists for the same task.
var ErrActiveAttemptExists = errors.New("taskattempts: active attempt already exists for this task")

// ErrReportConflict is returned when a raw worker report is ingested for an
// attempt that already has a different report hash stored. Attempt reports
// are immutable once persisted.
var ErrReportConflict = errors.New("taskattempts: raw report conflict (attempt_id already has a different hash)")
