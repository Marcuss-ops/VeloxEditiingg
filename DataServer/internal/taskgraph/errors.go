package taskgraph

import "errors"

// ErrTaskNotFound is returned when a task ID does not match any row.
var ErrTaskNotFound = errors.New("taskgraph: task not found")

// ErrTransitionConflict is returned when the CAS precondition (status or
// revision) does not match.
var ErrTransitionConflict = errors.New("taskgraph: transition conflict (status or revision mismatch)")

// ErrLeaseMismatch is returned when a worker-identity CAS tuple does not
// match the stored lease.
var ErrLeaseMismatch = errors.New("taskgraph: lease mismatch (worker or lease ID)")

// ErrInvalidSpec is returned when a TaskSpec fails validation.
var ErrInvalidSpec = errors.New("taskgraph: invalid task spec")
