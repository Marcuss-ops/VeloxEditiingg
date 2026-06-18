package outbox

import "errors"

// ErrNoHandler is returned by the Dispatcher when an event_type has no
// registered handler. Per the PR 8 spec, registering a new handler must
// work without an SQL migration; if a producer somehow wrote an event with
// an unrecognised type the dispatcher must surface it cleanly.
var ErrNoHandler = errors.New("outbox: no handler registered for event type")

// ErrInvalidEvent is returned by Producer-side helpers / Store.Insert when
// required fields are missing. Validation lives in Store.Insert.
var ErrInvalidEvent = errors.New("outbox: invalid event: aggregate_type/event_type/id required")

// ErrUnknownStatus is returned by internal cast helpers; surfaces to table
// columns that were written by a future-proofing hand-edited migration.
var ErrUnknownStatus = errors.New("outbox: unknown status")
