package outbox

import (
	"encoding/json"
	"fmt"
	"time"
)

// Status is the application-level state of an outbox event.
//
//	Pending   : written by a producer; not yet claimed.
//	Processing: a dispatcher has claimed it (locked_by + locked_until set).
//	Processed : handler succeeded.
//	Failed    : handler has exhausted MaxAttempts.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusProcessed  Status = "PROCESSED"
	StatusFailed     Status = "FAILED"
)

// Event is the canonical in-memory representation of an outbox row.
//
// Wire format (table: outbox_events):
//
//	event_id       TEXT PRIMARY KEY
//	aggregate_type TEXT NOT NULL
//	aggregate_id   TEXT NOT NULL
//	event_type     TEXT NOT NULL
//	payload_json   TEXT NOT NULL  (default '{}')
//	status         TEXT NOT NULL  (PENDING|PROCESSING|PROCESSED|FAILED)
//	available_at   TEXT NOT NULL  (RFC3339, deferred-dispatch support)
//	attempt_count  INTEGER NOT NULL
//	locked_by      TEXT
//	locked_until   TEXT
//	processed_at   TEXT
//	last_error     TEXT
//	created_at     TEXT NOT NULL
type Event struct {
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	Payload       []byte
	AttemptCount  int
	AvailableAt   time.Time
	CreatedAt     time.Time
}

// InsertParams is what a producer passes to Store.Insert.
type InsertParams struct {
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte

	// AvailableAt is the timestamp at which the event becomes claimable.
	// Zero-valued defaults to "now".
	AvailableAt time.Time
}

// HandlerError is returned by a Handler Handle to distinguish transient
// errors (will be retried up to MaxAttempts) from permanent errors (which
// move the event to FAILED immediately).
type HandlerError struct {
	// Transient: true means the dispatch loop will re-attempt after a
	// lock-extension; false means the event is marked FAILED on first hit.
	Transient bool
	Err       error
}

// Error implements error so HandlerError can be returned directly.
func (e *HandlerError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

// Unwrap implements errors.Unwrap.
func (e *HandlerError) Unwrap() error { return e.Err }

// Transient constructs a HandlerError marked transient.
func Transient(err error) error {
	return &HandlerError{Transient: true, Err: err}
}

// Permanent constructs a HandlerError marked permanent.
func Permanent(err error) error {
	return &HandlerError{Transient: false, Err: err}
}

// ParsePayload decodes e.Payload into target and returns a permanent
// HandlerError on failure. This is the canonical helper for outbox
// handlers so every handler uses the same JSON unmarshal + error
// wrapping pattern — no per-handler duplication.
//
// Usage:
//
//	var p struct { JobID string `json:"job_id"` }
//	if err := outbox.ParsePayload(e, &p); err != nil {
//	    return err
//	}
func ParsePayload(e Event, target interface{}) error {
	if err := json.Unmarshal(e.Payload, target); err != nil {
		return Permanent(fmt.Errorf("payload: %w", err))
	}
	return nil
}
