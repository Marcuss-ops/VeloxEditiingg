// Package api — Step 4/15 fleet-operator: admin audit DTO for the
// fleet_operations ledger.
//
// The shape mirrors admin_workers_dto.go's envelope discipline:
//   * GET /api/v1/admin/operations          → AdminOperationsListResponse
//     (envelope { count, operations[] }) so the dashboard can
//     render the dashboard widget shape unchanged when migrations
//     or new fields land.
//   * GET /api/v1/admin/operations/{id}     → OperationCard alone,
//     NOT wrapped (consistent with /api/v1/admin/workers/{id}:
//     the single-row endpoint returns the entity directly).
//
// Time fields are RFC3339 strings (NOT time.Time) so the JSON
// envelope is portable: a C++/TS/Python dashboard that already
// parses RFC3339 for heartbeat timestamps parses the operation
// timestamps without a type-shaped dependency on Go's string-
// ormat-cast.
//
// omitempty applies to started_at / finished_at / payload /
// error_message so the row's envelope stays clean for a
// QUEUED-with-no-execution row (started_at is NULL → omitted,
// payload "{}" is omitted because Go's json.RawMessage lifts
// to empty RawMessage when set to the "{}" byte string in the
// repository). The shape is stable as terminal transitions
// populate new fields.
package api

// OperationCard is the per-operation admin audit shape. Field
// mapping source is store.Operation; converting to a dedicated
// DTO (rather than aliasing store.Operation directly) lets us
// future-proof surface drift (a future aggregate field like
// "downstream_artifacts" that lives only on the admin view).
type OperationCard struct {
	OperationID  string `json:"operation_id"`
	WorkerID     string `json:"worker_id"`
	Op           string `json:"op"`
	RequestedBy  string `json:"requested_by"`
	Reason       string `json:"reason"`
	Status       string `json:"status"`
	QueuedAt     string `json:"queued_at"`
	StartedAt    string `json:"started_at,omitempty"`
	FinishedAt   string `json:"finished_at,omitempty"`
	Payload      string `json:"payload,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// AdminOperationsListResponse is the JSON envelope for
// GET /api/v1/admin/operations.
//
// count is a convenience for dashboards; len(operations) is the
// canonical count and the two MUST agree on every response
// (handler-side invariant, asserted by test).
//
// operations MUST NOT be `null` when the dispatch returns
// zero rows: the envelope is {"count": 0, "operations": []}
// so the dashboard parser does not have to fork on a missing
// field. The handler-side init pre-populates the slice with
// capacity zero before the loop runs.
type AdminOperationsListResponse struct {
	Count      int             `json:"count"`
	Operations []OperationCard `json:"operations"`
}
