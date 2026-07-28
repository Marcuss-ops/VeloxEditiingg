// Package pipeline — response_shaping.go owns the
// Submit-job response envelope helpers:
//   - submitJobAcceptedStatus: canonical 202 Accepted status
//     the SubmitJob handler emits on successful enqueue.
//   - buildSubmittedJobLocation: canonical Location-header
//     value for a freshly-accepted SubmitJob.
//
package pipeline

import "net/http"

// submitJobAcceptedStatus is the canonical 202 Accepted status the
// SubmitJob handler emits on successful enqueue. Anchored here so
// the wire-level status code can be referenced from a single
// location in audit / tests / docs.
const submitJobAcceptedStatus = http.StatusAccepted

// buildSubmittedJobLocation builds the canonical Location-header
// value for a freshly-accepted SubmitJob. The polling endpoint is
// the canonical self-link a client follows to learn the job's
// terminal state.
func buildSubmittedJobLocation(jobID string) string {
	return "/api/v1/jobs/" + jobID
}
