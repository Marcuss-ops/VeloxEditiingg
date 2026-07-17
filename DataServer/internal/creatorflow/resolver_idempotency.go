package creatorflow

import (
	"context"
	"log"
	"strings"

	"velox-server/internal/jobs"
)

// checkIdempotencyFastPath returns a ResolveOutput when the Job already
// exists. It also attempts to repair a stuck forwarding row via
// EnsureForwarded. The caller (Resolve) should return the output
// immediately when the second return value is true.
func (r *Resolver) checkIdempotencyFastPath(ctx context.Context, req ResolveRequest, jobID, targetExecutor string) (*ResolveOutput, bool) {
	existing, getErr := r.enqueuer.Jobs.Get(ctx, jobID)
	if getErr != nil || existing == nil || existing.ID != jobID {
		return nil, false
	}

	forwardingID := req.ForwardingID
	if forwardingID == "" {
		if cf, lookupErr := r.dbStore.GetCreatorForwardingBySource(ctx, req.SourceProvider, req.SourceJobID, targetExecutor); lookupErr == nil && cf != nil {
			forwardingID = cf.ForwardingID
		}
	}

	// Repair the forwarding row if it exists and is not yet FORWARDED.
	// EnsureForwarded is idempotent: nil if already FORWARDED with the
	// same job_id; ErrTransitionConflict if FORWARDED with a different
	// job_id or in a terminal FAILED/BLOCKED state.
	if forwardingID != "" {
		if repairErr := r.dbStore.EnsureForwarded(ctx, forwardingID, jobID); repairErr != nil {
			log.Printf("[CREATORFLOW] idempotency fast-path: EnsureForwarded failed forwarding=%s job=%s: %v",
				forwardingID, jobID, repairErr)
			// Non-fatal: the Job already exists, the forwarding row
			// repair is best-effort. A reaper or operator can
			// reconcile later. We still return the idempotent
			// response so the caller doesn't re-enqueue.
		}
	}

	return &ResolveOutput{
		JobID:        existing.ID,
		ForwardingID: forwardingID,
		Response:     buildIdempotentResolveResponse(existing),
	}, true
}

// buildIdempotentResolveResponse is the response body for the
// idempotency fast-path (the Job already exists). The runner path
// typically hits this on a duplicate poll + lease reclaim; the handler
// path hits it on a duplicate webhook.
func buildIdempotentResolveResponse(existing *jobs.Job) map[string]interface{} {
	resp := map[string]interface{}{
		"ok":                true,
		"job_id":            existing.ID,
		"created":           false,
		"status":            string(existing.Status),
		"enqueue_confirmed": true,
		"job_type":          "process_video",
	}
	if runID := strings.TrimSpace(existing.RunID); runID != "" {
		resp["job_run_id"] = runID
		resp["run_id"] = runID
	}
	return resp
}

// buildFreshResolveResponse is the response body for the freshly-created
// path (Job did not exist before Resolve ran).
//
// Status string-typing consistency: `jobs.StatusPending` is a typed
// `jobs.Status` constant (alias-shared with `store.JobStatus`). When
// stored in a `map[string]interface{}`, the value's DYNAMIC TYPE is the
// typed alias — not an untyped `string`. Downstream consumers that
// compare to the untyped literal "PENDING" (e.g.
// TestForwardCompletedEnqueuesWorkerJob) hit Go's interface-equality
// rule: two interface values are equal iff both their dynamic type AND
// dynamic value are equal. typed-string("PENDING") != string("PENDING")
// even when both values spell PENDING.
//
// To keep both response builders (buildIdempotentResolveResponse +
// buildFreshResolveResponse) wire-compatible with the HTTP/script
// callers — which universally treat response["status"] as a plain
// string — both builders cast to `string(...)`. This is the same
// pattern that jobStatus comparisons throughout the codebase already
// use (e.g. sqlite_writer.go does `string(j.Status)` for comparison
// with literal "PENDING"). The duplication is deliberate: the typed
// constant stays in the domain model (jobs.Status) and the wire shape
// stays as plain string.
func buildFreshResolveResponse(job *jobs.Job) map[string]interface{} {
	resp := map[string]interface{}{
		"ok":                true,
		"job_id":            job.ID,
		"created":           true,
		"status":            string(jobs.StatusPending),
		"enqueue_confirmed": true,
		"job_type":          "process_video",
	}
	if runID := strings.TrimSpace(job.RunID); runID != "" {
		resp["job_run_id"] = runID
		resp["run_id"] = runID
	}
	return resp
}
