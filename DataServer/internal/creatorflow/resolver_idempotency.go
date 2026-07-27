package creatorflow

import (
	"context"
	"log"
	"strings"

	"velox-server/internal/jobs"
)

// checkIdempotencyFastPath returns the cached ResolveOutput when the Job
// already exists with a matching payload hash. It also attempts to repair
// a stuck forwarding row via EnsureForwarded.
//
// Return contract:
//
//	(out != nil, err == nil)        — cached hit, return immediately
//	(out == nil, err == nil)        — no hit, caller proceeds to INSERT
//	(out == nil, err != nil)        — hard conflict (ErrIdempotencyKeyReused
//	                                  or any other error); do NOT continue.
//
// The payload-hash conflict check (P0 #3 contract) fires against the
// bySource canonical view: when the forwarding row's payload_sha256 is
// non-empty AND it differs from the SHA of the freshly-rebuilt
// workerPayload, the function returns ErrIdempotencyKeyReused. Pre-055
// rows whose payload_sha256 is the empty default cleanly skip this
// check and preserve legacy idempotency semantics (no false 409s).
//
// TODO(p0-hash-migration): when the post-055 forwarding population has
// fully aged out (operator-visible metric or a cutoff date), backfill
// payload_sha256 from payload_json for ANY row still carrying the
// empty default, then drop the `cf.PayloadSHA256 != ""` shortcut here.
// Until that migration finishes, the bypass is necessary to keep
// pre-migration duplicates behaving as they did at HEAD.
//
// EnsureForwarded uses the bySource canonical row id when available
// and falls back to req.ForwardingID only when the lookup did not
// resolve to a row. That preserves the runner-path lease-reclaim
// contract (the runner's explicit id is honored if bySource races or
// transiently fails); the hash comparison still anchors to the
// canonical row.
func (r *Resolver) checkIdempotencyFastPath(
	ctx context.Context,
	req ResolveRequest,
	jobID, targetExecutor string,
	workerPayload map[string]interface{},
) (*ResolveOutput, error) {
	existing, getErr := r.jobLookup.Get(ctx, jobID)
	if getErr != nil || existing == nil || existing.ID != jobID {
		return nil, nil
	}

	// Canonical-source lookup for the hash comparison. The bySource
	// view is the single source of truth for "what was previously
	// stamped on this forwarding row"; we MUST compare incoming
	// payload SHA against THAT row, not against any explicit id
	// supplied by the caller.
	forwardingID := ""
	cf, lookupErr := r.forwardRepo.GetCreatorForwardingBySource(ctx, req.SourceProvider, req.SourceJobID, targetExecutor)
	if lookupErr == nil && cf != nil {
		forwardingID = cf.ForwardingID

		// Payload-hash conflict check. Compute the SHA of the canonical
		// (rewritten) worker payload and compare it to the stored
		// payload_sha256. Mismatch → ErrIdempotencyKeyReused → HTTP 409.
		if cf.PayloadSHA256 != "" {
			_, incomingSHA := resolverMarshalPayload(workerPayload)
			if incomingSHA != "" && cf.PayloadSHA256 != incomingSHA {
				log.Printf(
					"[CREATORFLOW] idempotency conflict: forwarding=%s job=%s stored_sha=%s incoming_sha=%s source=%s source_job=%s target_executor=%s",
					cf.ForwardingID,
					jobID,
					shaShort(cf.PayloadSHA256),
					shaShort(incomingSHA),
					req.SourceProvider,
					req.SourceJobID,
					targetExecutor,
				)
				return nil, ErrIdempotencyKeyReused
			}
		}
	}

	// Explicit-id fallback only when bySource didn't resolve. The
	// runner path supplies req.ForwardingID directly (the row it
	// leased); if the canonical lookup at the start of this block
	// raced, transiently failed, or has been GC'd by an unrelated
	// writer, we MUST still honor the lease id so EnsureForwarded
	// can complete the FORWARDED repair.
	if forwardingID == "" {
		forwardingID = req.ForwardingID
	}

	// Repair the forwarding row if it exists and is not yet FORWARDED.
	// EnsureForwarded is idempotent: nil if already FORWARDED with the
	// same job_id; ErrTransitionConflict if FORWARDED with a different
	// job_id or in a terminal FAILED/BLOCKED state. We intentionally
	// ignore ErrTransitionConflict here (the Job already exists; the
	// forwarding row repair is best-effort and a reaper can reconcile).
	if forwardingID != "" {
		if repairErr := r.forwardRepo.EnsureForwarded(ctx, forwardingID, jobID); repairErr != nil {
			log.Printf("[CREATORFLOW] idempotency fast-path: EnsureForwarded failed forwarding=%s job=%s: %v",
				forwardingID, jobID, repairErr)
		}
	}

	return &ResolveOutput{
		JobID:        existing.ID,
		ForwardingID: forwardingID,
		Response:     buildIdempotentResolveResponse(existing),
	}, nil
}

// shaShort returns the first 12 hex chars of s, or s if shorter. Used to
// keep the conflict log line readable without leaking the full hash into
// the operator-eye view.
func shaShort(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
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
