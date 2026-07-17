// Package pipeline: forwarding.go isolates the canonical sync-forward
// path that turns a typed remote-engine result into a Velox job
// payload via creatorflow.Resolver.Resolve.
//
// The HTTP handler side callers:
//
//   - h.Generate (POST /api/remote/pipeline/generate, in generate.go)
//     reaches forwardPipelineResultToWorker synchronously when the
//     remote engine has returned a complete result.
//
//   - The async forward-and-poll path runs through
//     CreatorForwardingRunner in cmd/creatorrunner and ultimately
//     reaches the same Resolver.Resolve API.
//
// Both paths converge on the same Resolver contract; this file owns
// the Resolver entry call + a tiny map-key probe (firstStringResolver)
// used to recover the canonical source_job_id and target_executor_id
// from the worker payload before resolving.
//
// pipelineLog (the package-internal logger) lives in logging.go inside
// the same Go package, so forwarding.go can call it without owning
// the helper itself. The "[PIPELINE]" diagnostic prefix in this file
// remains uniform with the rest of the package.
package pipeline

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/creatorflow"
)

// forwardPipelineResultToWorker is the package-internal method that
// turns a remote-engine result map into a Velox job payload and
// enqueues it through the canonical Resolver.Resolve entry point.
//
// Blocco 5 of the Verdetto (P1 #11): this method delegates to the same
// Resolver the CreatorForwardingRunner uses, so the handler's sync
// forward path and the runner's async poll-and-forward path converge
// on the same (job_id, forwarding_id) for the same input. The legacy
// creatorflow.Service forwarder fallback was removed in Blocco 4 step
// #3 — composition-root callers must wire a non-nil Resolver.
func (h *Handlers) forwardPipelineResultToWorker(ctx context.Context, result map[string]interface{}) (map[string]interface{}, error) {
	pipelineLog("FORWARD: building worker payload...")

	if h.resolver == nil {
		// Fail loud: this means cmd/server wiring is broken (the
		// composition root unconditionally builds the Resolver
		// before constructing Handlers). Hiding it behind a legacy
		// forwarder fallback was removed in Blocco 4 step #3 because
		// the forwarder shim was indistinguishable from a
		// misconfigured Resolver at the URL-rewrite step.
		return nil, fmt.Errorf("pipeline handler requires a wired resolver (composition root MUST pass creatorflow.Resolver)")
	}

	out, err := h.resolver.Resolve(ctx, creatorflow.ResolveRequest{
		ForwardingID:     "", // sync handler path: INSERT PENDING row
		SourceProvider:   "remote_engine",
		SourceJobID:      firstStringResolver(result, "job_id", "trace_id", "id"),
		TargetExecutorID: firstStringResolver(result, "executor_id", "pipeline_id"),
		Payload:          result,
	})
	if err != nil {
		if err == creatorflow.ErrResolverNotComplete {
			return nil, nil
		}
		pipelineLog("FORWARD: Resolver.Resolve FAILED: %v", err)
		return nil, err
	}
	if out != nil {
		pipelineLog("FORWARD: enqueued via Resolver job_id=%s forwarding_id=%s",
			out.JobID, out.ForwardingID)
		return out.Response, nil
	}
	return nil, nil
}

// firstStringResolver reads the first non-empty string value from a map
// across the provided keys. Mirrors creatorflow.firstString but lives
// here so the pipeline package does not need to export the helper.
func firstStringResolver(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}
