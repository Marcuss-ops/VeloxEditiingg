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
//   - h.CreatorPush (POST /api/v1/creator/jobs, in creator_push.go)
//     accepts a completed payload produced independently by a creator
//     machine and routes it through the same Resolver.
//
//   - The async forward-and-poll path runs through
//     CreatorForwardingRunner in cmd/creatorrunner and ultimately
//     reaches the same Resolver.Resolve API.
//
// Every path converges on the same Resolver contract; this file owns
// the Resolver entry call + a tiny map-key probe (firstStringResolver)
// used to recover canonical source and executor identities.
//
// pipelineLog (the package-internal logger) lives in logging.go inside
// the same Go package, so forwarding.go can call it without owning
// the helper itself. The "[PIPELINE]" diagnostic prefix in this file
// remains uniform with the rest of the package.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"velox-server/internal/creatorflow"
	"velox-server/internal/pipelineruns"
)

// resolveCompletedPayload is the single HTTP-side adapter into
// creatorflow.Resolver.Resolve. It is intentionally provider-agnostic so both
// master-initiated remote-engine results and creator-initiated push requests
// converge on the same forwarding row, deterministic Job identity and atomic
// Job+Task write path.
func (h *Handlers) resolveCompletedPayload(
	ctx context.Context,
	sourceProvider string,
	sourceJobID string,
	targetExecutorID string,
	result map[string]interface{},
) (map[string]interface{}, error) {
	if h.resolver == nil {
		return nil, fmt.Errorf("pipeline handler requires a wired resolver (composition root MUST pass creatorflow.Resolver)")
	}

	sourceProvider = strings.TrimSpace(sourceProvider)
	if sourceProvider == "" {
		return nil, fmt.Errorf("source_provider is required")
	}
	sourceJobID = strings.TrimSpace(sourceJobID)
	if sourceJobID == "" {
		return nil, fmt.Errorf("source_job_id is required")
	}
	if result == nil {
		return nil, fmt.Errorf("payload is required")
	}

	out, err := h.resolver.Resolve(ctx, creatorflow.ResolveRequest{
		ForwardingID:     "", // HTTP push/sync path: INSERT PENDING row
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: strings.TrimSpace(targetExecutorID),
		Payload:          result,
	})
	if err != nil {
		pipelineLog("FORWARD: Resolver.Resolve FAILED provider=%s source_job=%s: %v", sourceProvider, sourceJobID, err)
		return nil, err
	}
	if out == nil {
		return nil, nil
	}

	pipelineLog(
		"FORWARD: enqueued via Resolver provider=%s source_job=%s job_id=%s forwarding_id=%s",
		sourceProvider,
		sourceJobID,
		out.JobID,
		out.ForwardingID,
	)
	return out.Response, nil
}

// forwardPipelineResultToWorker turns a remote-engine result map into a Velox
// job payload and enqueues it through the canonical provider-agnostic
// adapter.
//
// Deprecated: POST /api/v1/creator/jobs (the new creator_push path) is the
// canonical intake surface. /api/remote/pipeline remains functional for
// backward compatibility with existing remote-engine workers and will be
// removed in v2.0.0. The two paths converge on the SAME typed DTO
// normalization (normalizeRemoteEngineIntake) so behavior drift between
// them is mathematically impossible — see docs/CREATOR-PUSH.md §Deprecation
// timeline for the migration plan.
func (h *Handlers) forwardPipelineResultToWorker(ctx context.Context, result map[string]interface{}) (map[string]interface{}, error) {
	pipelineLog("FORWARD: building worker payload...")

	// DRIFT-PROOF: route the legacy remote-engine raw map through the same
	// shared normalizer used by /api/v1/creator/jobs so both intake paths
	// converge on identical typed-DTO normalization + identity derivation.
	// envelopeSourceProvider is hardcoded to "remote_engine" because the
	// legacy route never carries an envelope (the SourceProvider identity
	// is the URL itself). envelopeSourceJobID and envelopeTargetExecutorID
	// are empty so the normalizer falls back to firstStringResolver on
	// the typed worker payload (preserves the pre-refactor behavior for
	// existing callers that already passed job_id / executor_id as
	// top-level keys in the raw result map).
	normalized, err := normalizeRemoteEngineIntake(result, "remote_engine", "", "")
	if err != nil {
		return nil, err
	}

	forwarded, err := h.resolveCompletedPayload(
		ctx,
		normalized.SourceProvider,
		normalized.SourceJobID,
		normalized.TargetExecutorID,
		normalized.WorkerPayload,
	)
	if err == creatorflow.ErrResolverNotComplete {
		return nil, nil
	}

	// DEPRECATED-PATH TELEMETRY (post-CAS, mirroring creator_push).
	// The counter + log line are stamped ONLY after the atomic CAS
	// committed (resolveCompletedPayload returned success), so we never
	// claim success that the database has not seen. The log carries the
	// "use POST /api/v1/creator/jobs" hint so operators searching for
	// legacy traffic in observability dashboards can pivot straight to
	// the migration target. The legacy path is expected to trend to
	// zero as remote-engine workers migrate; sustained traffic after
	// v2.0.0 will trigger a follow-up that deletes this branch entirely.
	if err == nil && forwarded != nil {
		h.intakeSinkOrNoop().IncAccepted("remote_engine_legacy")
		jobID, _ := forwarded["job_id"].(string)
		pipelineLog(
			"DEPRECATED_REMOTE_ENGINE_INTAKE path=remote_engine_legacy source_provider=%s source_job_id=%s target_executor_id=%s job_id=%s — use POST /api/v1/creator/jobs",
			normalized.SourceProvider,
			normalized.SourceJobID,
			normalized.TargetExecutorID,
			jobID,
		)
	}

	return forwarded, err
}

// syncForwardResult handles the common sync-forward path for both
// CreatePipelineRun and RetryPipelineRun. It forwards a completed remote
// result to the Velox worker queue, updates the pipeline_run row, and
// returns the forwarded worker response. If forwarding fails, the run is
// marked as FORWARDING so a reconciler can retry.
func (h *Handlers) syncForwardResult(ctx context.Context, pr *pipelineruns.PipelineRun, result, workerPayload map[string]interface{}) (map[string]interface{}, error) {
	pipelineLog("FORWARD: result complete — forwarding to Velox workers (sync) run=%s", pr.ID)
	forwarded, forwardErr := h.forwardPipelineResultToWorker(ctx, workerPayload)
	if forwardErr != nil {
		pipelineLog("FORWARD: sync forward FAILED run=%s: %v", pr.ID, forwardErr)
		if err := h.store.UpdatePipelineRunStatus(ctx, pr.ID,
			pipelineruns.StatusForwarding, "sync forward failed"); err != nil {
			pipelineLog("FORWARD: failed to mark FORWARDING run=%s: %v", pr.ID, err)
		} else {
			pr.Status = pipelineruns.StatusForwarding
		}
	} else if forwarded != nil {
		workerJobID, _ := forwarded["job_id"].(string)
		pipelineLog("FORWARD: sync forward SUCCESS run=%s worker_job=%s", pr.ID, workerJobID)
		if workerJobID != "" {
			pr.VeloxJobID = workerJobID
			if err := h.store.UpdatePipelineRunVeloxJob(ctx, pr.ID,
				workerJobID, pipelineruns.StatusWorkerQueued); err != nil {
				pipelineLog("FORWARD: failed to stamp velox_job_id run=%s: %v", pr.ID, err)
			} else {
				pr.Status = pipelineruns.StatusWorkerQueued
			}
		}
	}

	// Update the run with the result JSON for audit.
	if resultJSON, mErr := json.Marshal(result); mErr == nil {
		if err := h.store.UpdatePipelineRunResult(ctx, pr.ID, string(resultJSON)); err != nil {
			pipelineLog("FORWARD: failed to stamp result_json run=%s: %v", pr.ID, err)
		}
	}

	return forwarded, forwardErr
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
