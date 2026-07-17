// Package pipeline serves the remote-engine + creatorflow integration
// endpoints. The HTTP layer fans out to a remote script-generation
// service, then forwards completed results into the Velox job queue.
//
// PR-DI-pipeline: constructor-based dependency injection. The previous
// design exposed three package-level globals (remoteEngineClient,
// pipelineEnqueuer, plus the old voiceover global removed in PR15.7a)
// mutated by InitRemoteEngine / InitPipelineEnqueuer at boot. Handlers
// now holds its dependencies on the struct so composition-root wiring
// is explicit, tests construct their own graphs, and `go test -race`
// stays clean across concurrent pipelines.
//
// Blocco 4 step #3: the legacy `forwarder *creatorflow.Service`
// fallback is gone. forward-completed result routing now runs through
// Resolver.Resolve exclusively; the composition root
// (cmd/server/bootstrap_composition.go) constructs the canonical
// Resolver and passes it via NewHandlersWithResolver. MasterURL is no
// longer discovered via `hostname -I` — it must be set in cfg
// (cfg.Workers.MasterURL or VELOX_MASTER_URL) at boot.
//
// File layout (one symbol-cluster per file so each file carries a
// single responsibility):
//
//	handlers.go        — Handlers struct, JobsDeps, public
//	                     constructors, WithJobsDeps, WithStore,
//	                     NewRemoteClientFromConfig.
//	generate.go        — h.Generate() (POST /api/remote/pipeline/generate):
//	                     request parsing, inline validation,
//	                     typed-DTO adoption (ParseRemotePipelineResult
//	                     → ToWorkerPayload), forward decision tree,
//	                     response envelope + voiceover-asset error
//	                     mapping.
//	pipeline.go (this) — RegisterRoutes, the package-internal
//	                     [PIPELINE] logger, the Resolver-based
//	                     forwardPipelineResultToWorker helper, and
//	                     the map-key probe firstStringResolver.
//	pipeline_lifecycle.go  — Status / Cancel / isTerminalStatus.
//	pipeline_create.go     — CreatePipelineRun + build helpers.
//	pipeline_run_status.go — PipelineRunStatus + projection.
//	pipeline_run_actions.go — lookupPipelineRun + cancel/retry/
//	                     timeline/artifacts/deliveries.
//	pipeline_run_validator.go — ValidateCreateRequest.
//	pipeline_scripts.go    — ScriptSimple / ScriptBatch.
//
// Forwards pipeline.go → forwarding.go is the next planned step; until
// then forwardPipelineResultToWorker + firstStringResolver stay co-
// located with RegisterRoutes so the package compiles.
package pipeline

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
)

// RegisterRoutes mounts all pipeline endpoints on the given engine.
//
//	adminAuth — when non-nil, applied to the operator routes
//	             (generate/status/cancel). Pass nil for the trusted
//	             network or test mounts.
func (h *Handlers) RegisterRoutes(r *gin.Engine, adminAuth gin.HandlerFunc) {
	r.POST("/api/script-simple", h.ScriptSimple())
	r.POST("/api/script-multiple", h.ScriptBatch())

	remote := r.Group("/api/remote/pipeline")
	if adminAuth != nil {
		remote.Use(adminAuth)
	}
	remote.POST("/generate", h.Generate())
	remote.GET("/status/:trace_id", h.Status())
	remote.DELETE("/cancel/:trace_id", h.Cancel())

	// Canonical, versioned pipeline-runs API. The POST creates a
	// durable pipeline_run before the remote call; the GET returns the
	// aggregated status projection. The :id param accepts either the
	// pipeline_run id (run_...) or the request_id (req_...) for
	// backwards compatibility with clients that only stored the request_id.
	r.POST("/api/v1/pipeline-runs", h.CreatePipelineRun())
	r.GET("/api/v1/pipeline-runs/:id", h.PipelineRunStatus())
	r.POST("/api/v1/pipeline-runs/:id/cancel", h.CancelPipelineRun())
	r.POST("/api/v1/pipeline-runs/:id/retry", h.RetryPipelineRun())
	r.GET("/api/v1/pipeline-runs/:id/timeline", h.PipelineRunTimeline())
	r.GET("/api/v1/pipeline-runs/:id/artifacts", h.PipelineRunArtifacts())
	r.GET("/api/v1/pipeline-runs/:id/deliveries", h.PipelineRunDeliveries())
}

// pipelineLog is the package-internal structured-log helper. Kept
// package-level (not a method on Handlers) so sibling files can call
// it without ceremony, and so diagnostic context such as "[PIPELINE]"
// remains uniform across all pipeline-installed routes.
func pipelineLog(format string, args ...interface{}) {
	log.Printf("[PIPELINE] "+format, args...)
}

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
