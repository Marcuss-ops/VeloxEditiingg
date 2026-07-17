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
package pipeline

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"velox-shared/payload"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// Handlers carries every dependency the pipeline HTTP layer needs.
//
// The struct carries the mandatory remote params (cfg, enqueuer, client,
// resolver) plus optional cancel-side dependencies bundled in JobsDeps.
//
// Blocco 5 of the Verdetto (P1 #11) — Resolver is the SINGLE
// authoritative forward-completed entry point. Built ONCE at
// construction time so URL resolution does NOT run per request, and
// so the HTTP handler converges with the CreatorForwardingRunner on
// the same (job_id, forwarding_id).
type Handlers struct {
	cfg      *config.Config
	enqueuer *enqueue.Enqueuer
	client   *remoteengine.Client
	resolver *creatorflow.Resolver
	jobs     JobsDeps
	store    *store.SQLiteStore
}

// JobsDeps bundles the optional jobs-layer dependencies used by
// PipelineCancel: list-all for hit-detection, delete for cleanup,
// command manager for per-worker cancel notifications.
type JobsDeps struct {
	Reader jobs.Reader
	Writer jobs.Writer
	CmdMgr *workers.CommandManager
}

// NewHandlers constructs a Handlers with the three mandatory deps:
//
//	cfg       — render settings (remote URL, poll interval, ...)
//	enqueuer  — the canonical *enqueue.Enqueuer shared with the rest
//	             of the server (script handler, creatorflow), used to
//	             forward completed pipeline results to Velox workers.
//	client    — the *remoteengine.Client talking to the script service
//	             (may be nil when VELOX_REMOTE_ENGINE_URL is unset).
//
// The resolver creatorflow.Resolver must be wired by the composition root
// (see NewHandlersWithResolver). MasterURL is resolved from cfg at boot
// time — there is no per-request hostname discovery.
//
// Compose with WithJobsDeps to add the optional cancel deps.
func NewHandlers(cfg *config.Config, enqueuer *enqueue.Enqueuer, client *remoteengine.Client) *Handlers {
	return HandlersFactory(cfg, enqueuer, client, nil, nil, nil, nil)
}

// NewHandlersFull is the composition-root constructor that wires
// every optional dependency (jobs reader/writer for cancellation
// cleanup, worker command manager for per-worker cancel notifications).
// Pre-builds the resolver at construction time for the same
// performance reason as NewHandlers.
func NewHandlersFull(
	cfg *config.Config,
	enqueuer *enqueue.Enqueuer,
	client *remoteengine.Client,
	jobsReader jobs.Reader,
	jobsWriter jobs.Writer,
	cmdMgr *workers.CommandManager,
) *Handlers {
	return HandlersFactory(cfg, enqueuer, client, nil, jobsReader, jobsWriter, cmdMgr)
}

// NewHandlersWithResolver is the Blocco 5 preferred composition-root
// constructor: the caller supplies a pre-built Resolver so the
// single forward-completed path is explicitly shared with the
// CreatorForwardingRunner (the runner also accepts the same Resolver
// via SetResolver). This is what runServer should call once it has
// constructed the canonical Resolver in buildModules.
func NewHandlersWithResolver(
	cfg *config.Config,
	enqueuer *enqueue.Enqueuer,
	client *remoteengine.Client,
	resolver *creatorflow.Resolver,
	jobsReader jobs.Reader,
	jobsWriter jobs.Writer,
	cmdMgr *workers.CommandManager,
) *Handlers {
	return HandlersFactory(cfg, enqueuer, client, resolver, jobsReader, jobsWriter, cmdMgr)
}

// HandlersFactory is the shared construction helper for the three
// public constructors above. resolver may be nil; the Handlers panics
// at request time if forward-completed is reached without a wired
// resolver — composition-root callers must pass a non-nil resolver
// (see cmd/server/bootstrap_composition.go::appComponents where
// `creatorflow.NewResolver(cfg, m.Enqueuer, p.SQLite)` is unconditionally
// built before the pipeline handler is constructed).
func HandlersFactory(
	cfg *config.Config,
	enqueuer *enqueue.Enqueuer,
	client *remoteengine.Client,
	resolver *creatorflow.Resolver,
	jobsReader jobs.Reader,
	jobsWriter jobs.Writer,
	cmdMgr *workers.CommandManager,
) *Handlers {
	return &Handlers{
		cfg:      cfg,
		enqueuer: enqueuer,
		client:   client,
		resolver: resolver,
		jobs:     JobsDeps{Reader: jobsReader, Writer: jobsWriter, CmdMgr: cmdMgr},
	}
}

// WithJobsDeps returns a copy of h with the optional JobsDeps set.
// Returns the same handler (mutated) for fluent composition.
func (h *Handlers) WithJobsDeps(reader jobs.Reader, writer jobs.Writer, cmdMgr *workers.CommandManager) *Handlers {
	h.jobs = JobsDeps{Reader: reader, Writer: writer, CmdMgr: cmdMgr}
	return h
}

// WithStore enables the durable aggregate status projection.
func (h *Handlers) WithStore(db *store.SQLiteStore) *Handlers {
	h.store = db
	return h
}

// NewRemoteClientFromConfig constructs the canonical
// *remoteengine.Client from a *config.Config at composition root.
//
// PR-DI-pipeline: replaces the previous `pipeline.InitRemoteEngine`
// package-level mutator that built the client and parked it on the
// `remoteEngineClient` global. Returns nil when the remote engine
// is unconfigured (VELOX_REMOTE_ENGINE_URL empty) so the handler's
// IsConfigured checks flow naturally into a 503 response.
//
// Callers: cmd/server/router.go (production), tests (with a custom
// URL/TimeoutMS pointing at a stub httptest server).
func NewRemoteClientFromConfig(cfg *config.Config) *remoteengine.Client {
	if cfg == nil || cfg.Render.RemoteEngineURL == "" {
		return nil
	}
	return remoteengine.NewClient(remoteengine.Config{
		URL:       cfg.Render.RemoteEngineURL,
		Token:     cfg.Render.RemoteEngineToken,
		TimeoutMS: cfg.Render.RemoteEngineTimeoutMS,
		Retries:   cfg.Render.RemoteEngineRetries,
	})
}

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

// Generate handles POST /api/remote/pipeline/generate.
//
// Dependencies (enqueuer, remote engine client) are read from the
// receiver `h` rather than from package-level globals, so two
// concurrent tests or two pipelines mounted on different admin groups
// cannot collide through shared state.
func (h *Handlers) Generate() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.enqueuer == nil {
			pipelineLog("REQUEST: enqueuer not wired — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "pipeline enqueuer not wired (call NewHandlers(... enqueuer) at composition root)",
			})
			return
		}

		var reqPayload map[string]interface{}
		if err := c.ShouldBindJSON(&reqPayload); err != nil {
			pipelineLog("REQUEST: invalid JSON from %s: %v", c.ClientIP(), err)
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		topic := payload.FirstString(reqPayload, "topic", "title", "source_text")
		language := payload.FirstString(reqPayload, "language")
		style := payload.FirstString(reqPayload, "style")
		sceneCount := reqPayload["scene_count"]
		pipelineLog("REQUEST: received topic=%q language=%s style=%s scenes=%v", topic, language, style, sceneCount)

		if h.client == nil || !h.client.IsConfigured() {
			pipelineLog("REQUEST: remote engine NOT configured — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "remote engine not configured",
				"hint":  "set VELOX_REMOTE_ENGINE_URL",
			})
			return
		}

		pipelineLog("REMOTE: forwarding to %s/api/script/generate-with-images", h.cfg.Render.RemoteEngineURL)
		// Legacy Generate path: no pipeline_run row exists, so we extract
		// the idempotency key from the payload if the client sent one.
		idemKey, _ := reqPayload["idempotency_key"].(string)
		result, err := h.client.StartPipeline(c.Request.Context(), reqPayload, idemKey)
		if err != nil {
			pipelineLog("REMOTE: request FAILED: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Area 2: Parse the raw result into the typed DTO and derive
		// the worker payload. The remote result must NOT be passed
		// raw to the worker — it must first be converted to a typed
		// RemotePipelineResult.
		dto, _ := remoteengine.ParseRemotePipelineResult(result)
		workerPayload := dto.ToWorkerPayload()

		jobID, _ := workerPayload["job_id"].(string)
		status, _ := result["status"].(string)
		if jobID != "" {
			pipelineLog("REMOTE: response job_id=%s status=%s", jobID, status)
		} else {
			pipelineLog("REMOTE: response ok=%v status=%s", result["ok"], status)
		}

		response := gin.H{}
		for k, v := range result {
			response[k] = v
		}

		// Try synchronous forward if the remote already returned a
		// complete result.
		if enqueue.ShouldForwardPipelineResult(workerPayload) {
			pipelineLog("FORWARD: result complete — forwarding to Velox workers (sync)")
			if forwarded, forwardErr := h.forwardPipelineResultToWorker(c.Request.Context(), workerPayload); forwardErr != nil {
				if assetErr, ok := voiceoverassets.AsAcquisitionError(forwardErr); ok {
					c.JSON(http.StatusUnprocessableEntity, gin.H{
						"ok":          false,
						"code":        assetErr.Code,
						"field":       assetErr.Field,
						"message":     assetErr.Message,
						"source_type": assetErr.SourceType,
					})
					return
				}
				pipelineLog("FORWARD: FAILED: %v", forwardErr)
				response["worker_forwarded"] = false
				response["worker_forward_error"] = forwardErr.Error()
			} else {
				workerJobID, _ := forwarded["job_id"].(string)
				pipelineLog("FORWARD: SUCCESS job_id=%s", workerJobID)
				response["worker_forwarded"] = true
				response["worker_forward_result"] = forwarded
			}
		} else if jobID != "" && !isTerminalStatus(status) {
			if h.resolver == nil || !h.resolver.HasDBAccess() {
				pipelineLog("FORWARD: durable resolver unavailable for remote job=%s", jobID)
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"ok":    false,
					"error": "durable forwarding is not configured",
				})
				return
			}

			targetExecutor := firstStringResolver(workerPayload, "executor_id", "pipeline_id")
			forwarding, persistErr := h.resolver.PersistPendingRemoteForwarding(
				c.Request.Context(), "remote_engine", jobID, targetExecutor,
			)
			if persistErr != nil {
				pipelineLog("FORWARD: failed to persist remote job=%s: %v", jobID, persistErr)
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": persistErr.Error()})
				return
			}

			pipelineLog("FORWARD: persisted remote job=%s forwarding_id=%s status=%s",
				jobID, forwarding.ForwardingID, forwarding.Status)
			response["ok"] = true
			response["remote_job_id"] = jobID
			response["forwarding_id"] = forwarding.ForwardingID
			response["forwarding_status"] = forwarding.Status
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "remote result is pending; durable forwarding runner will resume it"
			c.JSON(http.StatusAccepted, response)
			return
		} else if jobID != "" {
			pipelineLog("FORWARD: result NOT complete for job %s (status=%s) — missing scenes/voiceover", jobID, status)
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete enough for worker handoff — missing scenes/voiceover"
		}

		c.JSON(http.StatusOK, response)
	}
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
