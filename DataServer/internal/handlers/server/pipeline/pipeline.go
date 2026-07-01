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
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"velox-shared/payload"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/workers"
)

// Handlers carries every dependency the pipeline HTTP layer needs.
//
// The struct carries the mandatory remote params (cfg, enqueuer, client,
// forwarder) plus optional cancel-side dependencies bundled in JobsDeps.
// forwarder is the authoritative creatorflow.Service used to satisfy the
// single canonical forward-completed path. It is built ONCE at
// construction time (so resolvePublicMasterURL → detectPublicMasterURL
// → `hostname -I` does NOT run per request) and reused for every poll
// completion and synchronous forward.
type Handlers struct {
	cfg      *config.Config
	enqueuer *enqueue.Enqueuer
	client   *remoteengine.Client
	forwarder *creatorflow.Service
	jobs     JobsDeps
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
// The forwarder creatorflow.Service is pre-built here so the master-URL
// resolution (which shells out to `hostname -I` when no VELOX_MASTER_URL
// is set) happens once per process, not once per request.
//
// Compose with WithJobsDeps to add the optional cancel deps.
func NewHandlers(cfg *config.Config, enqueuer *enqueue.Enqueuer, client *remoteengine.Client) *Handlers {
	return &Handlers{
		cfg:      cfg,
		enqueuer: enqueuer,
		client:   client,
		forwarder: creatorflow.NewForwarder(cfg, enqueuer),
	}
}

// NewHandlersFull is the composition-root constructor that wires
// every optional dependency (jobs reader/writer for cancellation
// cleanup, worker command manager for per-worker cancel notifications).
// Pre-builds the forwarder creatorflow.Service at construction time
// for the same performance reason as NewHandlers.
func NewHandlersFull(
	cfg *config.Config,
	enqueuer *enqueue.Enqueuer,
	client *remoteengine.Client,
	jobsReader jobs.Reader,
	jobsWriter jobs.Writer,
	cmdMgr *workers.CommandManager,
) *Handlers {
	return &Handlers{
		cfg:      cfg,
		enqueuer: enqueuer,
		client:   client,
		forwarder: creatorflow.NewForwarder(cfg, enqueuer),
		jobs:     JobsDeps{Reader: jobsReader, Writer: jobsWriter, CmdMgr: cmdMgr},
	}
}

// WithJobsDeps returns a copy of h with the optional JobsDeps set.
// Returns the same handler (mutated) for fluent composition.
func (h *Handlers) WithJobsDeps(reader jobs.Reader, writer jobs.Writer, cmdMgr *workers.CommandManager) *Handlers {
	h.jobs = JobsDeps{Reader: reader, Writer: writer, CmdMgr: cmdMgr}
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
		result, err := h.client.StartPipeline(c.Request.Context(), reqPayload)
		if err != nil {
			pipelineLog("REMOTE: request FAILED: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
			return
		}

		jobID, _ := result["job_id"].(string)
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
		if enqueue.ShouldForwardPipelineResult(result) {
			pipelineLog("FORWARD: result complete — forwarding to Velox workers (sync)")
			if forwarded, forwardErr := h.forwardPipelineResultToWorker(c.Request.Context(), result); forwardErr != nil {
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
			// Async: spin up background polling. h.startPolling reads
			// client/enqueuer from the receiver, so the goroutine
			// captures the handler instance, not package state.
			pollInterval := h.cfg.Render.RemoteEnginePollInterval
			if pollInterval <= 0 {
				pollInterval = 30
			}
			maxPolls := (1800 + pollInterval - 1) / pollInterval
			if maxPolls > 120 {
				maxPolls = 120
			}
			pipelineLog("POLL: starting background polling job_id=%s status=%s interval=%ds max_polls=%d (~%d min timeout)",
				jobID, status, pollInterval, maxPolls, pollInterval*maxPolls/60)
			h.startPolling(jobID, pollInterval)
			response["polling_enabled"] = true
			response["poll_interval_sec"] = pollInterval
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete yet — background polling started"
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
// enqueues it through the authoritative creatorflow.Service.ForwardCompleted
// entry point (no free-function form). The Service was pre-built once
// in NewHandlers/NewHandlersFull so resolvePublicMasterURL +
// detectPublicMasterURL + `hostname -I` only execute at boot, not per
// request.
func (h *Handlers) forwardPipelineResultToWorker(ctx context.Context, result map[string]interface{}) (map[string]interface{}, error) {
	pipelineLog("FORWARD: building worker payload...")
	enqueued, err := h.forwarder.ForwardCompleted(ctx, result)
	if err != nil {
		pipelineLog("FORWARD: ForwardCompleted FAILED: %v", err)
		return nil, err
	}
	if enqueued == nil {
		return nil, fmt.Errorf("forward completed result returned no enqueue response")
	}

	jobPayload, buildErr := enqueue.BuildPipelinePayload(result)
	if buildErr == nil {
		payloadJSON, _ := json.Marshal(jobPayload)
		if len(payloadJSON) > 500 {
			pipelineLog("FORWARD: payload built size=%d bytes title=%s scenes=%v",
				len(payloadJSON), jobPayload["title"], jobPayload["scene_count"])
		} else {
			pipelineLog("FORWARD: payload built: %s", string(payloadJSON))
		}
	}
	pipelineLog("FORWARD: enqueued to Velox queue job_id=%v", enqueued["job_id"])
	return enqueued, nil
}
