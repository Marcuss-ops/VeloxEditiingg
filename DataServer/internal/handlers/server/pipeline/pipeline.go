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
//	routes.go          — h.RegisterRoutes() Gin mount surface for all
//	                     pipeline endpoints.
//	forwarding.go      — forwardPipelineResultToWorker + firstStringResolver.
//	pipeline.go (this) — package doc + package-internal [PIPELINE]
//	                     logger (pipelineLog). Keeping the logger
//	                     here means every other sibling file can
//	                     call it via package-internal visibility
//	                     without owning the helper itself.
//	pipeline_create.go     — CreatePipelineRun handler + buildRemotePayload
//	                         (request-shape mapper).
//	pipeline_run_status.go — PipelineRunStatus handler.
//	pipeline_run_actions.go — lookupPipelineRun + CancelPipelineRun +
//	                         RetryPipelineRun + PipelineRunTimeline +
//	                         PipelineRunArtifacts + PipelineRunDeliveries.
//	pipeline_run_validator.go — ValidateCreateRequest + ValidationError.
//	pipeline_scripts.go    — ScriptSimple / ScriptBatch.
package pipeline

import (
	"log"
)

// pipelineLog is the package-internal structured-log helper. Kept
// package-level (not a method on Handlers) so sibling files can call
// it without ceremony, and so diagnostic context such as "[PIPELINE]"
// remains uniform across all pipeline-installed routes.
func pipelineLog(format string, args ...interface{}) {
	log.Printf("[PIPELINE] "+format, args...)
}
