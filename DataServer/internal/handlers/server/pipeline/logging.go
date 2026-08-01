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
// Step 9: the canonical `pipeline.go` was deleted once every unique
// symbol it had owned (RegisterRoutes, Generate,
// forwardPipelineResultToWorker, firstStringResolver) was extracted
// into single-responsibility sibling files. The package doc and the
// package-internal [PIPELINE] logger now live in `logging.go`, which
// is the natural home for cross-file helpers used by every handler in
// the package.
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
//	logging.go (this)  — package doc + package-internal [PIPELINE]
//	                     logger (pipelineLog).
//	pipeline_create.go     — CreatePipelineRun handler + buildRemotePayload
//	                         (request-shape mapper).
//	pipeline_run_status.go — PipelineRunStatus handler.
//	pipeline_run_actions.go    — lookupPipelineRun + CancelPipelineRun +
//	                             errPipelineRunNotFound (shared run plumbing).
//	pipeline_run_submit.go     — RetryPipelineRun (submit phase).
//	pipeline_run_progress.go   — PipelineRunTimeline + eventTimestamp
//	                             (advancement phase).
//	pipeline_run_completion.go — PipelineRunArtifacts + PipelineRunDeliveries
//	                             (completion phase).
//	pipeline_run_validator.go — ValidateCreateRequest + ValidationError.
//	pipeline_scripts.go    — ScriptSimple / ScriptBatch.
//
// Status (`Status()`) and Cancel (`Cancel()`) handlers as well as
// isTerminalStatus live in `pipeline_lifecycle.go` until that file is
// dissolved into status.go + cancel.go.
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
)

// pipelineLog is the package-internal structured-log helper. Kept
// package-level (not a method on Handlers) so sibling files can call
// it without ceremony, and so diagnostic context such as "[PIPELINE]"
// remains uniform across all pipeline-installed routes.
func pipelineLog(format string, args ...interface{}) {
	log.Printf("[PIPELINE] "+format, args...)
}

// logHashShort returns the first 12 hex characters of the SHA-256
// digest of a client-supplied identifier (typically an idempotency
// token or the source-side job ID). This is the canonical log-hygiene
// helper used by both SubmitJob and CreatorPush whenever a pipelineLog
// site would otherwise emit a raw user-controlled value.
//
// Why we hash:
//
//  1. The raw value can carry user-supplied identifiers: email
//     addresses, customer IDs, accidental bearer tokens, anything
//     the client's caller chooses to put in. We do not want those
//     in journald / Loki / CloudWatch output.
//
//  2. The value can carry unusual Unicode that breaks terminal log
//     printers or even pipe-grep tooling that assumes printable
//     ASCII. A 12-hex-char ASCII hash is always safe to print.
//
//  3. The full value remains the source-of-truth dedup identifier
//     in the forwarding row (cf.source_job_id) and in the database.
//     Operators who need to look up a specific request can run
//     `SELECT * FROM creator_forwardings WHERE source_job_id = ?`
//     — they do not need the raw value in stdout.
//
// The 12-hex-char prefix is the same compact form used by
// creatorflow.shaShort in the payload-hash work, so log operators
// see a consistent format across both log sources.
//
// Truncating a SHA-256 to 12 hex chars leaves 48 bits of entropy —
// ample to distinguish concurrent jobs in practice but NOT a
// security guarantee against brute force. It is purely an
// operator-UX choice.
//
// Empty input is permitted and produces a stable hash value, NOT a
// panic. Callers can safely use the helper at the top of a request
// flow before they have validated the key's presence.
func logHashShort(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}
