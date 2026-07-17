package pipeline

import (
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
