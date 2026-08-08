package pipeline

import (
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/credentials"
	"velox-server/internal/inputsecurity"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	targetpublishing "velox-server/internal/publishing"
	"velox-server/internal/remoteengine"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
	"velox-server/internal/workers"
)

// Handlers carries every dependency the pipeline HTTP layer needs.
// Resolver and socialClient are built once at construction so target discovery
// and job intake do not create transport clients per request.
type Handlers struct {
	cfg             *config.Config
	enqueuer        *enqueue.Enqueuer
	client          *remoteengine.Client
	resolver        *creatorflow.Resolver
	submission      *creatorflow.JobSubmissionService
	socialClient    *socialclient.Client
	targetResolver  *targetpublishing.TargetResolver
	jobs            JobsDeps
	store           *store.SQLiteStore
	intakeSink      CreatorIntakeSink
	assetService    *voiceoverassets.AssetService
	inputPolicy     *inputsecurity.Policy
	credentialVault *credentials.Vault
}

// JobsDeps bundles the optional jobs-layer dependencies used by
// PipelineCancel: list-all for hit-detection, delete for cleanup,
// command manager for per-worker cancel notifications.
type JobsDeps struct {
	Reader     jobs.Reader
	Writer     jobs.Writer
	CmdMgr     *workers.CommandManager
	TaskReader taskgraph.Reader
}

func NewHandlers(cfg *config.Config, enqueuer *enqueue.Enqueuer, client *remoteengine.Client) *Handlers {
	return HandlersFactory(cfg, enqueuer, client, nil, nil, nil, nil)
}

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

// HandlersFactory is the shared construction helper. The Social API client is
// created from the typed Config snapshot once. Empty
// configuration remains fail-closed for the internal publishing resolver;
// public catalog discovery is retired because InstaEdit owns that catalog.
func HandlersFactory(
	cfg *config.Config,
	enqueuer *enqueue.Enqueuer,
	client *remoteengine.Client,
	resolver *creatorflow.Resolver,
	jobsReader jobs.Reader,
	jobsWriter jobs.Writer,
	cmdMgr *workers.CommandManager,
) *Handlers {
	h := &Handlers{
		cfg:          cfg,
		enqueuer:     enqueuer,
		client:       client,
		resolver:     resolver,
		submission:   creatorflow.NewJobSubmissionService(resolver),
		socialClient: socialclient.New(socialclient.ConfigFromRuntime(cfg.Runtime.Social)),
		jobs:         JobsDeps{Reader: jobsReader, Writer: jobsWriter, CmdMgr: cmdMgr},
	}
	h.targetResolver = targetpublishing.NewTargetResolver(h.socialClient, h.store)
	return h
}

func (h *Handlers) WithJobsDeps(reader jobs.Reader, writer jobs.Writer, cmdMgr *workers.CommandManager) *Handlers {
	h.jobs.Reader = reader
	h.jobs.Writer = writer
	h.jobs.CmdMgr = cmdMgr
	return h
}

func (h *Handlers) WithTaskReader(reader taskgraph.Reader) *Handlers {
	h.jobs.TaskReader = reader
	return h
}

func (h *Handlers) WithStore(db *store.SQLiteStore) *Handlers {
	h.store = db
	if h.cfg != nil {
		if keys, err := credentials.LoadKeyring(h.cfg.Runtime.Credentials); err == nil {
			h.credentialVault, _ = credentials.NewVault(db, keys)
		}
	}
	h.targetResolver = targetpublishing.NewTargetResolver(h.socialClient, h.store)
	return h
}

// WithSocialClient overrides the canonical Social API client. Production uses
// the factory default; tests inject an httptest-backed client.
func (h *Handlers) WithSocialClient(client *socialclient.Client) *Handlers {
	h.socialClient = client
	h.targetResolver = targetpublishing.NewTargetResolver(h.socialClient, h.store)
	return h
}

func (h *Handlers) WithIntakeSink(sink CreatorIntakeSink) *Handlers {
	h.intakeSink = sink
	return h
}

func (h *Handlers) WithAssetService(svc *voiceoverassets.AssetService) *Handlers {
	h.assetService = svc
	return h
}

// WithInputSecurityPolicy is intended for hermetic tests and controlled
// composition roots. Production handlers use the fail-closed default policy.
func (h *Handlers) WithInputSecurityPolicy(policy inputsecurity.Policy) *Handlers {
	h.inputPolicy = &policy
	return h
}

// NewRemoteClientFromConfig constructs the canonical remote-engine client.
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
