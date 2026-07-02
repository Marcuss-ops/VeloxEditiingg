// Package creatorflow / resolver.go
//
// Resolver is the SINGLE authoritative entry point for converting a
// completed remote-creator result into a Velox Job.
//
// Why a single resolver?
//
// Prior to this cutover there were two divergent forward-completed paths:
//
//   - Handler (sync): HTTP POST /api/remote/pipeline/generate receives a
//     complete result, calls creatorflow.Service.ForwardCompleted which
//     enqueues via enqueuer.Enqueue. NO creator_forwardings row is
//     written; no audit trail is generated.
//
//   - Runner (async/durable): CreatorForwardingRunner claims a PENDING
//     creator_forwardings row, polls the remote creator to completion, and
//     then calls dbStore.AtomicForwardAndEnqueue (a multi-table
//     InsertJob+InsertTask+TransitionToFORWARDED in one SQLite tx).
//
// Both paths compute the same job_id (derived deterministically from the
// forwarding key via enqueue.DeriveForwardingJobID) so the system
// converged on the Job identity. What it DID NOT converge on:
//
//   - Whether a creator_forwardings row was written at all (yes on the
//     async path, no on the sync path).
//   - Whether the Job creation was wrapped in the multi-table CAS that
//     prevents a stuck FORWARDING row after a crash.
//
// Blocco 5 of the Verdetto (P1 #11) resolves both divergences by making
// Resolver.Resolve the single entry point for both callers. The handler
// path now INSERTs a PENDING creator_forwardings row, promotes it to
// READY_TO_FORWARD, and runs the same atomic CAS the runner uses. The
// runner path keeps its existing lease-CAS promotion (the existing
// MarkCreatorForwardingReadyToForward transition still applies for lease
// holders) and uses Resolver only for the post-promotion atomic write.
//
// Public API:
//
//   - NewResolver(cfg, enqueuer, dbStore) → *Resolver
//   - NewResolverMinimal(enqueuer, dbStore) → *Resolver
//   - (*Resolver).Resolve(ctx, ResolveRequest) → (*ResolveOutput, error)
//
// NewResolver pulls dataDir + videosDir + masterURL from cfg so the URL
// rewriting step (BuildSceneImagePayloadForMaster) runs with the per-
// process values resolved at boot time. NewResolverMinimal is the
// in-runner fallback when the cfg isn't available; URL rewriting is
// skipped (the caller already had a complete remote result by then).
package creatorflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
	"velox-server/internal/store"
)

// Resolver bundles the canonical dependencies for Resolve. Holding them on
// a struct (not passing them per-call) means callers cannot accidentally
// pass a stale dbStore or the wrong enqueuer — the Resolver is wired
// once at composition root and reused.
type Resolver struct {
	enqueuer  *enqueue.Enqueuer
	dbStore   *store.SQLiteStore
	dataDir   string
	videosDir string
	masterURL string
}

// NewResolver is the canonical constructor for the handler-side Resolver.
// It pulls dataDir/videosDir/masterURL from the supplied config so the
// URL rewriting step (BuildSceneImagePayloadForMaster) uses per-process
// values resolved at boot time.
//
// Returns nil if cfg, enqueuer, or dbStore is missing — callers must
// nil-check before calling Resolve (Resolve itself also returns a
// typed error for missing dependencies).
func NewResolver(cfg *config.Config, enqueuer *enqueue.Enqueuer, dbStore *store.SQLiteStore) *Resolver {
	if cfg == nil || enqueuer == nil || dbStore == nil {
		return nil
	}
	return &Resolver{
		enqueuer:  enqueuer,
		dbStore:   dbStore,
		dataDir:   strings.TrimSpace(cfg.Runtime.DataDir),
		videosDir: strings.TrimSpace(cfg.Runtime.VideosDir),
		masterURL: resolvePublicMasterURL(cfg),
	}
}

// NewResolverMinimal constructs a Resolver without a *config.Config. It
// is the in-runner fallback — the runner captures the remote engine's
// completed payload directly and doesn't need URL rewriting (the remote
// engine already packaged scene-image URLs with their canonical refs).
//
// Any dataDir/videosDir/masterURL fields remain empty, which causes
// Resolve to skip BuildSceneImagePayloadForMaster.
func NewResolverMinimal(enqueuer *enqueue.Enqueuer, dbStore *store.SQLiteStore) *Resolver {
	if enqueuer == nil || dbStore == nil {
		return nil
	}
	return &Resolver{
		enqueuer: enqueuer,
		dbStore:  dbStore,
	}
}

// NewResolverFromDeps is the explicit-fields constructor. Useful for
// composition roots that have access to the data-dir/master-URL triple
// but not the full *config.Config. Same as NewResolver but takes the
// fields directly. The dataDir, videosDir, masterURL triple drives
// BuildSceneImagePayloadForMaster, so callers that want URL rewriting
// must supply non-empty dataDir + masterURL.
func NewResolverFromDeps(enqueuer *enqueue.Enqueuer, dbStore *store.SQLiteStore, dataDir, videosDir, masterURL string) *Resolver {
	if enqueuer == nil || dbStore == nil {
		return nil
	}
	return &Resolver{
		enqueuer:  enqueuer,
		dbStore:   dbStore,
		dataDir:   strings.TrimSpace(dataDir),
		videosDir: strings.TrimSpace(videosDir),
		masterURL: strings.TrimSpace(masterURL),
	}
}

// HasDBAccess returns true when the resolver can write to the
// creator_forwardings table. Callers (e.g. the pipeline handler) use
// this to decide whether to delegate to Resolver.Resolve or fall back
// to the legacy forwarder path. A resolver built via
// NewResolverFromDeps(_, nil, _, _, _) is a forwarder-only construct
// (deprecated; NewResolverFromDeps now returns nil in that case) but
// the guard remains as a defensive check for callers that constructed
// the struct directly.
func (r *Resolver) HasDBAccess() bool {
	return r != nil && r.dbStore != nil
}

// ResolveRequest is the typed input for Resolver.Resolve.
//
//   - ForwardingID: optional. When set (the runner path), the resolver
//     treats req.ForwardingID as the existing creator_forwardings row
//     from the runner's lease and UPDATES its payload + source_status
//     before the atomic enqueue. When empty (the handler sync path),
//     the resolver INSERTs a fresh PENDING creator_forwardings row and
//     immediately promotes it to READY_TO_FORWARD via the leaseless
//     MarkCreatorForwardingReadySync transition.
//   - SourceProvider: e.g. "remote_engine". Required.
//   - SourceJobID: the remote engine's job id. Required.
//   - TargetExecutorID: the executor that the Velox Job should route
//     to. Optional; defaults to "scene.composite.v1".
//   - Payload: the raw remote-engine response map. Required; must pass
//     enqueue.ShouldForwardPipelineResult or Resolve returns (nil, nil)
//     ("result not complete — caller should keep polling").
type ResolveRequest struct {
	ForwardingID     string
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	Payload          map[string]interface{}
}

// ResolveOutput is what every caller receives. JobID and ForwardingID
// are guaranteed to be the SAME across the handler and runner paths for
// the same (source_provider, source_job_id, target_executor_id) input.
// Response is the HTTP-flavored envelope (job_id, status, ok, …) that
// the handler returns to the client; the runner ignores it.
type ResolveOutput struct {
	JobID        string
	ForwardingID string
	Response     map[string]interface{}
}

// ErrResolverNotComplete is the sentinel for "payload not complete —
// caller should keep polling". Returned via (*ResolveOutput, nil) plus
// a nil error; the caller decides whether nil-output means "early exit"
// (handler: respond 202 polling, runner: mark retry-wait).
var ErrResolverNotComplete = fmt.Errorf("creatorflow: Resolve: payload is not complete enough to forward")

// Resolve returns the canonical (job_id, forwarding_id) pair for the
// input. Implementation invariants:
//
//  1. ShouldForwardPipelineResult guard. Reject the request if the
//     payload is not complete; return (nil, ErrResolverNotComplete).
//  2. Deterministic IDs.
//     - forwarding_key  = routing.FormatForwardingKey(source_provider,
//       source_job_id, target_executor_id).
//     - job_id          = enqueue.DeriveForwardingJobID(forwarding_key).
//     The UNIQUE index on creator_forwardings(source_provider,
//     source_job_id, target_executor_id) makes the forwarding_id lookup
//     idempotent across handlers, runners, and retries.
//  3. Idempotency fast-path. If the Job already exists, return
//     immediately without further writes. Both the handler (sync retry)
//     and the runner (lease reclaimed, common row) hit this path safely.
//  4. URL rewriting. workerPayload is rewritten via
//     BuildSceneImagePayloadForMaster so scene-image references point
//     to the public master URL. Skipped when dataDir or masterURL is
//     empty (test harness path + in-runner path).
//  5. Forwarding-row promotion.
//     - Sync (req.ForwardingID == ""): INSERT a PENDING row with a
//       fresh UUID, then MarkCreatorForwardingReadySync promotes it
//       to READY_TO_FORWARD. Concurrent calls converge on one row via
//       the UNIQUE index.
//     - Runner (req.ForwardingID != ""): UpsertCreatorForwardingPayload
//       stamps payload + source_status onto the leasable PENDING/POLLING.
//     Both paths end in READY_TO_FORWARD so AtomicForwardAndEnqueue can
//     take over.
//  6. Atomic commit. dbStore.AtomicForwardAndEnqueue packs (READY_TO_FORWARD
//     → FORWARDING → INSERT job/task/task_spec → FORWARDING → FORWARDED)
//     into a single SQLite tx. A crash mid-flight rolls the whole stack
//     back; the next runner tick re-claims the PENDING/READY row and
//     re-runs this method.
//
// The (resolver, request) tuple is intentionally not coupled to the
// pass-through signature of the legacy Service.ForwardCompleted — the
// old free function was an ad-hoc compatibility shim that bypassed
// master-URL rewriting. The Resolver applies that step exactly once
// for every caller.
func (r *Resolver) Resolve(ctx context.Context, req ResolveRequest) (*ResolveOutput, error) {
	if r == nil || r.enqueuer == nil || r.dbStore == nil {
		return nil, fmt.Errorf("creatorflow: Resolve: resolver dependencies missing")
	}
	if req.Payload == nil {
		return nil, fmt.Errorf("creatorflow: Resolve: payload is required")
	}
	if req.SourceProvider == "" || req.SourceJobID == "" {
		return nil, fmt.Errorf("creatorflow: Resolve: source_provider and source_job_id are required")
	}
	if !enqueue.ShouldForwardPipelineResult(req.Payload) {
		return nil, ErrResolverNotComplete
	}

	targetExecutor := req.TargetExecutorID
	if targetExecutor == "" {
		targetExecutor = "scene.composite.v1"
	}
	fwdKey := routing.FormatForwardingKey(req.SourceProvider, req.SourceJobID, targetExecutor)
	jobID := enqueue.DeriveForwardingJobID(fwdKey.String())

	// Stamp the forwarding key onto the payload so downstream
	// normalizeSceneVideoPayload + enqueuer.PrepareJobAndTask can carry
	// it into the compiled TaskSpec (the deterministic job_id is
	// re-derived inside PrepareJobAndTask from the payload's
	// _internal_forwarding_key). This is the same injection the legacy
	// Service.ForwardCompleted performed just before Enqueue.
	req.Payload[routing.KeyForwardingKey] = fwdKey.String()

	// 3. Idempotency fast-path. If the Job already exists, return
	// immediately so duplicate webhooks + retry storms don't write
	// twice. We still surface the forwarding_id when available so the
	// caller can audit-link to the row.
	if existing, getErr := r.enqueuer.Jobs.Get(ctx, jobID); getErr == nil && existing != nil && existing.ID == jobID {
		forwardingID := req.ForwardingID
		if forwardingID == "" {
			if cf, lookupErr := r.dbStore.GetCreatorForwardingBySource(ctx, req.SourceProvider, req.SourceJobID, targetExecutor); lookupErr == nil && cf != nil {
				forwardingID = cf.ForwardingID
			}
		}
		return &ResolveOutput{
			JobID:        existing.ID,
			ForwardingID: forwardingID,
			Response:     buildIdempotentResolveResponse(existing, req.Payload),
		}, nil
	}

	// 4. Build + rewrite worker payload. Skip rewriting when the
	// resolver was constructed without dataDir+masterURL (in-runner
	// path; the remote engine already produced a complete result).
	workerPayload, err := enqueue.BuildPipelinePayload(req.Payload)
	if err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve build worker payload: %w", err)
	}
	if r.dataDir != "" && r.masterURL != "" {
		workerPayload, err = enqueue.BuildSceneImagePayloadForMaster(workerPayload, r.dataDir, r.videosDir, r.masterURL)
		if err != nil {
			return nil, fmt.Errorf("creatorflow: Resolve rewrite master URL: %w", err)
		}
	}
	// Re-inject the forwarding key into the rewritten payload — both
	// BuildPipelinePayload and BuildSceneImagePayloadForMaster produce
	// fresh maps that drop the originally-injected key. This is the
	// same step the legacy Service.ForwardCompleted performed.
	fwdKey.InjectIntoPayload(workerPayload)

	// 5. Promote the forwarding row to READY_TO_FORWARD.
	forwardingID, err := r.ensureReadyForwarding(ctx, req, targetExecutor, workerPayload)
	if err != nil {
		return nil, err
	}

	// 6. Compile Job + TaskSpec.
	job, spec, priority, err := r.enqueuer.PrepareJobAndTask(ctx, workerPayload, costmodel.DefaultRequirements())
	if err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve prepare job/task: %w", err)
	}

	// 7. Atomic FORWARDED transition.
	if err := r.dbStore.AtomicForwardAndEnqueue(ctx, forwardingID, job, spec, priority); err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve atomic: %w", err)
	}

	return &ResolveOutput{
		JobID:        job.ID,
		ForwardingID: forwardingID,
		Response:     buildFreshResolveResponse(job, workerPayload),
	}, nil
}

// ensureReadyForwarding either
//   (a) reuses the existing ForwardingID from the request (runner path) and
//       stamps payload + source_status via the leasable guard, or
//   (b) INSERTs a fresh PENDING row and promotes it to READY_TO_FORWARD via
//       the leaseless MarkCreatorForwardingReadySync (handler sync path).
//
// The payload is JSON-serialized here so both paths pass the same shape
// into the atomic write. A marshal failure is treated as a fatal input
// error (the caller decides whether to surface it to the user).
func (r *Resolver) ensureReadyForwarding(ctx context.Context, req ResolveRequest, targetExecutor string, workerPayload map[string]interface{}) (string, error) {
	payloadJSON, payloadSHA256 := resolverMarshalPayload(workerPayload)
	if payloadJSON == "" && payloadSHA256 == "" {
		return "", fmt.Errorf("creatorflow: Resolve: worker payload is not JSON-serializable")
	}

	// (a) Runner path.
	if req.ForwardingID != "" {
		if err := r.dbStore.UpsertCreatorForwardingPayload(ctx, req.ForwardingID, payloadJSON, payloadSHA256); err != nil {
			return "", fmt.Errorf("creatorflow: Resolve upsert payload: %w", err)
		}
		return req.ForwardingID, nil
	}

	// (b) Handler sync path: INSERT PENDING, then promote.
	now := time.Now().UTC().Format(time.RFC3339)
	cf := &store.CreatorForwarding{
		ForwardingID:     "cf_" + uuid.NewString(),
		SourceProvider:   req.SourceProvider,
		SourceJobID:      req.SourceJobID,
		TargetExecutorID: targetExecutor,
		PayloadJSON:      payloadJSON,
		PayloadSHA256:    payloadSHA256,
		Status:           string(store.CFStatusPending),
		AttemptCount:     0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	inserted, err := r.dbStore.InsertCreatorForwarding(ctx, cf)
	if err != nil {
		return "", fmt.Errorf("creatorflow: Resolve insert forwarding: %w", err)
	}
	if inserted == nil || inserted.Forwarding == nil || inserted.Forwarding.ForwardingID == "" {
		return "", fmt.Errorf("creatorflow: Resolve: insert returned empty row")
	}

	// Promote PENDING → READY_TO_FORWARD via the leaseless sync method.
	if err := r.dbStore.MarkCreatorForwardingReadySync(ctx, inserted.Forwarding.ForwardingID, payloadJSON, payloadSHA256); err != nil {
		return "", fmt.Errorf("creatorflow: Resolve mark READY_TO_FORWARD: %w", err)
	}
	log.Printf("[CREATORFLOW] sync handler path: promoted %s to READY_TO_FORWARD (source=%s source_job=%s target_executor=%s)",
		inserted.Forwarding.ForwardingID, req.SourceProvider, req.SourceJobID, targetExecutor)
	return inserted.Forwarding.ForwardingID, nil
}

// buildIdempotentResolveResponse is the response body for the
// idempotency fast-path (the Job already exists). The runner path
// typically hits this on a duplicate poll + lease reclaim; the handler
// path hits it on a duplicate webhook.
func buildIdempotentResolveResponse(existing *jobs.Job, payload map[string]interface{}) map[string]interface{} {
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
func buildFreshResolveResponse(job *jobs.Job, payload map[string]interface{}) map[string]interface{} {
	resp := map[string]interface{}{
		"ok":                true,
		"job_id":            job.ID,
		"created":           true,
		"status":            jobs.StatusPending,
		"enqueue_confirmed": true,
		"job_type":          "process_video",
	}
	if runID := strings.TrimSpace(job.RunID); runID != "" {
		resp["job_run_id"] = runID
		resp["run_id"] = runID
	}
	return resp
}

// resolverMarshalPayload serializes a worker payload map to canonical
// JSON + SHA-256. Empty inputs yield a literal "{}" payload — the
// caller decides whether empty sha is a fatal input error. Mirrors the
// runner's marshalPayload semantics so the two paths produce identical
// payload_json/payload_sha256 bytes for the same input map.
func resolverMarshalPayload(result map[string]interface{}) (payloadJSON, payloadSHA256 string) {
	if result == nil {
		raw := []byte("{}")
		return string(raw), sha256HexResolver(raw)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return "", ""
	}
	return string(raw), sha256HexResolver(raw)
}

func sha256HexResolver(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
