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
	"fmt"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
	"velox-server/internal/store"
)

// Resolver bundles the canonical dependencies for Resolve. Holding them on
// a struct (not passing them per-call) means callers cannot accidentally
// pass a stale dbStore or the wrong enqueuer — the Resolver is wired
// once at composition root and reused.
//
// Blocco 4 del Verdetto: ForwardingRepository + JobLookup interfaces are
// declared in resolver_repositories.go. The Resolver now depends on these
// interfaces rather than the concrete *store.SQLiteStore, which improves
// testability and makes the dependency contract explicit.
type Resolver struct {
	enqueuer    *enqueue.Enqueuer
	jobLookup   JobLookup
	forwardRepo ForwardingRepository
	dataDir     string
	videosDir   string
	masterURL   string
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
		enqueuer:    enqueuer,
		jobLookup:   enqueuer.Jobs,
		forwardRepo: dbStore,
		dataDir:     strings.TrimSpace(cfg.Runtime.DataDir),
		videosDir:   strings.TrimSpace(cfg.Runtime.VideosDir),
		masterURL:   resolvePublicMasterURL(cfg),
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
		enqueuer:    enqueuer,
		jobLookup:   enqueuer.Jobs,
		forwardRepo: dbStore,
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
		enqueuer:    enqueuer,
		jobLookup:   enqueuer.Jobs,
		forwardRepo: dbStore,
		dataDir:     strings.TrimSpace(dataDir),
		videosDir:   strings.TrimSpace(videosDir),
		masterURL:   strings.TrimSpace(masterURL),
	}
}

// HasDBAccess returns true when the resolver can write to the
// creator_forwardings table. Callers (e.g. the pipeline handler) use
// this to decide whether to delegate to Resolver.Resolve or fall back
// to the legacy forwarder path. A resolver built via
// NewResolverFromDeps(_, nil, _, _, _) is a forwarder-only construct
// (deprecated; NewResolverFromDeps now returns nil in that case) but
// the guard remains as a defensive check for callers that constructed
// the struct directly. The actual dependency is the ForwardingRepository
// interface, so any repository implementation satisfies this guard.
func (r *Resolver) HasDBAccess() bool {
	return r != nil && r.forwardRepo != nil
}

// Resolve returns the canonical (job_id, forwarding_id) pair for the
// input. Implementation invariants:
//
//  1. ShouldForwardPipelineResult guard. Reject the request if the
//     payload is not complete; return (nil, ErrResolverNotComplete).
//  2. Deterministic IDs.
//     - forwarding_key  = routing.FormatForwardingKey(source_provider,
//     source_job_id, target_executor_id).
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
//     fresh UUID, then MarkCreatorForwardingReadySync promotes it
//     to READY_TO_FORWARD. Concurrent calls converge on one row via
//     the UNIQUE index.
//     - Runner (req.ForwardingID != ""): UpsertCreatorForwardingPayload
//     stamps payload + source_status onto the leasable PENDING/POLLING.
//     Both paths end in READY_TO_FORWARD so AtomicForwardAndEnqueue can
//     take over.
//  6. Atomic commit. forwardRepo.AtomicForwardAndEnqueue packs
//     (READY_TO_FORWARD → FORWARDING → INSERT job/task/task_spec →
//     FORWARDING → FORWARDED) into a single SQLite tx. A crash mid-flight
//     rolls the whole stack back; the next runner tick re-claims the
//     PENDING/READY row and re-runs this method.
//
// The (resolver, request) tuple is intentionally not coupled to the
// pass-through signature of the legacy Service.ForwardCompleted — the
// old free function was an ad-hoc compatibility shim that bypassed
// master-URL rewriting. The Resolver applies that step exactly once
// for every caller.
func (r *Resolver) Resolve(ctx context.Context, req ResolveRequest) (*ResolveOutput, error) {
	if r == nil || r.enqueuer == nil || r.forwardRepo == nil {
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
	//
	// P0-02 repair: if the forwarding row exists but is NOT yet FORWARDED
	// (crash interrupted AtomicForwardAndEnqueue after Job INSERT but
	// before the FORWARDED CAS), call EnsureForwarded to stamp it. This
	// closes the "Job exists, forwarding row stuck in FORWARDING" window.
	if out, hit := r.checkIdempotencyFastPath(ctx, req, jobID, targetExecutor); hit {
		return out, nil
	}

	// 4. Build + rewrite worker payload. Skip rewriting when the
	// resolver was constructed without dataDir+masterURL (in-runner
	// path; the remote engine already produced a complete result).
	workerPayload, err := r.buildAndRewritePayload(req.Payload, fwdKey)
	if err != nil {
		return nil, err
	}

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
	if err := r.forwardRepo.AtomicForwardAndEnqueue(ctx, forwardingID, job, spec, priority); err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve atomic: %w", err)
	}

	return &ResolveOutput{
		JobID:        job.ID,
		ForwardingID: forwardingID,
		Response:     buildFreshResolveResponse(job),
	}, nil
}
