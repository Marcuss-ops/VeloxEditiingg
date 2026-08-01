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
	"database/sql"
	"fmt"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
	"velox-server/internal/store"
	"velox-shared/contract/deliveryplan"
	"velox-shared/publication"
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
	db          *sql.DB
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
		db:          dbStore.DB(),
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
		db:          dbStore.DB(),
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
		db:          dbStore.DB(),
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
// preparePayloadWithControlPlaneDelivery builds the short-lived enqueue
// envelope. Delivery routing is present only long enough for enqueue's
// validation/parser; compileSceneVideoJob removes it before TaskSpec.Payload
// is persisted for the renderer. Publication specs never enter this map.
func preparePayloadWithControlPlaneDelivery(rendererPayload, deliveryPlan map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(rendererPayload)+len(deliveryPlan))
	for key, value := range rendererPayload {
		out[key] = value
	}
	for key, value := range deliveryPlan {
		out[key] = value
	}
	return out
}

func clonePublicationSpecs(specs []publication.Spec) []map[string]interface{} {
	if len(specs) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, len(specs))
	for i, spec := range specs {
		out[i] = publicationSpecToMap(spec)
	}
	return out
}

// publicationSpecToMap projects the typed control-plane contract directly to
// the map shape required by TaskSpec.PublicationSpecs. The previous
// implementation marshaled and unmarshaled every spec through JSON, adding
// substantial CPU and allocation cost to every fresh Resolve. Keep the
// projection explicit so renderer payloads remain untouched and the wire
// contract stays map-based.
func publicationSpecToMap(spec publication.Spec) map[string]interface{} {
	out := make(map[string]interface{}, 9)
	if spec.Version != 0 {
		out["version"] = spec.Version
	}
	out["publication_id"] = spec.PublicationID

	outputRef := make(map[string]interface{}, 2)
	if spec.OutputRef.VariantID != "" {
		outputRef["variant_id"] = spec.OutputRef.VariantID
	}
	if spec.OutputRef.ArtifactRole != "" {
		outputRef["artifact_role"] = spec.OutputRef.ArtifactRole
	}
	out["output_ref"] = outputRef
	if spec.Language != "" {
		out["language"] = spec.Language
	}
	if spec.DefaultLanguage != "" {
		out["default_language"] = spec.DefaultLanguage
	}
	out["metadata"] = publicationMetadataToMap(spec.Metadata)

	if len(spec.Localizations) > 0 {
		localizations := make(map[string]interface{}, len(spec.Localizations))
		for locale, metadata := range spec.Localizations {
			value := make(map[string]interface{}, 2)
			if metadata.Title != "" {
				value["title"] = metadata.Title
			}
			if metadata.Description != "" {
				value["description"] = metadata.Description
			}
			localizations[locale] = value
		}
		out["localizations"] = localizations
	}

	if spec.Destinations == nil {
		out["destinations"] = nil
	} else {
		destinations := make([]interface{}, len(spec.Destinations))
		for i, destination := range spec.Destinations {
			value := make(map[string]interface{}, 5)
			value["destination_id"] = destination.DestinationID
			if destination.Priority != 0 {
				value["priority"] = destination.Priority
			}
			if destination.RetryBudget != nil {
				value["retry_budget"] = *destination.RetryBudget
			}
			if destination.MetadataOverride != nil {
				value["metadata_override"] = publicationMetadataToMap(*destination.MetadataOverride)
			}
			if len(destination.ProviderOptions) > 0 {
				value["provider_options"] = clonePublicationValue(destination.ProviderOptions)
			}
			destinations[i] = value
		}
		out["destinations"] = destinations
	}
	if len(spec.ProviderOptions) > 0 {
		out["provider_options"] = clonePublicationValue(spec.ProviderOptions)
	}
	return out
}

func publicationMetadataToMap(metadata publication.Metadata) map[string]interface{} {
	out := make(map[string]interface{}, 8)
	if metadata.Title != "" {
		out["title"] = metadata.Title
	}
	if metadata.Description != "" {
		out["description"] = metadata.Description
	}
	if len(metadata.Tags) > 0 {
		out["tags"] = append([]string(nil), metadata.Tags...)
	}
	if metadata.CategoryID != "" {
		out["category_id"] = metadata.CategoryID
	}
	if metadata.Privacy != "" {
		out["privacy"] = metadata.Privacy
	}
	if metadata.PublishAt != "" {
		out["publish_at"] = metadata.PublishAt
	}
	if metadata.MadeForKids != nil {
		out["made_for_kids"] = *metadata.MadeForKids
	}
	if metadata.ContainsSyntheticMedia != nil {
		out["contains_synthetic_media"] = *metadata.ContainsSyntheticMedia
	}
	return out
}

// clonePublicationValue deep-copies JSON-compatible provider options without
// re-encoding them. Resolver inputs originate from decoded JSON, so these are
// the map/slice forms that need recursive copying; scalar values are immutable.
func clonePublicationValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			out[key] = clonePublicationValue(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = clonePublicationValue(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, len(typed))
		for i, item := range typed {
			if item != nil {
				out[i] = clonePublicationValue(item).(map[string]interface{})
			}
		}
		return out
	case []int:
		return append([]int(nil), typed...)
	case []int64:
		return append([]int64(nil), typed...)
	case []float64:
		return append([]float64(nil), typed...)
	case []bool:
		return append([]bool(nil), typed...)
	default:
		return value
	}
}

func cloneControlPlaneMap(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

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
	if len(req.DeliveryPlan) == 0 {
		req.DeliveryPlan = deliveryplan.ExtractEnvelope(req.Payload)
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

	// 3. Build + rewrite worker payload. Skip rewriting when the
	// resolver was constructed without dataDir+masterURL (in-runner
	// path; the remote engine already produced a complete result).
	workerPayload, err := r.buildAndRewritePayload(req.Payload, fwdKey)
	if err != nil {
		return nil, err
	}

	// 4. Idempotency fast-path with payload-hash check. If the Job
	// already exists AND the existing forwarding row's payload_sha256
	// matches the SHA of the freshly-rebuilt worker payload, return
	// the cached output so duplicates don't write twice.
	//
	// The fast-path MAY return ErrIdempotencyKeyReused when the
	// stored hash differs from the incoming one (P0 #3 contract).
	// That sentinel bubbles up to the handler, which maps it to
	// HTTP 409 `idempotency_key_reused`.
	//
	// P0-02 repair: if the forwarding row exists but is NOT yet
	// FORWARDED (crash interrupted AtomicForwardAndEnqueue after
	// Job INSERT but before the FORWARDED CAS), call EnsureForwarded
	// to stamp it. This closes the "Job exists, forwarding row stuck
	// in FORWARDING" window.
	if out, err := r.checkIdempotencyFastPath(ctx, req, jobID, targetExecutor, workerPayload); out != nil || err != nil {
		return out, err
	}

	// 5. Promote the forwarding row to READY_TO_FORWARD.
	forwardingID, err := r.ensureReadyForwarding(ctx, req, targetExecutor, workerPayload)
	if err != nil {
		return nil, err
	}

	// 6. Compile Job + TaskSpec. The delivery envelope is supplied to
	// enqueue only as a separate control-plane input. PrepareJobAndTask
	// validates it and compiles TaskSpec.DeliveryPlan, while its renderer
	// projection removes the same fields from TaskSpec.Payload.
	preparePayload := preparePayloadWithControlPlaneDelivery(workerPayload, req.DeliveryPlan)
	job, spec, priority, err := r.enqueuer.PrepareJobAndTask(ctx, preparePayload, costmodel.DefaultRequirements())
	if err != nil {
		return nil, fmt.Errorf("creatorflow: Resolve prepare job/task: %w", err)
	}
	// Delivery routing belongs to the control plane. Keep it out of the
	// renderer payload while attaching the separately-carried envelope to
	// TaskSpec, where AtomicJobTaskCreator persists job_delivery_plans.
	spec.DeliveryPlan = cloneControlPlaneMap(req.DeliveryPlan)
	spec.PublicationSpecs = clonePublicationSpecs(req.PublicationSpecs)

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
