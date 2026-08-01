// Package enqueue fornisce funzioni condivise per la normalizzazione, il building e
// l'inoltro di job video (process_video) nella coda. Usato da endpoint canonici come
// script/generate-with-images e pipeline.
//
// The Enqueuer is a Compiler: it normalizes, validates, resolves
// voiceover/scene-image assets, compiles a TaskSpec, and delegates to
// store.AtomicJobTaskCreator for atomic Job+Task creation. All producers
// (HTTP, creator result, calendar) route through the single atomic
// creation path.
//
// Layering note (R2-A split): the 14 pure stateless payload-normalization
// helpers (validatePlanPayload, normalizeScene*, hasClipTimelinePayload,
// syncAudioURLFromVoiceover, sceneVideoFingerprint, extractPlanMaxRetry,
// resolveInternalExecutorID, resolveRequiredCapabilities, etc.) live in
// sibling normalize.go so the orchestration code below can be reasoned
// about linearly. Same `package enqueue`, so private symbols
// (validationError alias, PlanDestination, ResolvedPlan, PlanResolver)
// remain in scope across both files without re-export.
//
// Migration note: the previously-private *validationError struct/method
// surface in this package was collapsed into the canonical typed
// surface via the package-wide type alias
// `type validationError = deliveryplan.ValidationError` (see
// delivery_plan_validator.go for the declaration). Every literal
// that previously used the unexported `field/message/wrapped`
// field names now routes through the canonical constructors:
//
//	deliveryplan.NewValidationError(field, msg)
//	deliveryplan.NewValidationErrorWrapped(field, msg, wrappedErr)
//
// The `<FieldPath>: <Msg>` envelope is preserved verbatim so the
// existing creatorflow.WriteResolverError detection path,
// integration_test golden assertions, and substring-matched log
// callers continue to work without any test-side change. The shape
// rules themselves (allowed payload shapes, legacy fallback, per-
// entry invariants, field paths) live in shared/contract/
// deliveryplan/parser.go as the single source of truth; this
// package constructs only the enqueue-layer-specific precondition
// messages (e.g. "no plan resolver configured", "resolve failed: ...,
// create job_delivery_plans rows for this job before enqueueing")
// and the normalizer's video-metadata validation strings via the
// canonical constructors.
package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	assetbridge "velox-server/internal/assets"
	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/routing"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
	"velox-server/internal/telemetry"
	"velox-shared/contract/deliveryplan"
	"velox-shared/payload"

	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
)

// Enqueuer bundles the atomic creator + jobs reader + the asset service
// that rewrites voiceover and scene-image payload references. Construct via
// NewEnqueuer.
//
// A PlanResolver is mandatory: NewEnqueuer panics on nil so
// misconfiguration surfaces at boot.
//
// SocialValidator is OPTIONAL: use WithSocialValidator if a `*socialclient.Client`
// has been wired at the composition root. When nil, the per-entry
// pre-flight loop in validateDeliveryPlanRequires is a no-op
// (NOOP_DESTINATION_VALIDATOR) so existing callers (Drive-only, dev
// mode without a Social API configured) keep working unchanged.
type Enqueuer struct {
	Creator         *store.AtomicJobTaskCreator
	Jobs            jobs.Reader
	Voiceover       *assetbridge.AssetService
	PlanResolver    PlanResolver
	SocialValidator DestinationValidator
}

// NewEnqueuer constructs an Enqueuer with mandatory Creator + Jobs + PlanResolver.
// The voiceover service is optional (nil-safe). The SocialValidator is
// optional — wire it via WithSocialValidator at the composition root
// if the social_repo boundary is available in this environment.
//
// PlanResolver is mandatory: passing nil panics so misconfiguration
// surfaces at construction time, not on the first enqueue.
func NewEnqueuer(creator *store.AtomicJobTaskCreator, jobsRepo jobs.Reader, voiceover *assetbridge.AssetService, planResolver PlanResolver) *Enqueuer {
	if planResolver == nil {
		panic("enqueue.NewEnqueuer: planResolver is required (delivery plan precondition must be enforced at enqueue time)")
	}
	return &Enqueuer{Creator: creator, Jobs: jobsRepo, Voiceover: voiceover, PlanResolver: planResolver}
}

// WithSocialValidator returns the Enqueuer with a destination
// validator wired in for the per-entry pre-flight loop in
// validateDeliveryPlanRequires. The typical wiring is
// `enqueuer.WithSocialValidator(socialclient.New(socialclient.ConfigFromEnv()))`
// at the composition root; nil disables the pre-flight (legacy /
// drive-only / dev consumers).
func (e *Enqueuer) WithSocialValidator(v DestinationValidator) *Enqueuer {
	if e == nil {
		return e
	}
	e.SocialValidator = v
	return e
}

// =============================================================================
// Core enqueue entry point
// =============================================================================

// EnqueueOption customizes the Job before it is persisted. It is used
// by callers that need to stamp system-level fields (e.g. workspace_id)
// without polluting the user's payload map.
type EnqueueOption func(*jobs.Job)

// WithWorkspaceID scopes the created Job to the given InstaEdit
// workspace. It is a no-op when id == 0 (legacy callers).
func WithWorkspaceID(id int64) EnqueueOption {
	return func(j *jobs.Job) {
		if id != 0 {
			j.WorkspaceID = &id
		}
	}
}

// Enqueue is the canonical scene-video enqueue. The Enqueuer owns both
// the atomic creator + asset service so rewrite invariants are applied
// exactly once before the atomic Job+Task creation.
//
// Callers MUST publish the per-job `costmodel.JobRequirements` for the
// eligibility layer + future-rank site to consume.
//
// When the payload carries `_internal_forwarding_key`, the job_id is
// derived deterministically from that key (via DeriveForwardingJobID)
// instead of generating a random UUID. This ensures concurrent
// pollers, duplicate webhooks, and post-crash retries always produce
// the same Job ID.
//
// Callers that need the Job+TaskSpec without a DB write (e.g. for an
// atomic multi-table transaction with creator_forwardings) should use
// PrepareJobAndTask instead.
func (e *Enqueuer) Enqueue(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements, opts ...EnqueueOption) (map[string]interface{}, error) {
	ctx, enqueueMetrics := telemetry.EnsureEnqueueMetrics(ctx)
	ctx, span := telemetry.StartSpan(ctx, "enqueue")
	defer span.End()
	defer enqueueMetrics.RecordOnSpan(span)
	if e == nil || e.Creator == nil {
		return nil, wrapEnqueuePhase(EnqueuePhaseValidateInput, fmt.Errorf("creator unavailable"))
	}

	job, spec, priority, err := e.PrepareJobAndTask(ctx, payloadMap, req, opts...)
	if err != nil {
		return nil, err
	}

	jobID := job.ID
	normalized := spec.Payload

	// Idempotency check: when the Job already exists, return the REAL
	// persisted status instead of claiming PENDING with enqueue_confirmed=true.
	// The UNIQUE constraint on jobs.job_id is the authoritative dedup;
	// this pre-check reads the actual state so callers know whether the
	// job is still running, succeeded, or failed.
	if e.Jobs != nil {
		if existing, getErr := e.Jobs.Get(ctx, jobID); getErr == nil && existing != nil && existing.ID == jobID {
			return buildIdempotentResponse(normalized, existing), nil
		}
	}

	if err := e.persistEnqueueJobTask(ctx, job, spec, priority); err != nil {
		// The pre-check above is only an optimization. Concurrent retries
		// can both miss it and race on the authoritative jobs.job_id
		// constraint. A losing deterministic retry must converge on the
		// committed row after its transaction rolls back; unrelated DB
		// errors remain hard failures.
		if isJobIDUniqueConflict(err) && e.Jobs != nil {
			if existing, getErr := e.Jobs.Get(ctx, jobID); getErr == nil && existing != nil && existing.ID == jobID {
				return buildIdempotentResponse(normalized, existing), nil
			}
		}
		return nil, wrapEnqueuePhase(EnqueuePhasePersistJobAndTask, fmt.Errorf("enqueue: atomic create: %w", err))
	}

	// The atomic creator has already validated and persisted the delivery
	// plan in the same transaction as Job+Task. Do not run the DB-backed
	// resolver as a second gate here: its read uses a separate connection,
	// and any error at this point would report enqueue failure after the
	// transaction committed, leaving a job that callers may retry.
	// Finalization remains responsible for resolving the durable plan when
	// delivery records are created.
	return buildSceneVideoResponse(normalized), nil
}

// isJobIDUniqueConflict identifies only a SQLite uniqueness conflict for
// the canonical jobs.job_id identity. The transaction owner has already
// rolled back before this helper is reached, so a successful lookup is a
// safe idempotent retry confirmation. Other constraint failures must not be
// converted into success.
func isJobIDUniqueConflict(err error) bool {
	if err == nil || !strings.Contains(err.Error(), "jobs.job_id") {
		return false
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return isJobIdentityConstraint(sqliteErr.ExtendedCode)
	}
	var sqliteErrPtr *sqlite3.Error
	if errors.As(err, &sqliteErrPtr) && sqliteErrPtr != nil {
		return isJobIdentityConstraint(sqliteErrPtr.ExtendedCode)
	}
	return false
}

func isJobIdentityConstraint(code sqlite3.ErrNoExtended) bool {
	return code == sqlite3.ErrConstraintUnique || code == sqlite3.ErrConstraintPrimaryKey
}

// enforceDeliveryPlanPrecondition resolves the per-job plan and applies
// the precondition invariants. On success, the Job's MaxRetries is set
// to the MAX retry_budget across destinations.
func (e *Enqueuer) enforceDeliveryPlanPrecondition(ctx context.Context, jobID string, job *jobs.Job) error {
	if e == nil || e.PlanResolver == nil {
		return deliveryplan.NewValidationError(
			"delivery_plan",
			"no plan resolver configured",
		)
	}
	plan, err := e.PlanResolver.ResolvePlan(ctx, jobID, "")
	if err != nil {
		return deliveryplan.NewValidationErrorWrapped(
			"delivery_plan",
			fmt.Sprintf("resolve failed: %v; create job_delivery_plans rows for this job before enqueueing", err),
			err,
		)
	}
	return validatePlanPayload(plan, job)
}

// PrepareJobAndTask normalizes the payload, resolves assets, and compiles
// a Job+TaskSpec WITHOUT writing to the database.
//
// Scorecard v2 / Step 15: starts a "schedule_task" span for distributed
// tracing. The span context propagates through the returned Job ID so
// downstream claim/execute/report spans link to this root span.
func (e *Enqueuer) PrepareJobAndTask(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements, opts ...EnqueueOption) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
	ctx, enqueueMetrics := telemetry.EnsureEnqueueMetrics(ctx)
	ctx, span := telemetry.StartSpan(ctx, "schedule_task")
	defer span.End()
	defer enqueueMetrics.RecordOnSpan(span)

	return e.prepareJobAndTask(ctx, payloadMap, req, opts...)
}

// prepareJobAndTask is the internal implementation extracted so the
// span wrapper above keeps the defer span.End() clean.
func (e *Enqueuer) prepareJobAndTask(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements, opts ...EnqueueOption) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
	finishPhase := telemetry.BeginEnqueuePhase(ctx, string(EnqueuePhaseValidateInput))
	forwardingKey, hasForwardingKey, err := e.validateEnqueueInput(payloadMap)
	finishPhase()
	if err != nil {
		return nil, nil, 0, wrapEnqueuePhase(EnqueuePhaseValidateInput, err)
	}

	// Delivery validation intentionally remains after asset resolution and
	// canonical normalization: deliveryplan.Parse receives the single
	// canonical map used by the rest of enqueue. Legacy delivery aliases are
	// retained by the compatibility projection during normalization.
	finishPhase = telemetry.BeginEnqueuePhase(ctx, string(EnqueuePhaseResolveAssets))
	if err := e.resolveEnqueueAssets(ctx, payloadMap); err != nil {
		finishPhase()
		return nil, nil, 0, wrapEnqueuePhase(EnqueuePhaseResolveAssets, err)
	}
	finishPhase()

	finishPhase = telemetry.BeginEnqueuePhase(ctx, string(EnqueuePhaseNormalizePayload))
	normalized, err := normalizeEnqueuePayload(ctx, payloadMap, forwardingKey, hasForwardingKey)
	finishPhase()
	if err != nil {
		return nil, nil, 0, wrapEnqueuePhase(EnqueuePhaseNormalizePayload, err)
	}
	finishPhase = telemetry.BeginEnqueuePhase(ctx, string(EnqueuePhaseValidateInput))
	if err := e.validateEnqueueDeliveryPlan(ctx, normalized); err != nil {
		finishPhase()
		return nil, nil, 0, wrapEnqueuePhase(EnqueuePhaseValidateInput, err)
	}
	finishPhase()

	// Forwarding identity is deterministic and must be finalized before
	// worker projection. Ordinary payloads receive a UUID only when no
	// forwarding key supplied an identity.
	jobID, _ := normalized["job_id"].(string)
	fwdMeta := routing.FromPayload(normalized)
	if fwdMeta.ForwardingKey != "" {
		jobID = DeriveForwardingJobID(fwdMeta.ForwardingKey.String())
		normalized["job_id"] = jobID
	}
	if jobID == "" {
		jobID = uuid.NewString()
		normalized["job_id"] = jobID
	}

	finishPhase = telemetry.BeginEnqueuePhase(ctx, string(EnqueuePhaseProjectWorker))
	job, spec, priority, err := projectEnqueueJobContext(ctx, normalized, req)
	finishPhase()
	if err != nil {
		return nil, nil, 0, wrapEnqueuePhase(EnqueuePhaseProjectWorker, err)
	}
	if job == nil || spec == nil {
		return nil, nil, 0, wrapEnqueuePhase(EnqueuePhaseProjectWorker, fmt.Errorf("projected job/task is nil"))
	}
	if maxRetry := extractPlanMaxRetry(normalized); maxRetry > 0 {
		job.MaxRetries = maxRetry
	}

	for _, opt := range opts {
		if opt != nil {
			opt(job)
		}
	}
	return job, spec, priority, nil
}

// validateForwardingIdentity enforces the fail-closed identity boundary.
// Ordinary jobs may omit the internal key and keep their UUID identity. Once
// the key is present, however, it must be a canonical, complete
// source-provider/source-job/target-executor tuple; otherwise the caller is on
// an idempotent path whose identity cannot be trusted.
func validateForwardingIdentity(payloadMap map[string]interface{}) (string, bool, error) {
	if payloadMap == nil {
		return "", false, nil
	}
	raw, present := payloadMap[routing.KeyForwardingKey]
	if !present {
		return "", false, nil
	}
	key, ok := raw.(string)
	if !ok {
		return "", true, fmt.Errorf("%s must be a string for idempotent forwarding", routing.KeyForwardingKey)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", true, fmt.Errorf("%s is required for idempotent forwarding", routing.KeyForwardingKey)
	}

	provider, sourceJobID, executorID := routing.ForwardingKey(key).Parse()
	if provider == "" || sourceJobID == "" || executorID == "" {
		return "", true, fmt.Errorf("%s must contain source_provider, source_job_id, and target_executor_id", routing.KeyForwardingKey)
	}
	canonical := routing.FormatForwardingKey(provider, sourceJobID, executorID).String()
	if canonical != key {
		return "", true, fmt.Errorf("%s is not canonical", routing.KeyForwardingKey)
	}
	return key, true, nil
}

func buildSceneVideoResponse(normalized map[string]interface{}) map[string]interface{} {
	jobID, _ := normalized["job_id"].(string)
	jobRunID := strings.TrimSpace(payload.FirstString(normalized, "job_run_id", "run_id"))
	correlationID := strings.TrimSpace(payload.FirstString(normalized, "correlation_id"))
	jobFingerprint := strings.TrimSpace(payload.FirstString(normalized, "job_fingerprint"))

	return map[string]interface{}{
		"ok":                true,
		"job_id":            jobID,
		"job_run_id":        jobRunID,
		"correlation_id":    correlationID,
		"job_type":          "process_video",
		"status":            "PENDING",
		"enqueue_confirmed": true,
		"dispatch_status":   "queued_for_workers",
		"scene_count":       sceneCountFromPayload(normalized),
		"voiceover_count":   voiceoverCountFromPayload(normalized),
		"job_fingerprint":   jobFingerprint,
	}
}

// buildIdempotentResponse returns a response for an already-existing Job,
// carrying the REAL persisted status instead of hardcoding PENDING.
// When the existing Job is SUCCEEDED, FAILED, or any other terminal state,
// callers see the truth instead of a misleading "queued_for_workers".
func buildIdempotentResponse(normalized map[string]interface{}, existing *jobs.Job) map[string]interface{} {
	jobID := existing.ID
	status := string(existing.Status)
	jobRunID := existing.RunID
	correlationID := strings.TrimSpace(payload.FirstString(normalized, "correlation_id"))
	jobFingerprint := strings.TrimSpace(payload.FirstString(normalized, "job_fingerprint"))

	resp := map[string]interface{}{
		"ok":                true,
		"job_id":            jobID,
		"created":           false,
		"status":            status,
		"enqueue_confirmed": true,
		"job_type":          "process_video",
		"scene_count":       sceneCountFromPayload(normalized),
		"voiceover_count":   voiceoverCountFromPayload(normalized),
	}
	if jobRunID != "" {
		// Drop the redundant `run_id` dual-write: the idempotent-confirm
		// response emits canonical `job_run_id` only.
		resp["job_run_id"] = jobRunID
	}
	if correlationID != "" {
		resp["correlation_id"] = correlationID
	}
	if jobFingerprint != "" {
		resp["job_fingerprint"] = jobFingerprint
	}
	return resp
}

// =============================================================================
// Asset rewrite (shared with `internal/assets` package)
// =============================================================================

// rewriteVoiceoverPayloadFor is the single canonical implementation of
// voiceover rewrite. Both the (e *Enqueuer) method and the package-level
// fallback `resolveVoiceoverPayload` delegate here so the rewrite
// invariants live in ONE place; only the service source differs.
func rewriteVoiceoverPayloadFor(ctx context.Context, service *assetbridge.AssetService, payloadMap map[string]interface{}) error {
	if service == nil || payloadMap == nil {
		return nil
	}
	return service.RewriteVoiceoverPayload(ctx, payloadMap)
}

// rewriteSceneImagePayloadFor mirrors rewriteVoiceoverPayloadFor for
// scene-image resolution. Shared invariant: nil service is a no-op.
func rewriteSceneImagePayloadFor(ctx context.Context, service *assetbridge.AssetService, payloadMap map[string]interface{}) error {
	if service == nil || payloadMap == nil {
		return nil
	}
	return service.RewriteSceneImagePayload(ctx, payloadMap)
}

func (e *Enqueuer) resolveVoiceoverPayload(ctx context.Context, payloadMap map[string]interface{}) error {
	if e == nil {
		return nil
	}
	if err := rewriteVoiceoverPayloadFor(ctx, e.Voiceover, payloadMap); err != nil {
		return err
	}
	syncAudioURLFromVoiceover(payloadMap)
	return nil
}

func (e *Enqueuer) resolveSceneImagePayload(ctx context.Context, payloadMap map[string]interface{}) error {
	if e == nil {
		return nil
	}
	return rewriteSceneImagePayloadFor(ctx, e.Voiceover, payloadMap)
}

func hasTimedVideoClipSegments(value interface{}) bool {
	return hasTimedVideoClipSegmentsContext(context.Background(), value)
}

func hasTimedVideoClipSegmentsContext(ctx context.Context, value interface{}) bool {
	switch typed := value.(type) {
	case map[string]interface{}:
		if _, hasStart := typed["start_seconds"]; hasStart {
			if _, hasEnd := typed["end_seconds"]; hasEnd {
				return true
			}
		}
		if _, hasStart := typed["start_ms"]; hasStart {
			if _, hasEnd := typed["end_ms"]; hasEnd {
				return true
			}
		}
		for _, child := range typed {
			if hasTimedVideoClipSegmentsContext(ctx, child) {
				return true
			}
		}
	case []interface{}:
		for _, child := range typed {
			if hasTimedVideoClipSegmentsContext(ctx, child) {
				return true
			}
		}
	case []map[string]interface{}:
		for _, child := range typed {
			if hasTimedVideoClipSegmentsContext(ctx, child) {
				return true
			}
		}
	case string:
		var decoded interface{}
		if strings.HasPrefix(strings.TrimSpace(typed), "[") || strings.HasPrefix(strings.TrimSpace(typed), "{") {
			telemetry.RecordEnqueueJSONUnmarshal(ctx)
			if json.Unmarshal([]byte(typed), &decoded) == nil {
				return hasTimedVideoClipSegmentsContext(ctx, decoded)
			}
		}
	}
	return false
}

// DeriveForwardingJobID produces a deterministic, UUID-shaped job ID from a
// forwarding key. The key should be formatted as:
//
//	source_provider + ":" + source_job_id + ":" + target_executor_id
//
// Two calls with the same key always produce the same job ID, ensuring that
// concurrent pollers, duplicate webhooks, and post-crash retries converge on
// a single Velox Job row. The UNIQUE constraint on jobs.job_id is the
// authoritative dedup; this helper makes the deterministic derivation explicit.
func DeriveForwardingJobID(forwardingKey string) string {
	sum := sha256.Sum256([]byte(forwardingKey))
	return "job_" + hex.EncodeToString(sum[:8])
}

// =============================================================================
// Plan types
// =============================================================================

// PlanDestination is a minimal subset of the per-destination plan that the
// Enqueuer needs to enforce the precondition. Defined locally to decouple
// the enqueue contract from the deliveries package (no import edge) and
// to allow the precondition to be unit-tested with a hand-rolled mock.
type PlanDestination struct {
	DestinationID string
	Priority      int
	RetryBudget   int
}

// ResolvedPlan is the per-job delivery plan returned by PlanResolver.
// Destinations is the full per-destination slice with retry_budget.
type ResolvedPlan struct {
	JobID        string
	Destinations []PlanDestination
}

// PlanResolver is the contract Enqueuer needs at enqueue time.
// ResolvePlan (NOT ResolveDestinations) is the chosen method so the
// per-destination retry_budget is available for validation AND
// propagation to the Job. The deliveries.SQLiteDeliveryPlanResolver
// implements this contract via a thin adapter at the composition
// root; in tests, a hand-rolled mock struct satisfies the interface
// directly.
type PlanResolver interface {
	ResolvePlan(ctx context.Context, jobID, artifactID string) (*ResolvedPlan, error)
}
