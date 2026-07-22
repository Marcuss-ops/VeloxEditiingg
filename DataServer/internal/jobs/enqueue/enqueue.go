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
// (validationError, PlanDestination, ResolvedPlan, PlanResolver) remain
// in scope across both files without re-export.
package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"velox-shared/payload"

	"github.com/google/uuid"
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
	if e == nil || e.Creator == nil {
		return nil, fmt.Errorf("creator unavailable")
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

	if err := e.Creator.CreateJobWithTask(ctx, job, spec, priority); err != nil {
		return nil, fmt.Errorf("enqueue: atomic create: %w", err)
	}

	// ResolvePlan runs AFTER the atomic create so it sees the plan
	// rows this call just committed. A small matcher race window
	// exists before the precondition returns, but worker-side
	// re-resolution lands on WAITING_FOR_PLAN so observability holds.
	if err := e.enforceDeliveryPlanPrecondition(ctx, jobID, job); err != nil {
		return nil, fmt.Errorf("enqueue: post-create plan precondition: %w", err)
	}

	return buildSceneVideoResponse(normalized), nil
}

// enforceDeliveryPlanPrecondition resolves the per-job plan and applies
// the precondition invariants. On success, the Job's MaxRetries is set
// to the MAX retry_budget across destinations.
func (e *Enqueuer) enforceDeliveryPlanPrecondition(ctx context.Context, jobID string, job *jobs.Job) error {
	if e == nil || e.PlanResolver == nil {
		return &validationError{field: "delivery_plan", message: "no plan resolver configured"}
	}
	plan, err := e.PlanResolver.ResolvePlan(ctx, jobID, "")
	if err != nil {
		return &validationError{
			field:   "delivery_plan",
			message: fmt.Sprintf("resolve failed: %v; create job_delivery_plans rows for this job before enqueueing", err),
			wrapped: err,
		}
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
	ctx, span := telemetry.StartSpan(ctx, "schedule_task")
	defer span.End()

	return e.prepareJobAndTask(ctx, payloadMap, req, opts...)
}

// prepareJobAndTask is the internal implementation extracted so the
// span wrapper above keeps the defer span.End() clean.
func (e *Enqueuer) prepareJobAndTask(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements, opts ...EnqueueOption) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
	if e == nil || e.Creator == nil {
		return nil, nil, 0, fmt.Errorf("creator unavailable")
	}

	if err := e.resolveVoiceoverPayload(ctx, payloadMap); err != nil {
		return nil, nil, 0, err
	}
	if err := e.resolveSceneImagePayload(ctx, payloadMap); err != nil {
		return nil, nil, 0, err
	}

	normalized, err := normalizeSceneVideoPayload(payloadMap)
	if err != nil {
		return nil, nil, 0, err
	}

	// A Job without an explicit delivery plan (and per-entry retry_budget > 0)
	// must never reach the workers. Without this preflight,
	// FinalizeVerified would discover the missing plan AFTER the render
	// has burned its budget — the diagnostic's
	// "Validate delivery plan at enqueue or pre-render" regression.
	if err := validateDeliveryPlanRequires(ctx, payloadMap, e.SocialValidator); err != nil {
		return nil, nil, 0, err
	}

	jobID, _ := normalized["job_id"].(string)

	// When a forwarding key is present, derive the job_id
	// deterministically regardless of any auto-generated ID from
	// NewJobPayloadV2 or SetIdentity. Concurrent pollers, duplicate
	// webhooks, and post-crash retries must converge on the same Job ID.
	fwdMeta := routing.FromPayload(normalized)
	if fwdMeta.ForwardingKey != "" {
		jobID = DeriveForwardingJobID(fwdMeta.ForwardingKey.String())
		normalized["job_id"] = jobID
	}

	if jobID == "" {
		jobID = uuid.NewString()
		normalized["job_id"] = jobID
	}

	// compileSceneVideoJob sets MaxRetries=0; extractPlanMaxRetry below
	// is the single writer of that field on the insert path. The
	// post-create precondition in Enqueue re-reads the plan from the
	// DB for consistency gating but no longer mutates the committed
	// value.
	job, spec, priority := compileSceneVideoJob(normalized, req)

	if maxRetry := extractPlanMaxRetry(normalized); maxRetry > 0 {
		job.MaxRetries = maxRetry
	}

	// Apply caller-supplied options (e.g. workspace scoping) before
	// the job is persisted. Options run after the payload-derived
	// fields are computed so they can override defaults.
	for _, opt := range opts {
		if opt != nil {
			opt(job)
		}
	}

	// The plan precondition (ResolvePlan + retry_budget > 0 + MaxRetries
	// propagation) is enforced POST-create in Enqueue(). PrepareJobAndTask
	// stays pure: it only validates the payload shape via
	// validateDeliveryPlanRequires (above) and pre-computes
	// job.MaxRetries from the payload's delivery_plan (above) so the
	// insert-time column matches the resolver's view at INSERT time.
	//
	// *Tx-variant callers (AtomicForwardAndEnqueue etc.) get the same
	// guard via validateDeliveryDestinationTx inside CreateJobWithTaskTx,
	// which rejects malformed delivery contracts in the same SQLite tx.
	// The public Enqueue path additionally runs a post-create resolver
	// round-trip purely so observability on the new-submit hot path
	// surfaces an actionable "missing plan" hint before the worker
	// invests in a lease — the canonical fix for the "manual preinsert
	// required" production bug surfaced on the Jackie Chan doc-voiceover
	// real run.

	return job, spec, priority, nil
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

type validationError struct {
	field   string
	message string
	wrapped error // optional underlying cause (e.g. deliveries.ErrNoExplicitPlan)
}

func (e *validationError) Error() string {
	return e.field + ": " + e.message
}

// Field returns the structured field path that produced the rejection
// (e.g. "delivery_plan[0].social_destination_id"). Exposed via a
// getter (rather than exporting the field) so the unexported
// `field` stays a private invariant — but cross-package callers can
// still reach the path via `errors.As(err, &verr); verr.Field()`.
func (e *validationError) Field() string {
	if e == nil {
		return ""
	}
	return e.field
}

// Message returns the human-readable rejection message WITHOUT the
// field-path prefix (use Error() if you want the field+message
// concatenation). Exposed via a getter for the same reason as Field.
func (e *validationError) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

// Unwrap returns the underlying cause so errors.Is / errors.As can
// inspect the original resolver error (e.g. deliveries.ErrNoExplicitPlan).
// Without this, callers can only inspect the formatted message, which is
// fragile across message refactors.
func (e *validationError) Unwrap() error {
	return e.wrapped
}

// ValidationErrorField returns the structured field path (e.g.
// "delivery_plan[0].social_destination_id") of the *validationError
// wrapped inside err, or "" if err is not a validationError. Exposed
// as a package-level helper so cross-package callers (the
// integration_test package + future HTTP handlers + CLI tooling) can
// extract the field path without needing access to the unexported
// `validationError` type itself.
//
// Typical usage:
//
//	if got := enqueue.ValidationErrorField(err); got != "delivery_plan[0].social_destination_id" {
//	    // fail the assertion
//	}
//
// This helper intentionally returns "" (not an error) on a
// non-validationError input so callers can use it in expression
// position without short-circuiting their flow.
func ValidationErrorField(err error) string {
	if err == nil {
		return ""
	}
	var verr *validationError
	if errors.As(err, &verr) && verr != nil {
		return verr.Field()
	}
	return ""
}

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
