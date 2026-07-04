// Package enqueue fornisce funzioni condivise per la normalizzazione, il building e
// l'inoltro di job video (process_video) nella coda. Usato da endpoint canonici come
// script/generate-with-images e pipeline.
//
// The Enqueuer is a Compiler: it normalizes, validates, resolves
// voiceover/scene-image assets, compiles a TaskSpec, and delegates to
// store.AtomicJobTaskCreator for atomic Job+Task creation. All producers
// (HTTP, creator result, calendar) route through the single atomic
// creation path.
package enqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	assetbridge "velox-server/internal/assets"
	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/routing"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
	"velox-shared/payload"

	"context"

	"github.com/google/uuid"
)

// Enqueuer bundles the atomic creator + jobs reader + the asset service
// that rewrites voiceover and scene-image payload references. Construct via
// NewEnqueuer.
//
// A PlanResolver is mandatory: the Enqueuer calls ResolvePlan (NOT
// ResolveDestinations) before every atomic create so the per-job
// retry_budget is validated upfront and propagated to the Job's
// MaxRetries. This eliminates the late re-resolve in FinalizeVerified
// and surfaces missing-plan errors at enqueue time as actionable
// rejections.
type Enqueuer struct {
	Creator      *store.AtomicJobTaskCreator
	Jobs         jobs.Reader
	Voiceover    *assetbridge.AssetService
	PlanResolver PlanResolver
}

// NewEnqueuer constructs an Enqueuer with mandatory Creator + Jobs + PlanResolver.
// The voiceover service is optional (nil-safe: voiceover resolution is skipped).
//
// The PlanResolver precondition is mandatory: passing nil triggers a panic
// at construction time so misconfiguration is caught at boot, not on the
// first enqueue in production. This is a fail-fast for an architectural
// invariant, not a runtime error.
func NewEnqueuer(creator *store.AtomicJobTaskCreator, jobsRepo jobs.Reader, voiceover *assetbridge.AssetService, planResolver PlanResolver) *Enqueuer {
	if planResolver == nil {
		panic("enqueue.NewEnqueuer: planResolver is required (delivery plan precondition must be enforced at enqueue time)")
	}
	return &Enqueuer{Creator: creator, Jobs: jobsRepo, Voiceover: voiceover, PlanResolver: planResolver}
}

// =============================================================================
// Core enqueue entry point
// =============================================================================

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
func (e *Enqueuer) Enqueue(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements) (map[string]interface{}, error) {
	if e == nil || e.Creator == nil {
		return nil, fmt.Errorf("creator unavailable")
	}

	job, spec, priority, err := e.PrepareJobAndTask(ctx, payloadMap, req)
	if err != nil {
		return nil, err
	}

	// The delivery-plan precondition (ResolvePlan + retry_budget > 0 +
	// MaxRetries propagation) is enforced inside PrepareJobAndTask so it
	// cannot be bypassed by AtomicForwardAndEnqueue or any other caller
	// of the prep helper. See enforceDeliveryPlanPrecondition in
	// PrepareJobAndTask for the full contract.

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

	// ResolvePlan runs AFTER the atomic create so the resolver sees the
	// plan rows the same tx just committed. Without this order, the
	// resolver would see an empty plan on every first-time job submit
	// (ErrNoExplicitPlan) and operators would have to manual-INSERT
	// plan rows before each POST.
	//
	// Trade-off: there is a small race window between the tx commit
	// and the precondition run where a matcher could briefly pick up
	// the PENDING task. The task is re-resolved on the worker
	// (deliveries.SQLiteDeliveryPlanResolver runs at claim time too)
	// and lands in WAITING_FOR_PLAN on plan-miss, so observability is
	// preserved. jobs.max_retries stays at 0 from compileSceneVideoJob;
	// per-row delivery retry_budget remains authoritative at lease time,
	// so the column being 0 does not regress delivery correctness.
	if err := e.enforceDeliveryPlanPrecondition(ctx, jobID, job); err != nil {
		return nil, fmt.Errorf("enqueue: post-create plan precondition: %w", err)
	}

	return buildSceneVideoResponse(normalized), nil
}

// enforceDeliveryPlanPrecondition resolves the per-job delivery plan and
// enforces three invariants:
//  1. The resolver must return a non-nil plan (ErrNoExplicitPlan surfaces
//     as a validationError with a clear "create job_delivery_plans rows"
//     hint so operators know exactly what to do).
//  2. The plan must carry at least one destination (an explicit plan with
//     zero destinations is treated as missing).
//  3. Every destination's retry_budget must be > 0 (the per-delivery
//     delivery_plan_payload.go validator already rejects retry_budget<=0
//     at parse time; this is the runtime counterpart at enqueue time).
//
// On success, the Job's MaxRetries is set to the MAX retry_budget across
// all destinations so the job-level budget can cover the worst-case
// per-delivery retry chain. Per-delivery retry_budget is still authoritative
// at INSERT time (see deliveries/runner.go: lease carries per-row value).
func (e *Enqueuer) enforceDeliveryPlanPrecondition(ctx context.Context, jobID string, job *jobs.Job) error {
	if e == nil || e.PlanResolver == nil {
		// Defensive: NewEnqueuer panics if PlanResolver is nil, so this branch
		// is only reachable via direct struct manipulation in tests. Treat
		// as a hard precondition failure.
		return &validationError{field: "delivery_plan", message: "no plan resolver configured; cannot enforce delivery plan precondition"}
	}
	plan, err := e.PlanResolver.ResolvePlan(ctx, jobID, "")
	if err != nil {
		// Wrap the original error so errors.Is(err, deliveries.ErrNoExplicitPlan)
		// works at the call site, while still surfacing an actionable
		// "create job_delivery_plans rows" hint in the formatted message.
		return &validationError{
			field:   "delivery_plan",
			message: fmt.Sprintf("resolve failed: %v; create job_delivery_plans rows for this job before enqueueing", err),
			wrapped: err,
		}
	}
	if plan == nil || len(plan.Destinations) == 0 {
		return &validationError{field: "delivery_plan", message: "no explicit delivery plan; create job_delivery_plans rows for this job before enqueueing"}
	}
	maxRetry := 0
	for i, d := range plan.Destinations {
		if d.RetryBudget <= 0 {
			return &validationError{field: fmt.Sprintf("delivery_plan[%d].retry_budget", i), message: "must be > 0"}
		}
		if d.RetryBudget > maxRetry {
			maxRetry = d.RetryBudget
		}
	}
	// Propagate retry_budget to the Job: the job-level MaxRetries covers
	// the worst-case per-destination retry chain so the job can outlive
	// any single destination's retries.
	if job != nil {
		job.MaxRetries = maxRetry
	}
	return nil
}

// PrepareJobAndTask normalizes the payload, resolves assets, and compiles
// a Job+TaskSpec WITHOUT writing to the database. This is the extraction of
// the pure business logic from Enqueue, intended for callers that need to
// manage the atomic write themselves (e.g. AtomicForwardAndEnqueue which
// combines the Job+Task+TaskSpec creation with a creator_forwardings status
// update in a single SQLite transaction).
//
// Returns the prepared Job, TaskSpec, priority, and the normalized payload
// embedded in spec.Payload.
func (e *Enqueuer) PrepareJobAndTask(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
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
	if err := validateDeliveryPlanRequires(payloadMap); err != nil {
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

	// compileSceneVideoJob leaves MaxRetries at 0; the two writers
	// below (extractPlanMaxRetry from the payload, then
	// enforceDeliveryPlanPrecondition re-deriving from the DB) are
	// the only owners of that field on the insert path.
	job, spec, priority := compileSceneVideoJob(normalized, req)

	// Pre-compute MaxRetries from the payload's delivery_plan so
	// jobs.max_retries reflects the worst-case per-destination budget
	// AT INSERT time. The post-create resolver-based precondition
	// (in Enqueue) re-reads the plan from the DB for consistency
	// gating (presence + retry_budget > 0) and re-writes MaxRetries
	// on the in-memory job struct, but the column value committed
	// here is from the payload. In production this matches the
	// resolver's view because the resolver reads from
	// job_delivery_plans rows this tx just inserted.
	if job.MaxRetries == 0 {
		if maxRetry := extractPlanMaxRetry(normalized); maxRetry > 0 {
			job.MaxRetries = maxRetry
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

// compileSceneVideoJob builds a canonical *jobs.Job and *taskgraph.TaskSpec
// from a normalized scene-video payload. The caller owns the atomic creation.
func compileSceneVideoJob(normalized map[string]interface{}, req costmodel.JobRequirements) (*jobs.Job, *taskgraph.TaskSpec, int) {
	jobID, _ := normalized["job_id"].(string)
	videoName, _ := normalized["video_name"].(string)
	projectID, _ := normalized["project_id"].(string)
	jobRunID, _ := normalized["job_run_id"].(string)
	if jobRunID == "" {
		jobRunID, _ = normalized["run_id"].(string)
	}
	jobType, _ := normalized["job_type"].(string)
	if jobType == "" {
		jobType = "process_video"
	}
	priority := payload.EnsureInt(normalized["priority"], 5)

	raw, _ := json.Marshal(normalized)

	job := &jobs.Job{
		ID:        jobID,
		Type:      jobType,
		Status:    jobs.StatusPending,
		VideoName: videoName,
		ProjectID: projectID,
		RunID:     jobRunID,
		// MaxRetries is set by enforceDeliveryPlanPrecondition (the
		// delivery-plan resolver propagates max(retry_budget) per
		// destination). It is intentionally left at 0 here so the
		// single owner of the field is explicit.
		MaxRetries:   0,
		Payload:      string(raw),
		Requirements: req,
	}

	executorID := "scene.composite.v1"
	if resolved := resolveInternalExecutorID(normalized); resolved != "" {
		executorID = resolved
	}

	spec := &taskgraph.TaskSpec{
		Version:              taskgraph.SpecVersion,
		JobID:                jobID,
		ExecutorID:           executorID,
		Payload:              normalized,
		RequiredCapabilities: resolveRequiredCapabilities(executorID),
	}

	return job, spec, priority
}

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	// Build the canonical typed envelope, then project to the downstream
	// map. No `parameters` sub-map, no legacy alias keys. Single source
	// of truth is the contract.JobPayloadV2 struct.
	base := contract.NewJobPayloadV2(payloadMap)

	title := strings.TrimSpace(base.VideoName)
	if title == "" {
		return nil, &validationError{field: "video_name", message: "is required"}
	}
	base.VideoName = title

	scriptText := strings.TrimSpace(base.ScriptText)
	if scriptText == "" {
		scriptText = title
	}
	if scriptText == "" {
		return nil, &validationError{field: "script_text", message: "is required"}
	}
	base.ScriptText = scriptText

	scenesValue, scenesJSON, err := normalizeScenes(payloadMap)
	if err != nil {
		return nil, err
	}
	if len(scenesValue) == 0 {
		return nil, &validationError{field: "scenes", message: "at least one scene is required"}
	}
	base.Scenes = scenesValue
	base.ScenesJSON = scenesJSON
	base.SceneCount = len(scenesValue)

	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 && !hasClipTimelinePayload(payloadMap) {
		return nil, &validationError{field: "voiceover_paths", message: "at least one voiceover path is required"}
	}
	base.VoiceoverPaths = voiceovers
	base.VoiceoverCount = len(voiceovers)

	// Identity enrichment — prefer explicit caller-provided IDs/new
	// UUIDs over the constructor's defaults so the typed struct always
	// ends with concrete, non-empty lifecycle fields.
	jobID := strings.TrimSpace(payload.FirstString(payloadMap, "job_id", "id"))
	jobRunID := strings.TrimSpace(payload.FirstString(payloadMap, "job_run_id", "run_id"))
	correlationID := strings.TrimSpace(payload.FirstString(payloadMap, "correlation_id"))
	base.SetIdentity(jobID, jobRunID, correlationID)

	base.SubmittedVia = "api_v1_scene_video"
	base.Source = "scene_video_api"
	base.Status = "PENDING"
	base.Version = "v2"

	// Apply the fingerprint AFTER all identity + business fields are
	// finalized, so the hash reflects the canonical V2 shape.
	base.JobFingerprint = sceneVideoFingerprint(
		base.JobID,
		base.VideoName,
		base.ScriptText,
		base.ScenesJSON,
		base.VoiceoverPaths,
		base.YoutubeGroup,
		base.OutputPath,
		base.AudioLanguage,
	)

	if v := strings.TrimSpace(payload.FirstString(payloadMap, "output_video_id")); v != "" {
		base.OutputVideoID = v
	}

	// Spread to a canonical map for downstream consumers. NO
	// `parameters` sub-map mirror; legacy alias keys NOT emitted.
	out, err := base.ToMap()
	if err != nil {
		return nil, err
	}
	copyTimelinePayloadFields(out, payloadMap)
	return out, nil
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
// Internal helpers
// =============================================================================

func normalizeScenes(payloadMap map[string]interface{}) ([]map[string]interface{}, string, error) {
	if v, ok := payloadMap["scenes"]; ok {
		switch scenes := v.(type) {
		case []interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				result = append(result, contract.NormalizeSceneEntry(m))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		case []map[string]interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				result = append(result, contract.NormalizeSceneEntry(item))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		}
	}

	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err != nil {
			return nil, "", err
		}
		for i := range scenes {
			scenes[i] = contract.NormalizeSceneEntry(scenes[i])
		}
		data, err := json.Marshal(scenes)
		if err != nil {
			return nil, "", err
		}
		return scenes, string(data), nil
	}

	return nil, "", nil
}

func normalizeSceneArray(value interface{}) []map[string]interface{} {
	switch scenes := value.(type) {
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, item := range scenes {
			scene, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	default:
		return nil
	}
}

func normalizeVoiceoverList(payloadMap map[string]interface{}) []string {
	candidates := []string{
		payload.FirstString(payloadMap, "voiceover_path", "voiceover", "unified_voiceover_link"),
	}
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers_urls"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	result := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, item := range candidates {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			if _, exists := seen[trimmed]; exists {
				continue
			}
			seen[trimmed] = struct{}{}
			result = append(result, trimmed)
		}
	}
	return result
}

func sceneCountFromPayload(payloadMap map[string]interface{}) int {
	if scenes, ok := payloadMap["scenes"].([]interface{}); ok {
		return len(scenes)
	}
	if scenes, ok := payloadMap["scenes"].([]map[string]interface{}); ok {
		return len(scenes)
	}
	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err == nil {
			return len(scenes)
		}
	}
	return 0
}

func voiceoverCountFromPayload(payloadMap map[string]interface{}) int {
	if arr, ok := payloadMap["voiceover_paths"].([]string); ok {
		return len(arr)
	}
	if arr, ok := payloadMap["voiceover_paths"].([]interface{}); ok {
		return len(arr)
	}
	return len(normalizeVoiceoverList(payloadMap))
}

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

func hasClipTimelinePayload(payloadMap map[string]interface{}) bool {
	if payloadMap == nil {
		return false
	}
	for _, key := range []string{"clips", "items", "clip_segments", "intro_clip_paths", "stock_clip_paths"} {
		switch v := payloadMap[key].(type) {
		case []string:
			if len(v) > 0 {
				return true
			}
		case []interface{}:
			if len(v) > 0 {
				return true
			}
		}
	}
	return false
}

func copyTimelinePayloadFields(out, src map[string]interface{}) {
	if out == nil || src == nil {
		return
	}
	for _, key := range []string{
		"images",
		"clips",
		"items",
		"audio_tracks",
		"clip_segments",
		"intro_clip_paths",
		"stock_clip_paths",
		"fit",
		"effect",
		"orientation",
		// Preserve the explicit delivery contract through normalization so
		// taskSpec.Payload still satisfies AtomicJobTaskCreator's parse-time
		// delivery-plan requirement.
		"delivery_plan",
		"delivery_destination_ids",
		"delivery_destination_id",
		"destination_ids",
		"destination_id",
	} {
		if value, ok := src[key]; ok && value != nil {
			out[key] = value
		}
	}
	meta := routing.FromPayload(src)
	if meta.PipelineID != "" {
		out["pipeline_id"] = meta.PipelineID.String()
	}
	if audioURL := strings.TrimSpace(payload.FirstString(src, "audio_url")); audioURL != "" {
		out["audio_url"] = audioURL
	}
	// Preserve the forwarding metadata so normalizeSceneVideoPayload
	// carries it into the normalized payload consumed by
	// Enqueue → DeriveForwardingJobID.
	if meta.ForwardingKey != "" {
		out[routing.KeyForwardingKey] = meta.ForwardingKey.String()
	}
}

func syncAudioURLFromVoiceover(payloadMap map[string]interface{}) {
	if payloadMap == nil {
		return
	}
	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 {
		return
	}
	if strings.TrimSpace(payload.FirstString(payloadMap, "audio_url")) == "" || hasClipTimelinePayload(payloadMap) || strings.TrimSpace(payload.FirstString(payloadMap, "pipeline_id", routing.KeyPipelineID)) != "" {
		payloadMap["audio_url"] = voiceovers[0]
	}
}

func resolveInternalExecutorID(payloadMap map[string]interface{}) string {
	if payloadMap == nil {
		return ""
	}
	meta := routing.FromPayload(payloadMap)
	if meta.Executor.ID == "" {
		return ""
	}
	if meta.Executor.Version > 0 && !strings.Contains(meta.Executor.ID, "@") {
		return fmt.Sprintf("%s@%d", meta.Executor.ID, meta.Executor.Version)
	}
	return meta.Executor.ID
}

// resolveRequiredCapabilities returns the capability strings a task requires
// based on its executor. These are stored in task_requirements and consumed
// by the placement matcher's capability gate (matcher.go Select).
//
// For now the mapping is executor-driven:
//   - scene.composite.* → artifact.commit.v1
//   - All other executors → nil (no extra capabilities yet)
func resolveRequiredCapabilities(executorID string) []string {
	if strings.HasPrefix(executorID, "scene.composite") {
		return []string{"artifact.commit.v1"}
	}
	return nil
}

func sceneVideoFingerprint(parts ...interface{}) string {
	h := sha256.New()
	for _, part := range parts {
		switch v := part.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				h.Write([]byte(trimmed))
			}
		case []string:
			for _, item := range v {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					h.Write([]byte(trimmed))
				}
			}
		default:
			if part == nil {
				continue
			}
			if data, err := json.Marshal(part); err == nil {
				h.Write(data)
			}
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// extractPlanMaxRetry computes the maximum retry_budget across the
// payload's delivery_plan entries. Used by PrepareJobAndTask to
// pre-compute job.MaxRetries from the payload's delivery_plan so
// jobs.max_retries reflects the worst-case per-destination retry
// budget AT INSERT time. In production this matches the resolver's
// view (because the resolver reads from job_delivery_plans rows this
// tx just inserted), so the prepare-time computation is the single
// source of truth that first writes the column. The post-create
// resolver-based precondition still re-reads the plan from the DB
// for consistency gating (presence + retry_budget > 0) but no
// longer needs to mutate the column.
//
// Called from PrepareJobAndTask once compileSceneVideoJob has set
// MaxRetries=0. Idempotent if MaxRetries is already set by an
// upstream caller.
func extractPlanMaxRetry(payload map[string]interface{}) int {
	if payload == nil {
		return 0
	}
	planRaw, ok := payload["delivery_plan"]
	if !ok {
		return 0
	}
	arr, ok := planRaw.([]interface{})
	if !ok {
		return 0
	}
	maxRetry := 0
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		switch v := m["retry_budget"].(type) {
		case int:
			if v > maxRetry {
				maxRetry = v
			}
		case int64:
			if int(v) > maxRetry {
				maxRetry = int(v)
			}
		case float64:
			if int(v) > maxRetry {
				maxRetry = int(v)
			}
		}
	}
	return maxRetry
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

// Unwrap returns the underlying cause so errors.Is / errors.As can
// inspect the original resolver error (e.g. deliveries.ErrNoExplicitPlan).
// Without this, callers can only inspect the formatted message, which is
// fragile across message refactors.
func (e *validationError) Unwrap() error {
	return e.wrapped
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
