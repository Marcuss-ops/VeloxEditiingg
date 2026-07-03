// Package enqueue fornisce funzioni condivise per la normalizzazione, il building e
// l'inoltro di job video (process_video) nella coda. Usato da endpoint canonici come
// script/generate-with-images e pipeline.
//
// PR #3 (refactor/single-execution-create): Enqueuer is now a Compiler — it
// normalizes, validates, resolves voiceover/scene-image assets, compiles a
// TaskSpec, and delegates to store.AtomicJobTaskCreator for atomic Job+Task
// creation. The JobQueue interface and writerAdapter are eliminated. ALL
// producers (HTTP, creator result, calendar) route through the single atomic
// creation path.
package enqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
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
// PR #3: Queue (JobQueue) removed. The Enqueuer now compiles a Job+TaskSpec
// and delegates to Creator (AtomicJobTaskCreator) for atomic insertion.
type Enqueuer struct {
	Creator   *store.AtomicJobTaskCreator
	Jobs      jobs.Reader
	Voiceover *assetbridge.AssetService
}

// NewEnqueuer constructs an Enqueuer with mandatory Creator + Jobs.
// The voiceover service is optional (nil-safe: voiceover resolution is skipped).
func NewEnqueuer(creator *store.AtomicJobTaskCreator, jobsRepo jobs.Reader, voiceover *assetbridge.AssetService) *Enqueuer {
	return &Enqueuer{Creator: creator, Jobs: jobsRepo, Voiceover: voiceover}
}

// =============================================================================
// Core enqueue entry point
// =============================================================================

// Enqueue is the canonical scene-video enqueue. The Enqueuer owns both
// the atomic creator + asset service so rewrite invariants are applied
// exactly once before the atomic Job+Task creation.
//
// PR #3: instead of delegating to Queue.SubmitJob (Job-only), the Enqueuer
// now compiles a Job+TaskSpec from the normalized payload and calls
// Creator.CreateJobWithTask(ctx, job, spec, priority) for atomic
// Job+Task insertion. Jobs is used for idempotency pre-check.
//
// PR-04.5: callers MUST publish the per-job `costmodel.JobRequirements`
// for the eligibility layer + future-rank site to consume.
//
// PR-forwarding-deterministic-id: when the payload carries
// `_internal_forwarding_key`, the job_id is derived deterministically
// from that key (via DeriveForwardingJobID) instead of generating a
// random UUID. This ensures concurrent pollers, duplicate webhooks, and
// post-crash retries always produce the same Job ID.
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

	return buildSceneVideoResponse(normalized), nil
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

	jobID, _ := normalized["job_id"].(string)

	// PR-forwarding-deterministic-id: when a forwarding key is present,
	// derive the job_id deterministically regardless of any auto-generated
	// ID from NewJobPayloadV2 or SetIdentity. This ensures concurrent
	// pollers, duplicate webhooks, and post-crash retries always produce
	// the same Job ID.
	fwdMeta := routing.FromPayload(normalized)
	if fwdMeta.ForwardingKey != "" {
		jobID = DeriveForwardingJobID(fwdMeta.ForwardingKey.String())
		normalized["job_id"] = jobID
	}

	if jobID == "" {
		jobID = uuid.NewString()
		normalized["job_id"] = jobID
	}

	// PR #3: compile Job+TaskSpec.
	job, spec, priority := compileSceneVideoJob(normalized, req)
	return job, spec, priority, nil
}

// =============================================================================
// Job response formatter (shared across endpoints)
// =============================================================================

// RenderHTTPBoundaryJobResponse builds the HTTP-edge JSON response map for
// a job record, READing via legacy-alias-tolerant fallbacks so old SQLite
// rows that still carry `id`/`run_id`/`title`/`voiceover_path`/`audio_path`
// (written before PR15.6) continue to render correctly.
//
// PR15.6: renamed from RenderJobResponse. The function is the sole canonical-
// to-alias adapter at the HTTP boundary; internal callers (script handler,
// creatorflow, pipeline) all consume canonical keys already. ONLY this
// helper tolerates dual-write reads.
func RenderHTTPBoundaryJobResponse(job map[string]interface{}, full bool) map[string]interface{} {
	if job == nil {
		return map[string]interface{}{"ok": false}
	}
	response := map[string]interface{}{
		"ok":     true,
		"job_id": payload.FirstString(job, "job_id"),
		// legacy aliases kept only on HTTP-edge reads (PR15.6). The
		// chain tolerates rows written before PR15.6 that still carry
		// `id` (HTTP01 subtest basic_legacy_alias_fallback) — `id` is
		// consulted LAST so canonical `job_id` wins when present.
		"script_id":           payload.FirstString(job, "job_id", "script_id", "id"),
		"status":              payload.FirstString(job, "status"),
		"video_name":          payload.FirstString(job, "video_name", "title"),
		"job_run_id":          payload.FirstString(job, "job_run_id", "run_id"),
		"run_id":              payload.FirstString(job, "run_id", "job_run_id"),
		"created_at":          job["created_at"],
		"updated_at":          job["updated_at"],
		"started_at":          job["started_at"],
		"completed_at":        job["completed_at"],
		"output_path":         payload.FirstString(job, "output_path"),
		"drive_output_folder": ResolveDriveOutputFolderReference(os.Getenv("VELOX_DATA_DIR"), payload.FirstString(job, "drive_output_folder")),
		"scene_count":         job["scene_count"],
		"voiceover_count":     job["voiceover_count"],
		"video_mode":          payload.FirstString(job, "video_mode"),
	}
	if errMsg := payload.FirstString(job, "error", "last_error", "error_message"); errMsg != "" {
		response["error"] = errMsg
	}
	if result := job["result"]; result != nil {
		response["result"] = result
	}
	if full {
		response["job"] = job
		response["request"] = job["request"]
	}
	return response
}

// =============================================================================
// PR #3: compile Job+TaskSpec from normalized scene-video payload
// =============================================================================

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
		ID:           jobID,
		Type:         jobType,
		Status:       jobs.StatusPending,
		VideoName:    videoName,
		ProjectID:    projectID,
		RunID:        jobRunID,
		MaxRetries:   3,
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
	// refactor/payload-v2-single-shape: build the canonical typed
	// envelope, then project to the downstream map. No `parameters`
	// sub-map. No legacy alias keys. Single source of truth is the
	// contract.JobPayloadV2 struct.
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
		resp["job_run_id"] = jobRunID
		resp["run_id"] = jobRunID
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
	for _, key := range []string{"images", "clips", "items", "audio_tracks", "clip_segments", "intro_clip_paths", "stock_clip_paths", "fit", "effect", "orientation"} {
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
	// PR-forwarding-deterministic-id: preserve the forwarding metadata so
	// normalizeSceneVideoPayload carries it into the normalized payload
	// consumed by Enqueue → DeriveForwardingJobID.
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
}

func (e *validationError) Error() string {
	return e.field + ": " + e.message
}
