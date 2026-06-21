// Package enqueue fornisce funzioni condivise per la normalizzazione, il building e
// l'inoltro di job video (process_video) nella coda. Usato da endpoint canonici come
// script/generate-with-images e pipeline.
//
// PR15.7a (enqueuer-struct): the package-level voiceoverAssetService mutex-guarded
// global was removed. The single entry point is now `*Enqueuer`: callers construct
// one with NewEnqueuer(queue, voiceoverService) and call .Enqueue(ctx, payload).
// All rewrite invariants (voiceover + scene-image) live behind the Enqueuer
// methods so the queue+service travel together as injected dependencies and
// there is no longer any package-level mutable state to coordinate.
package enqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	assetbridge "velox-server/internal/assets"
	"velox-server/internal/costmodel"
	"velox-shared/contract"
	"velox-shared/payload"

	"context"

	"github.com/google/uuid"
)

// JobQueue is the minimal surface Enqueuer depends on. jobs.Writer
// (and any future in-memory or Redis-backed queue) satisfies this via writerAdapter.
//
// PR-04.5: SubmitJob now carries the per-job costmodel.JobRequirements.
// The requirements are persisted to SQLite alongside the request_json
// blob (dedicated columns _plus_ a `_requirements` JSON sub-object for
// redundancy). The canonical read path is
// `jobs.Writer.Get → jobs.Job.Requirements`; future rank sites (PR-04.6)
// and `Registry.GetEligibleWorkers(ctx, req)` consume from there.
type JobQueue interface {
	SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}, req costmodel.JobRequirements) error
}

// Enqueuer bundles the queue + the asset service that rewrites voiceover
// and scene-image payload references. Construct via NewEnqueuer.
//
// Replaces the package-level voiceoverAssetService global (PR15.7a).
// Tests can construct an Enqueuer directly without mutating shared state,
// so each test owns its own dependencies.
type Enqueuer struct {
	Queue     JobQueue
	Voiceover *assetbridge.AssetService
}

// NewEnqueuer constructs an Enqueuer with mandatory Queue. The voiceover
// service is optional (nil-safe: voiceover resolution is skipped).
func NewEnqueuer(q JobQueue, voiceover *assetbridge.AssetService) *Enqueuer {
	return &Enqueuer{Queue: q, Voiceover: voiceover}
}

// =============================================================================
// Core enqueue entry point
// =============================================================================

// Enqueue is the canonical scene-video enqueue. The Enqueuer owns both
// the queue and the voiceover/scene-image asset service so rewrite
// invariants are applied exactly once before submission, with no
// package-level mutable state involved.
//
// PR-04.5: callers MUST publish the per-job
// `costmodel.JobRequirements` for the eligibility layer + future-rank
// site to consume. Legacy callers that have not yet decided on
// requirements should pass `costmodel.DefaultRequirements()` (an
// empty JobRequirements = permissive default = preserves today's
// FIFO queue routing). Callers MUST construct an Enqueuer (typically
// once at composition-root time) and pass it down via DI; there is
// no package-level fallback.
//
// The Requirements are NOT injected into the `payload` map here; the
// downstream adapter (jobs.Writer.Create) is responsible for
// embedding them once in BOTH the dedicated columns AND the
// `_requirements` JSON sub-object inside request_json. This split
// keeps the Enqueuer free of SQLite-specific knowledge.
func (e *Enqueuer) Enqueue(ctx context.Context, payloadMap map[string]interface{}, req costmodel.JobRequirements) (map[string]interface{}, error) {
	if e == nil || e.Queue == nil {
		return nil, fmt.Errorf("queue unavailable")
	}

	if err := e.resolveVoiceoverPayload(ctx, payloadMap); err != nil {
		return nil, err
	}
	if err := e.resolveSceneImagePayload(ctx, payloadMap); err != nil {
		return nil, err
	}

	normalized, err := normalizeSceneVideoPayload(payloadMap)
	if err != nil {
		return nil, err
	}

	jobID, _ := normalized["job_id"].(string)
	if jobID == "" {
		jobID = uuid.NewString()
		normalized["job_id"] = jobID
	}

	if err := e.Queue.SubmitJob(ctx, jobID, normalized, req); err != nil {
		return nil, err
	}

	return buildSceneVideoResponse(normalized), nil
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
		"ok":                  true,
		"job_id":              payload.FirstString(job, "job_id"),
		// legacy aliases kept only on HTTP-edge reads (PR15.6)
		"script_id":           payload.FirstString(job, "job_id", "script_id"),
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
// Internal: scene video payload normalization (used by *Enqueuer.Enqueue)
// =============================================================================

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	title := strings.TrimSpace(payload.FirstString(payloadMap, "video_name", "title", "project_name"))
	if title == "" {
		return nil, &validationError{field: "video_name", message: "is required"}
	}

	scriptText := strings.TrimSpace(payload.FirstString(payloadMap, "script_text", "script", "source_text", "title", "video_name"))
	if scriptText == "" {
		scriptText = title
	}
	if scriptText == "" {
		return nil, &validationError{field: "script_text", message: "is required"}
	}

	scenesValue, scenesJSON, err := normalizeScenes(payloadMap)
	if err != nil {
		return nil, err
	}
	if len(scenesValue) == 0 {
		return nil, &validationError{field: "scenes", message: "at least one scene is required"}
	}

	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 {
		return nil, &validationError{field: "voiceover_paths", message: "at least one voiceover path is required"}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	jobID := strings.TrimSpace(payload.FirstString(payloadMap, "job_id", "id"))
	if jobID == "" {
		jobID = "scene_" + uuid.NewString()
	}
	jobRunID := strings.TrimSpace(payload.FirstString(payloadMap, "job_run_id", "run_id"))
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := strings.TrimSpace(payload.FirstString(payloadMap, "correlation_id"))
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}
	jobFingerprint := sceneVideoFingerprint(
		jobID,
		title,
		scriptText,
		scenesJSON,
		voiceovers,
		payload.FirstString(payloadMap, "youtube_group"),
		payload.FirstString(payloadMap, "output_path"),
		payload.FirstString(payloadMap, "audio_language_for_srt", "audio_lang"),
	)

	normalized := make(map[string]interface{}, len(payloadMap)+16)
	for k, v := range payloadMap {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["created_at"] = payload.EnsureRFC3339(payload.FirstString(payloadMap, "created_at"), now)
	normalized["updated_at"] = payload.EnsureRFC3339(payload.FirstString(payloadMap, "updated_at"), now)
	normalized["video_name"] = title
	normalized["script_text"] = scriptText
	normalized["scenes"] = scenesValue
	normalized["scenes_json"] = scenesJSON
	normalized["voiceover_paths"] = voiceovers
	normalized["priority"] = payload.EnsureInt(payloadMap["priority"], 1)
	normalized["timeout_secs"] = payload.EnsureInt(payloadMap["timeout_secs"], 3600)
	normalized["scene_count"] = len(scenesValue)
	normalized["voiceover_count"] = len(voiceovers)
	normalized["submitted_via"] = "api_v1_scene_video"
	normalized["source"] = "scene_video_api"
	normalized["job_fingerprint"] = jobFingerprint
	normalized["version"] = "v1"
	// PR15.6: canonical-only parameters mirror. Legacy aliases
	// (id, run_id, title, voiceover_path, audio_path) are NOT written here
	// — they live in older rows and are read on the HTTP edge by the
	// boundary adapter (RenderHTTPBoundaryJobResponse).
	normalized["parameters"] = map[string]interface{}{
		"version":         "v1",
		"job_id":          jobID,
		"job_run_id":      jobRunID,
		"correlation_id":  correlationID,
		"job_type":        "process_video",
		"video_name":      title,
		"script_text":     scriptText,
		"scenes_json":     scenesJSON,
		"scenes":          scenesValue,
		"voiceover_paths": voiceovers,
		"youtube_group":   payload.FirstString(payloadMap, "youtube_group"),
		"output_path":     payload.FirstString(payloadMap, "output_path"),
		"job_fingerprint": jobFingerprint,
		"submitted_via":   "api_v1_scene_video",
		"source":          "scene_video_api",
		"scene_count":     len(scenesValue),
		"voiceover_count": len(voiceovers),
		"priority":        payload.EnsureInt(payloadMap["priority"], 1),
		"timeout_secs":    payload.EnsureInt(payloadMap["timeout_secs"], 3600),
	}

	if v := strings.TrimSpace(payload.FirstString(payloadMap, "youtube_group")); v != "" {
		normalized["youtube_group"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "output_video_id")); v != "" {
		normalized["output_video_id"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "audio_language_for_srt", "audio_lang")); v != "" {
		normalized["audio_language_for_srt"] = v
		normalized["parameters"].(map[string]interface{})["audio_language_for_srt"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "output_path")); v != "" {
		normalized["output_path"] = v
		normalized["parameters"].(map[string]interface{})["output_path"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "scene_image_paths")); v != "" {
		normalized["scene_image_paths"] = v
		normalized["parameters"].(map[string]interface{})["scene_image_paths"] = v
	}

	return normalized, nil
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
	return rewriteVoiceoverPayloadFor(ctx, e.Voiceover, payloadMap)
}

func (e *Enqueuer) resolveSceneImagePayload(ctx context.Context, payloadMap map[string]interface{}) error {
	if e == nil {
		return nil
	}
	return rewriteSceneImagePayloadFor(ctx, e.Voiceover, payloadMap)
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

type validationError struct {
	field   string
	message string
}

func (e *validationError) Error() string {
	return e.field + ": " + e.message
}
