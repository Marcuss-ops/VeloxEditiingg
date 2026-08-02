package enqueue

// enqueue_assets.go: asset-rewrite + response-builder helpers of the
// Enqueuer. Split out of enqueue.go; the core orchestration and the
// shared plan types (PlanDestination / ResolvedPlan / PlanResolver)
// live in enqueue.go.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	assetbridge "velox-server/internal/assets"
	"velox-server/internal/jobs"
	"velox-server/internal/telemetry"
	"velox-shared/payload"
)

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
// Response builders
// =============================================================================

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
