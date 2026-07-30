// Package pipeline — plan_derivation.go owns the
// typed-DTO → worker payload normalization for
// POST /api/v1/jobs:
//   - ExternalAPISourceProvider / JobSubmitTargetExecutorID
//     identity-tuple constants (low-cardinality provider stamp
//     on forwardings).
//   - clipToMap / voiceoverToMap / subtitlesToMap per-scene
//     nested-asset map builders (return nil when source is
//     nil so the caller appends without explicit nil-check).
//   - NormalizeExternalJobSubmission: typed-DTO →
//     *CanonicalCompletedPayload adapter that walks the SAME
//     path as creator_push.normalizeCreatorPushRequest (raw
//     map → remoteengine.ParseRemotePipelineResult →
//     ToWorkerPayload).
//   - submitRequestToRawPayload: flat-map shim that bridges
//     Submit* typed structs into the canonical envelope shape
//     consumed by remoteengine. Owns the *int RetryBudget
//     round-trip boundary (nil → DefaultRetryBudget, *0 → 0,
//     *>0 → *d).
//
// Caller in this package: job_submit.go (thin composer).
package pipeline

import (
	"strings"
	"velox-server/internal/remoteengine"
)

// truncation mid-rune at the 32-byte boundary. A fixed provider also
// lets the runner honour a future "client_id" extension without
// touching the schema contract again.
const ExternalAPISourceProvider = "external_api"

// JobSubmitTargetExecutorID is the canonical executor that POST /api/v1/jobs
// requests dispatch to. It is the same as Creator-push's default so the
// same worker pool services both intake paths.
const JobSubmitTargetExecutorID = "scene.composite.v1"

// MaxVideoNameBytes is the byte-length cap on SubmitJobRequest.video_name.
// The cap protects log-line printers (some wire up to 4096-byte lines)
// and prevents accidental "paste the entire script in the title" classes
// of misuse. Picked to comfortably fit real video titles; raise only
// with operator approval.
//
// MUST mirror openapi.yaml:SubmitJobRequest.video_name.maxLength.
// Drift here silently breaks the contract: clients receive 422 for
// names the spec claimed were valid.

// "Trim policy" block). Asset-id and language are passed verbatim
// (they're not URL-shaped so the parser doesn't care about whitespace).
func clipToMap(c *SubmitClip) map[string]interface{} {
	if c == nil {
		return nil
	}
	out := map[string]interface{}{}
	if c.AssetID != "" {
		out["asset_id"] = c.AssetID
	}
	if c.DriveFileID != "" {
		out["drive_file_id"] = c.DriveFileID
	}
	if c.URL != "" {
		out["url"] = strings.TrimSpace(c.URL)
	}
	if c.SHA256 != "" {
		out["sha256"] = c.SHA256
	}
	if c.StartMS > 0 {
		out["start_ms"] = c.StartMS
	}
	if c.EndMS > 0 {
		out["end_ms"] = c.EndMS
	}
	if c.DurationMS > 0 {
		out["duration_ms"] = c.DurationMS
	}
	return out
}

func voiceoverToMap(v *SubmitVoiceover) map[string]interface{} {
	if v == nil {
		return nil
	}
	out := map[string]interface{}{}
	if v.AssetID != "" {
		out["asset_id"] = v.AssetID
	}
	if v.DriveFileID != "" {
		out["drive_file_id"] = v.DriveFileID
	}
	if v.URL != "" {
		out["url"] = strings.TrimSpace(v.URL)
	}
	if v.SHA256 != "" {
		out["sha256"] = v.SHA256
	}
	if v.DurationMS > 0 {
		out["duration_ms"] = v.DurationMS
	}
	if v.Language != "" {
		out["language"] = v.Language
	}
	return out
}

func subtitlesToMap(s *SubmitSubtitles) map[string]interface{} {
	if s == nil {
		return nil
	}
	out := map[string]interface{}{}
	if s.AssetID != "" {
		out["asset_id"] = s.AssetID
	}
	if s.Format != "" {
		out["format"] = s.Format
	}
	if s.URL != "" {
		out["url"] = strings.TrimSpace(s.URL)
	}
	if s.SHA256 != "" {
		out["sha256"] = s.SHA256
	}
	if s.Language != "" {
		out["language"] = s.Language
	}
	return out
}

// audioTrackToMap converts a SubmitAudioTrack to the canonical
// worker-payload shape consumed by the hybrid.v1 compiler. The
// shape matches plan.AudioTrack (source_url, volume, role,
// start_time_offset, duration_seconds, loop, fade_in/out_seconds,
// ducking_enabled) plus the optional asset_id for Master-side
// resolution.
func audioTrackToMap(t SubmitAudioTrack) map[string]interface{} {
	out := map[string]interface{}{}
	if trimmed := strings.TrimSpace(t.SourceURL); trimmed != "" {
		out["source_url"] = trimmed
	}
	if t.AssetID != "" {
		out["asset_id"] = t.AssetID
	}
	if t.Role != "" {
		out["role"] = t.Role
	}
	if t.Volume > 0 {
		out["volume"] = t.Volume
	}
	if t.StartTimeOffset > 0 {
		out["start_time_offset"] = t.StartTimeOffset
	}
	if t.DurationSeconds > 0 {
		out["duration_seconds"] = t.DurationSeconds
	}
	if t.Loop {
		out["loop"] = true
	}
	if t.FadeInSeconds > 0 {
		out["fade_in_seconds"] = t.FadeInSeconds
	}
	if t.FadeOutSeconds > 0 {
		out["fade_out_seconds"] = t.FadeOutSeconds
	}
	if t.DuckingEnabled {
		out["ducking_enabled"] = true
	}
	return out
}

// SubmitJobRequest is the simplified, versioned API contract for
// POST /api/v1/jobs. It allows external systems to submit complete
// video jobs without going through the Creator intermediary.
//
// The format is intentionally flat and intuitive:
//   - No nested "payload" envelope
//   - No source_provider/source_job_id/target_executor_id ceremony
//   - idempotency_key is the single dedup handle
//
// The system derives the Creator-compatible identity tuple
// (source_provider, source_job_id, target_executor_id) automatically.
//
// Field validation rules — every contract on this struct mirrors
// fields on openapi.yaml's SubmitJobRequest schema. The handler uses
// strict JSON decoding (DisallowUnknownFields) + ValidateSubmitJobRequest
// (the canonical programmatic validator) so the schema and the Go
// code can stay in lockstep without relying on Gin binding tags,
// which silently default to permissive behaviour for missing
// cross-field constraints.

// Trim policy in submitRequestToRawPayload: trim SPACE around
// identity-bearing fields (IdempotencyKey, VideoName, scene
// clip_link / image_link, delivery destination_id) because these
// participate in dedup / URL parsing downstream. Do NOT trim
// ScriptText or scene `text` — these are CONTENT fields where
// legitimate whitespace might be present.
func (h *Handlers) NormalizeExternalJobSubmission(req SubmitJobRequest) *CanonicalCompletedPayload {
	// Legacy-body-shape warning (P1): when the request body carries
	// the pre-manifest_ref compat shape (top-level voiceover_paths[]
	// / scenes[N].clip_link / subtitle_tracks[]) AND no manifest_ref
	// was supplied, emit a structured metric + a structured log line
	// so operators can monitor PipelineGen migration progress. The
	// emit is INTENTIONALLY NON-BLOCKING — the submission still
	// passes through the canonical resolver path; only the operator-
	// visible signal fires. The compat path stays open until the
	// PipelineGen manifest_ref migration is complete; the metric is
	// the durability signal, not a gate.
	//
	// Detection criteria (any of):
	//   - len(req.VoiceoverPaths) > 0      — pre-manifest_ref top-level voiceover list
	//   - any req.Scenes[i].ClipLink != "" — pre-manifest_ref flat per-scene clip
	//   - len(req.SubtitleTracks) > 0     — pre-manifest_ref top-level subtitle tracks
	//
	// A scene with the new nested Clip{}/Voiceover{}/Subtitles{}
	// objects is NOT a legacy-shape signal (the per-scene enrichment
	// is the migration target). A body that ALSO supplies
	// manifest_ref is also NOT a legacy-shape signal — the client has
	// migrated and the resolver will use the manifest side instead.
	if req.ManifestRef == nil && isLegacyCompatShape(req) {
		if sink := h.legacyBodySinkOrNoop(); sink != nil {
			sink.IncLegacyBody(LegacyBodySinkClientKindPreManifestRef)
		}
		pipelineLog(
			"LEGACY_BODY_WARNING client_kind=%s idempotency_hash=%s voiceover_paths=%d scenes_with_clip_link=%d subtitle_tracks=%d manifest_ref=absent",
			LegacyBodySinkClientKindPreManifestRef,
			logHashShort(req.IdempotencyKey),
			len(req.VoiceoverPaths),
			countScenesWithClipLink(req.Scenes),
			len(req.SubtitleTracks),
		)
	}

	rawPayload := submitRequestToRawPayload(&req)

	dto, _ := remoteengine.ParseRemotePipelineResult(rawPayload)
	workerPayload := dto.ToWorkerPayload()
	preserveWorkerPayloadFields(workerPayload, rawPayload, "subtitle_tracks", "audio_tracks", "layers", "_placement_pin_worker_id")

	return &CanonicalCompletedPayload{
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      req.IdempotencyKey,
		TargetExecutorID: JobSubmitTargetExecutorID,
		WorkerPayload:    workerPayload,
	}
}

func preserveWorkerPayloadFields(dst, src map[string]interface{}, keys ...string) {
	if dst == nil || src == nil {
		return
	}
	for _, key := range keys {
		if value, ok := src[key]; ok && value != nil {
			dst[key] = value
		}
	}
}

// isLegacyCompatShape reports whether the SubmitJobRequest carries
// the pre-manifest_ref compatibility body shape. Pure function: no
// handler/sink access. Used by NormalizeExternalJobSubmission to gate
// the legacy-body-shape warning emit; the test suite calls it
// directly to pin the detection criteria.

// Trim policy (matching NormalizeExternalJobSubmission's contract):
// identity-bearing fields are trimmed (IdempotencyKey, VideoName,
// URL-shaped scene clip_link / image_link, delivery destination_id,
// scene nested object URLs). CONTENT fields (ScriptText, scene text)
// are passed through verbatim because legitimate whitespace might
// appear.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	m := map[string]interface{}{
		"status": "completed",
		"job_id": strings.TrimSpace(req.IdempotencyKey),
	}

	if req.VideoName != "" {
		m["video_name"] = strings.TrimSpace(req.VideoName)
	}
	if req.ScriptText != "" {
		m["script_text"] = req.ScriptText
	}
	if req.ResolvedManifest != nil {
		m["render_manifest"] = req.ResolvedManifest
	}
	if req.ResolvedManifestRef != nil {
		m["manifest_ref"] = req.ResolvedManifestRef
	}
	if req.ResolvedManifestSHA256 != "" {
		m["manifest_sha256"] = req.ResolvedManifestSHA256
	}
	if len(req.VoiceoverPaths) > 0 {
		// NormalizeToStrings shape matches what
		// extractVoiceoverPathsDTO scans for.
		//
		// Phase-2 note: the per-scene voiceover.url (when present)
		// is the SOURCE OF TRUTH; this top-level array is preserved
		// for back-compat with legacy worker consumers that read
		// voiceover_paths[] directly. ToWorkerPayload (remoteengine
		// side) merges both sources into a single deduped array
		// so the legacy field stays consistent for old workers
		// even when new clients send only the per-scene form.
		m["voiceover_paths"] = req.VoiceoverPaths
	}

	if len(req.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(req.Scenes))
		for i, s := range req.Scenes {
			scene := map[string]interface{}{
				"text":             s.Text,
				"duration_seconds": s.DurationSeconds,
			}
			if s.SceneID != "" {
				scene["scene_id"] = strings.TrimSpace(s.SceneID)
			}
			if s.Index > 0 {
				scene["index"] = s.Index
			}
			if s.Kind != "" {
				scene["kind"] = strings.TrimSpace(s.Kind)
			}
			// Legacy flat-shape alias keys: preserved verbatim when
			// supplied, so old clients that haven't migrated still
			// see a working round-trip. When the nested Clip{}
			// also carries a URL, BOTH end up in the map — the
			// worker's scenes_json consumer picks the nested form
			// (authoritative) but the legacy key remains visible
			// to any code that still reads `clip_link` directly.
			if s.ClipLink != "" {
				scene["clip_link"] = strings.TrimSpace(s.ClipLink)
			}
			if s.ImageLink != "" {
				scene["image_link"] = strings.TrimSpace(s.ImageLink)
			}
			// Per-scene nested objects (Phase 2): clip / voiceover /
			// subtitles carry their own asset references so the
			// worker reads the canonical URL directly from
			// scenes_json[i].voiceover.url (no more positional
			// coupling with top-level voiceover_paths[]).
			if s.Clip != nil {
				scene["clip"] = clipToMap(s.Clip)
			}
			if s.Voiceover != nil {
				scene["voiceover"] = voiceoverToMap(s.Voiceover)
			} else if i < len(req.VoiceoverPaths) && strings.TrimSpace(req.VoiceoverPaths[i]) != "" {
				scene["voiceover"] = map[string]interface{}{
					"url": strings.TrimSpace(req.VoiceoverPaths[i]),
				}
			}
			if s.Subtitles != nil {
				scene["subtitles"] = subtitlesToMap(s.Subtitles)
			}
			scenes = append(scenes, scene)
		}
		m["scenes"] = scenes
	}
	if len(req.Layers) > 0 {
		layers := make([]interface{}, 0, len(req.Layers))
		for _, layer := range req.Layers {
			entry := map[string]interface{}{"id": strings.TrimSpace(layer.ID), "type": strings.TrimSpace(layer.Type)}
			for key, value := range map[string]interface{}{
				"role": layer.Role, "text": layer.Text, "asset": layer.Asset, "source": layer.Source,
				"font": layer.Font, "font_size": layer.FontSize, "position": layer.Position,
				"start_seconds": layer.StartSeconds, "duration_seconds": layer.DurationSeconds,
				"preset": layer.Preset, "animation": layer.Animation,
			} {
				if value != nil && value != "" && !(key == "position" && len(layer.Position) == 0) && !(key != "position" && value == float64(0)) {
					entry[key] = value
				}
			}
			layers = append(layers, entry)
		}
		m["layers"] = layers
	}
	if len(req.SubtitleTracks) > 0 {
		subtitles := make([]interface{}, 0, len(req.SubtitleTracks))
		for _, track := range req.SubtitleTracks {
			subtitles = append(subtitles, map[string]interface{}{"source": strings.TrimSpace(track.Source), "preset": track.Preset, "font": track.Font})
		}
		m["subtitle_tracks"] = subtitles
	}
	if len(req.AudioTracks) > 0 {
		audioTracks := make([]interface{}, 0, len(req.AudioTracks))
		for _, track := range req.AudioTracks {
			audioTracks = append(audioTracks, audioTrackToMap(track))
		}
		m["audio_tracks"] = audioTracks
	}

	if req.PlacementPinWorkerID != "" {
		m["_placement_pin_worker_id"] = strings.TrimSpace(req.PlacementPinWorkerID)
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": strings.TrimSpace(d.DestinationID),
			}
			if d.Priority > 0 {
				entry["priority"] = d.Priority
			}
			if d.RetryBudget == nil {
				entry["retry_budget"] = DefaultRetryBudget
			} else {
				entry["retry_budget"] = *d.RetryBudget
			}
			if d.Metadata != nil {
				entry["metadata"] = d.Metadata
			}
			plan = append(plan, entry)
		}
		m["delivery_plan"] = plan
	}

	return m
}
