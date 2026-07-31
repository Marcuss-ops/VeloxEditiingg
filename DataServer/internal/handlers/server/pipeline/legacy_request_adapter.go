// Package pipeline — legacy_request_adapter.go isolates compatibility-only intake behavior.
package pipeline

// emitLegacyRequestWarning emits the existing non-blocking migration signal.
// It intentionally preserves the previous detection, metric, and log behavior.
func (h *Handlers) emitLegacyRequestWarning(req SubmitJobRequest) {
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
}
