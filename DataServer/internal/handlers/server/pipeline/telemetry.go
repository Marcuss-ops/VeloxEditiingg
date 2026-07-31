// Package pipeline — telemetry.go owns:
//   - isLegacyCompatShape: pre-manifest_ref legacy-shape detector.
//   - countScenesWithClipLink: migration-dashboard counter helper.
//   - legacyBodySinkOrNoop: nil-safe legacy sink selector.
//
// The warning emission itself lives in legacy_request_adapter.go;
// this file owns only detection and telemetry plumbing.
package pipeline

import (
	"strings"
)

// isLegacyCompatShape reports whether the SubmitJobRequest carries
// the pre-manifest_ref compatibility body shape. Pure function: no
// handler/sink access. Used by NormalizeExternalJobSubmission to gate
// the legacy-body-shape warning emit; the test suite calls it
// directly to pin the detection criteria.
func isLegacyCompatShape(req SubmitJobRequest) bool {
	if len(req.VoiceoverPaths) > 0 {
		return true
	}
	if len(req.SubtitleTracks) > 0 {
		return true
	}
	for _, s := range req.Scenes {
		if strings.TrimSpace(s.ClipLink) != "" {
			return true
		}
	}
	return false
}

// countScenesWithClipLink returns the number of scenes whose
// pre-manifest_ref flat `clip_link` field is non-empty. Used by the
// legacy-body-shape warning log line so operators can see the per-
// scene distribution in the compat body without grepping every
// scene in the structured log.
func countScenesWithClipLink(scenes []SubmitScene) int {
	n := 0
	for _, s := range scenes {
		if strings.TrimSpace(s.ClipLink) != "" {
			n++
		}
	}
	return n
}

// legacyBodySinkOrNoop returns the wired legacy-body-shape sink or a
// noop if not set. Mirrors intakeSinkOrNoop's contract: the handler
// never panics on a missing wiring and never silently drops a metric
// event.
func (h *Handlers) legacyBodySinkOrNoop() LegacyBodySink {
	if h.legacyBodySink == nil {
		return noopLegacyBodySink{}
	}
	return h.legacyBodySink
}
