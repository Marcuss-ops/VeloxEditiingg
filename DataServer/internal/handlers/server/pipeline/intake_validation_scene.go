package pipeline

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
)

func validateSubmitScenes(req SubmitJobRequest) []gin.H {
	var details []gin.H
	// Per-scene validation: text non-empty, duration in [0.1, 86400].
	for i, s := range req.Scenes {
		pathPrefix := fmt.Sprintf("scenes.%d", i)
		if strings.TrimSpace(s.Text) == "" {
			details = append(details, gin.H{
				"path":  pathPrefix + ".text",
				"issue": "empty",
			})
		}
		if s.DurationSeconds < MinSceneDurationSeconds || s.DurationSeconds > MaxSceneDurationSeconds {
			details = append(details, gin.H{
				"path":     pathPrefix + ".duration_seconds",
				"issue":    "out_of_range",
				"min":      MinSceneDurationSeconds,
				"max":      MaxSceneDurationSeconds,
				"observed": s.DurationSeconds,
			})
		}
	}

	// Per-scene nested-asset validation. Runs ONLY when the scene
	// at index i has a non-nil Clip / Voiceover / Subtitles
	// pointer. A nil pointer is the canonical "scene carries no
	// clip/vo/sub" path and MUST pass silently — legacy clients
	// never sent nested objects, so every existing client is
	// unaffected by the new fields' presence.
	//
	// Shape rules (matching apiwire validate tags):
	//   - URL: must be non-empty after trim and must match the
	//     http(s) + velox-asset:// + velox-drive:// scheme
	//     allow-list (the self-sufficient wire; the SSRF blocklist
	//     layer downstream enforces the egress policy separately).
	//   - SHA256: must be exactly 64 lowercase hex chars.
	//   - Subtitles.format: closed enum (ass / srt / vtt).
	//   - Language: 2-byte ISO 639-1 (best-effort — not strictly
	//     validated against the full ISO list; an empty string is
	//     permitted because the worker can fall back to the
	//     project default).
	//
	// An empty-object nested (pointer non-nil with all fields
	// empty) is rejected with at least one violation per nested
	// object that has an empty URL — the canonical "client sent
	// {}" shape must not silently pass.
	for i, s := range req.Scenes {
		if s.Clip != nil {
			pathPrefix := fmt.Sprintf("scenes.%d.clip", i)
			if trimmed := strings.TrimSpace(s.Clip.URL); trimmed == "" {
				details = append(details, gin.H{
					"path":  pathPrefix + ".url",
					"issue": "empty",
				})
			} else if !assetURLRegexp.MatchString(trimmed) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".url",
					"issue":    "unsupported_scheme",
					"observed": trimmed,
					"allowed":  []string{"https://", "http://", "velox-asset://", "velox-drive://"},
				})
			}
			if s.Clip.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(s.Clip.SHA256) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".sha256",
					"issue":    "malformed",
					"observed": s.Clip.SHA256,
					"expected": "64 lowercase hex characters ([0-9a-f]{64})",
				})
			}
		}
		if s.Voiceover != nil {
			pathPrefix := fmt.Sprintf("scenes.%d.voiceover", i)
			if trimmed := strings.TrimSpace(s.Voiceover.URL); trimmed == "" {
				details = append(details, gin.H{
					"path":  pathPrefix + ".url",
					"issue": "empty",
				})
			} else if !assetURLRegexp.MatchString(trimmed) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".url",
					"issue":    "unsupported_scheme",
					"observed": trimmed,
					"allowed":  []string{"https://", "http://", "velox-asset://", "velox-drive://"},
				})
			}
			if s.Voiceover.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(s.Voiceover.SHA256) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".sha256",
					"issue":    "malformed",
					"observed": s.Voiceover.SHA256,
					"expected": "64 lowercase hex characters ([0-9a-f]{64})",
				})
			}
		}
		if s.Subtitles != nil {
			pathPrefix := fmt.Sprintf("scenes.%d.subtitles", i)
			if trimmed := strings.TrimSpace(s.Subtitles.URL); trimmed == "" {
				details = append(details, gin.H{
					"path":  pathPrefix + ".url",
					"issue": "empty",
				})
			} else if !assetURLRegexp.MatchString(trimmed) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".url",
					"issue":    "unsupported_scheme",
					"observed": trimmed,
					"allowed":  []string{"https://", "http://", "velox-asset://", "velox-drive://"},
				})
			}
			if s.Subtitles.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(s.Subtitles.SHA256) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".sha256",
					"issue":    "malformed",
					"observed": s.Subtitles.SHA256,
					"expected": "64 lowercase hex characters ([0-9a-f]{64})",
				})
			}
			if s.Subtitles.Format != "" && !containsString([]string{"ass", "srt", "vtt"}, s.Subtitles.Format) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".format",
					"issue":    "unsupported_value",
					"observed": s.Subtitles.Format,
					"allowed":  []string{"ass", "srt", "vtt"},
				})
			}
		}
	}

	return details
}
