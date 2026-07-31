// Package pipeline — asset_projection.go owns typed nested-asset projections.
package pipeline

import "strings"

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
