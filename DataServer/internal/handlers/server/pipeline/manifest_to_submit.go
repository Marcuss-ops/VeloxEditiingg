package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

func renderManifestToSubmitRequest(base SubmitJobRequest, manifest map[string]interface{}) (SubmitJobRequest, []gin.H) {
	var details []gin.H
	requiredTop := []string{"schema_version", "manifest_id", "created_at", "source", "video", "script", "scenes", "delivery_plan", "integrity"}
	allowedOptional := []string{"audio_tracks"}
	for _, key := range requiredTop {
		if _, ok := manifest[key]; !ok {
			details = append(details, gin.H{"path": key, "issue": "missing"})
		}
	}
	for _, key := range sortedMapKeys(manifest) {
		if !containsString(requiredTop, key) && !containsString(allowedOptional, key) {
			details = append(details, gin.H{"path": key, "issue": "unexpected"})
		}
	}
	if sv := stringField(manifest, "schema_version"); sv != "velox.render-manifest.v1" {
		details = append(details, gin.H{
			"path":     "schema_version",
			"issue":    "unsupported_value",
			"observed": sv,
			"allowed":  []string{"velox.render-manifest.v1"},
		})
	}
	if created := stringField(manifest, "created_at"); created != "" {
		if _, err := time.Parse(time.RFC3339, created); err != nil {
			details = append(details, gin.H{"path": "created_at", "issue": "malformed"})
		}
	}

	video := objectField(manifest, "video")
	script := objectField(manifest, "script")
	integrity := objectField(manifest, "integrity")
	if video == nil {
		details = append(details, gin.H{"path": "video", "issue": "type"})
	}
	if script == nil {
		details = append(details, gin.H{"path": "script", "issue": "type"})
	}
	if integrity == nil {
		details = append(details, gin.H{"path": "integrity", "issue": "type"})
	}

	if integrity != nil {
		if alg := stringField(integrity, "algorithm"); alg != "sha256" {
			details = append(details, gin.H{"path": "integrity.algorithm", "issue": "unsupported_value", "observed": alg})
		}
		if got := stringField(integrity, "manifest_sha256"); !manifestRefSHA256Regexp.MatchString(got) {
			details = append(details, gin.H{"path": "integrity.manifest_sha256", "issue": "malformed"})
		} else if expected, err := canonicalManifestIntegritySHA256(manifest); err != nil {
			details = append(details, gin.H{"path": "integrity.manifest_sha256", "issue": "hash_failed"})
		} else if got != expected {
			details = append(details, gin.H{
				"path":     "integrity.manifest_sha256",
				"issue":    "mismatch",
				"observed": got,
				"expected": expected,
			})
		}
	}

	scenes, sceneDetails := manifestScenesToSubmitScenes(manifest["scenes"])
	details = append(details, sceneDetails...)
	plan, planDetails := manifestDeliveryPlanToSubmit(manifest["delivery_plan"])
	details = append(details, planDetails...)
	audioTracks, audioDetails := manifestAudioTracksToSubmit(manifest["audio_tracks"])
	details = append(details, audioDetails...)

	if integrity != nil {
		if got := int64Field(integrity, "scene_count"); got != int64(len(scenes)) {
			details = append(details, gin.H{
				"path":     "integrity.scene_count",
				"issue":    "mismatch",
				"observed": got,
				"expected": len(scenes),
			})
		}
		if got := int64Field(integrity, "audio_track_count"); got != int64(len(audioTracks)) {
			details = append(details, gin.H{
				"path":     "integrity.audio_track_count",
				"issue":    "mismatch",
				"observed": got,
				"expected": len(audioTracks),
			})
		}
		var total int64
		for _, s := range scenes {
			total += int64(s.DurationSeconds * 1000)
		}
		if got := int64Field(integrity, "total_duration_ms"); got != total {
			details = append(details, gin.H{
				"path":     "integrity.total_duration_ms",
				"issue":    "mismatch",
				"observed": got,
				"expected": total,
			})
		}
	}

	base.VideoName = stringField(video, "name")
	base.ScriptText = stringField(script, "text")
	base.Scenes = scenes
	base.DeliveryPlan = plan
	base.AudioTracks = audioTracks
	base.VoiceoverPaths = nil
	base.SubtitleTracks = nil
	base.Layers = nil
	return base, details
}

func manifestScenesToSubmitScenes(raw interface{}) ([]SubmitScene, []gin.H) {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, []gin.H{{"path": "scenes", "issue": "empty"}}
	}
	out := make([]SubmitScene, 0, len(arr))
	var details []gin.H
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d", i), "issue": "type"})
			continue
		}
		durationMS := int64Field(m, "duration_ms")
		scene := SubmitScene{
			SceneID:         stringField(m, "scene_id"),
			Index:           int64Field(m, "index"),
			Kind:            stringField(m, "kind"),
			Text:            stringField(m, "text"),
			DurationSeconds: float64(durationMS) / 1000.0,
		}
		if scene.SceneID == "" {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d.scene_id", i), "issue": "empty"})
		}
		if scene.Kind == "" {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d.kind", i), "issue": "empty"})
		}
		if durationMS < 0 {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d.duration_ms", i), "issue": "out_of_range"})
		}
		if clip := objectField(m, "clip"); clip != nil {
			scene.Clip = &SubmitClip{
				AssetID:     stringField(clip, "asset_id"),
				DriveFileID: stringField(clip, "drive_file_id"),
				URL:         stringField(clip, "url"),
				SHA256:      stringField(clip, "sha256"),
				StartMS:     int64Field(clip, "start_ms"),
				EndMS:       int64Field(clip, "end_ms"),
				DurationMS:  int64Field(clip, "duration_ms"),
			}
		}
		if vo := objectField(m, "voiceover"); vo != nil {
			scene.Voiceover = &SubmitVoiceover{
				AssetID:     stringField(vo, "asset_id"),
				DriveFileID: stringField(vo, "drive_file_id"),
				URL:         stringField(vo, "url"),
				SHA256:      stringField(vo, "sha256"),
				DurationMS:  int64Field(vo, "duration_ms"),
				Language:    stringField(vo, "language"),
			}
		}
		if sub := objectField(m, "subtitles"); sub != nil {
			scene.Subtitles = &SubmitSubtitles{
				AssetID:  stringField(sub, "asset_id"),
				Format:   stringField(sub, "format"),
				URL:      stringField(sub, "url"),
				SHA256:   stringField(sub, "sha256"),
				Language: stringField(sub, "language"),
			}
		}
		out = append(out, scene)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, details
}

func manifestDeliveryPlanToSubmit(raw interface{}) ([]SubmitDeliveryPlanEntry, []gin.H) {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, []gin.H{{"path": "delivery_plan", "issue": "empty"}}
	}
	out := make([]SubmitDeliveryPlanEntry, 0, len(arr))
	var details []gin.H
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			details = append(details, gin.H{"path": fmt.Sprintf("delivery_plan.%d", i), "issue": "type"})
			continue
		}
		entry := SubmitDeliveryPlanEntry{
			DestinationID: stringField(m, "destination_id"),
			Priority:      int(int64Field(m, "priority")),
			Metadata:      objectField(m, "metadata"),
		}
		if _, ok := m["retry_budget"]; ok {
			rb := int(int64Field(m, "retry_budget"))
			entry.RetryBudget = &rb
		}
		out = append(out, entry)
	}
	return out, details
}

// manifestAudioTracksToSubmit parses the optional top-level audio_tracks array
// from a velox.render-manifest.v1 document into []SubmitAudioTrack. An absent
// or null audio_tracks key returns an empty slice with no validation errors.
func manifestAudioTracksToSubmit(raw interface{}) ([]SubmitAudioTrack, []gin.H) {
	if raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, []gin.H{{"path": "audio_tracks", "issue": "type"}}
	}
	if len(arr) == 0 {
		return nil, nil
	}
	out := make([]SubmitAudioTrack, 0, len(arr))
	var details []gin.H
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			details = append(details, gin.H{"path": fmt.Sprintf("audio_tracks.%d", i), "issue": "type"})
			continue
		}
		track := SubmitAudioTrack{
			AssetID:         stringField(m, "asset_id"),
			SourceURL:       stringField(m, "source_url"),
			Role:            stringField(m, "role"),
			Volume:          float64Field(m, "volume"),
			StartTimeOffset: float64Field(m, "start_time_offset"),
			DurationSeconds: float64Field(m, "duration_seconds"),
		}
		// At least one of asset_id or source_url must be present.
		if track.AssetID == "" && track.SourceURL == "" {
			details = append(details, gin.H{
				"path":  fmt.Sprintf("audio_tracks.%d", i),
				"issue": "missing_source",
			})
		}
		out = append(out, track)
	}
	return out, details
}

func canonicalManifestIntegritySHA256(manifest map[string]interface{}) (string, error) {
	body := cloneJSONMap(manifest)
	delete(body, "integrity")
	canonical, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func manifestValidationError(details ...gin.H) *SubmitJobValidationError {
	return &SubmitJobValidationError{
		Code:    "invalid_payload",
		Reason:  "manifest_ref_invalid",
		Message: fmt.Sprintf("manifest_ref has %d validation failure(s) (see details)", len(details)),
		Details: details,
	}
}
