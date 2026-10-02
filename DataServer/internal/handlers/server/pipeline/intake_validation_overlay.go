package pipeline

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-shared/assetref"
)

func validateSubmitOverlays(overlays []SubmitOverlay) []gin.H {
	if len(overlays) == 0 {
		return nil
	}
	details := make([]gin.H, 0)
	seen := make(map[string]struct{}, len(overlays))
	replacements := make([]SubmitOverlay, 0)
	for i, overlay := range overlays {
		path := fmt.Sprintf("overlays.%d", i)
		id := strings.TrimSpace(overlay.ID)
		if id == "" {
			details = append(details, gin.H{"path": path + ".id", "issue": "empty"})
		}
		if _, exists := seen[id]; exists && id != "" {
			details = append(details, gin.H{"path": path + ".id", "issue": "duplicate"})
		}
		seen[id] = struct{}{}
		assetIDPresent := strings.TrimSpace(overlay.AssetID) != "" || strings.TrimSpace(overlay.DriveFileID) != ""
		if !assetIDPresent {
			link := strings.TrimSpace(overlay.DriveLink)
			if link == "" {
				link = strings.TrimSpace(overlay.URL)
			}
			_, err := assetref.ParseDriveFileID(link)
			assetIDPresent = err == nil
		}
		if !assetIDPresent {
			details = append(details, gin.H{"path": path + ".asset_id", "issue": "asset_id_or_drive_file_id_required"})
		}
		if overlay.StartFrame < 0 {
			details = append(details, gin.H{"path": path + ".start_frame", "issue": "out_of_range"})
		}
		if err := normalizeSubmitOverlayWindow(&overlay); err != nil {
			details = append(details, gin.H{"path": path + ".end_frame", "issue": "invalid_window", "message": err.Error()})
		}
		if overlay.Mode == "composite" {
			details = append(details, gin.H{"path": path + ".mode", "issue": "requires_prepared_video_replacement", "message": "render the composite into a finished video asset and submit it during FINALIZE as visual_replacements"})
		} else if overlay.Mode != "replace" {
			details = append(details, gin.H{"path": path + ".mode", "issue": "unsupported_value", "allowed": []string{"replace"}})
		}
		if overlay.AudioMode != "" && overlay.AudioMode != "preserve_final_audio" {
			details = append(details, gin.H{"path": path + ".audio_mode", "issue": "unsupported_value", "allowed": []string{"preserve_final_audio"}})
		}
		assetURL := strings.TrimSpace(overlay.URL)
		if assetURL == "" {
			assetURL = strings.TrimSpace(overlay.DriveLink)
		}
		if assetURL != "" && !isAcceptedAssetURL(assetURL) {
			details = append(details, gin.H{"path": path + ".url", "issue": "unsupported_scheme"})
		}
		if overlay.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(strings.TrimSpace(overlay.SHA256)) {
			details = append(details, gin.H{"path": path + ".sha256", "issue": "malformed"})
		}
		if overlay.Mode == "replace" {
			replacements = append(replacements, overlay)
		}
	}
	sort.SliceStable(replacements, func(i, j int) bool {
		if replacements[i].StartFrame != replacements[j].StartFrame {
			return replacements[i].StartFrame < replacements[j].StartFrame
		}
		return replacements[i].ID < replacements[j].ID
	})
	for i := 1; i < len(replacements); i++ {
		if replacements[i].StartFrame < replacements[i-1].StartFrame+replacements[i-1].FrameCount {
			details = append(details, gin.H{"path": fmt.Sprintf("overlays.%s", replacements[i].ID), "issue": "replace_overlap", "overlaps": replacements[i-1].ID})
		}
	}
	return details
}

// validateSubmitOverlayClipCollisions rejects replace overlays that would
// hide a scene clip. The worker timeline inserts clip scenes before applying
// overlays, so stale absolute overlay windows can otherwise silently cover
// the clip after a producer inserts scenes.
func validateSubmitOverlayClipCollisions(req SubmitJobRequest) []gin.H {
	if len(req.Scenes) == 0 || len(req.Overlays) == 0 {
		return nil
	}
	const fps = 24.0
	type clipWindow struct {
		id         string
		startFrame int64
		endFrame   int64
	}
	windows := make([]clipWindow, 0)
	cursor := 0.0
	for index, scene := range req.Scenes {
		duration := scene.DurationSeconds
		if scene.Voiceover != nil && scene.Voiceover.DurationMS > 0 {
			duration = float64(scene.Voiceover.DurationMS) / 1000
		}
		if duration < 0 || math.IsNaN(duration) || math.IsInf(duration, 0) {
			duration = 0
		}
		if scene.Clip != nil {
			clipDuration := duration
			if scene.Clip.DurationMS > 0 {
				clipDuration = float64(scene.Clip.DurationMS) / 1000
			}
			clipStart := cursor
			if scene.Stock != nil || len(scene.StockAssets) > 0 {
				clipStart += duration
			}
			clipEnd := clipStart + clipDuration
			startFrame := int64(math.Round(clipStart * fps))
			endFrame := int64(math.Round(clipEnd * fps))
			if endFrame > startFrame {
				id := strings.TrimSpace(scene.SceneID)
				if id == "" {
					id = fmt.Sprintf("scenes.%d", index)
				}
				windows = append(windows, clipWindow{id: id, startFrame: startFrame, endFrame: endFrame})
			}
			if scene.Stock != nil || len(scene.StockAssets) > 0 {
				cursor = clipEnd
			} else {
				cursor = clipStart + math.Max(clipDuration, duration)
			}
			continue
		}
		cursor += duration
	}
	if len(windows) == 0 {
		return nil
	}
	var details []gin.H
	for _, overlay := range req.Overlays {
		if overlay.Mode != "replace" {
			continue
		}
		endFrame := overlay.EndFrame
		if endFrame == 0 && overlay.FrameCount > 0 {
			endFrame = overlay.StartFrame + overlay.FrameCount
		}
		for _, clip := range windows {
			if overlay.StartFrame < clip.endFrame && endFrame > clip.startFrame {
				details = append(details, gin.H{
					"path":           "overlays." + strings.TrimSpace(overlay.ID),
					"issue":          "overlaps_clip_scene",
					"clip_scene":     clip.id,
					"overlay_frames": []int64{overlay.StartFrame, endFrame},
					"clip_frames":    []int64{clip.startFrame, clip.endFrame},
				})
			}
		}
	}
	return details
}

// validateWorkerPayloadOverlayClipCollisions applies the same fail-closed
// visibility rule to Creator Push, whose request enters as an opaque payload
// map rather than SubmitJobRequest. The check runs on the normalized worker
// projection so it sees the exact scenes and replacement windows the worker
// will compile.
func validateWorkerPayloadOverlayClipCollisions(workerPayload map[string]interface{}) []gin.H {
	if workerPayload == nil {
		return nil
	}
	req := SubmitJobRequest{}
	var rawScenes []map[string]interface{}
	if encoded, ok := workerPayload["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		if err := json.Unmarshal([]byte(encoded), &rawScenes); err != nil {
			return []gin.H{{"path": "scenes_json", "issue": "invalid"}}
		}
	} else if raw, ok := workerPayload["scenes"]; ok {
		encoded, err := json.Marshal(raw)
		if err != nil || json.Unmarshal(encoded, &rawScenes) != nil {
			return []gin.H{{"path": "scenes", "issue": "invalid"}}
		}
	}
	if len(rawScenes) > 0 {
		req.Scenes = make([]SubmitScene, 0, len(rawScenes))
		for _, rawScene := range rawScenes {
			if rawScene == nil {
				continue
			}
			scene := SubmitScene{
				SceneID:         strings.TrimSpace(stringField(rawScene, "scene_id")),
				DurationSeconds: dryRunNumber(rawScene["duration_seconds"]),
			}
			if rawClip, ok := rawScene["clip"]; ok {
				encoded, err := json.Marshal(rawClip)
				if err != nil || json.Unmarshal(encoded, &scene.Clip) != nil {
					return []gin.H{{"path": "scenes.clip", "issue": "invalid"}}
				}
			}
			if dryRunHasAsset(rawScene["stock"]) {
				scene.StockAssets = []SubmitClip{{}}
			}
			if rawVoiceover, ok := rawScene["voiceover"]; ok {
				encoded, err := json.Marshal(rawVoiceover)
				if err != nil || json.Unmarshal(encoded, &scene.Voiceover) != nil {
					return []gin.H{{"path": "scenes.voiceover", "issue": "invalid"}}
				}
			}
			req.Scenes = append(req.Scenes, scene)
		}
	}
	if rawOverlays, ok := workerPayload["overlays"]; ok {
		encoded, err := json.Marshal(rawOverlays)
		if err != nil || json.Unmarshal(encoded, &req.Overlays) != nil {
			return []gin.H{{"path": "overlays", "issue": "invalid"}}
		}
	}
	return validateSubmitOverlayClipCollisions(req)
}

func normalizeSubmitOverlayWindow(overlay *SubmitOverlay) error {
	if overlay == nil {
		return fmt.Errorf("overlay is nil")
	}
	if overlay.StartFrame < 0 {
		return fmt.Errorf("start_frame must be >= 0")
	}
	if overlay.EndFrame > 0 {
		if overlay.EndFrame <= overlay.StartFrame {
			return fmt.Errorf("end_frame must be greater than start_frame")
		}
		if overlay.FrameCount > 0 && overlay.StartFrame+overlay.FrameCount != overlay.EndFrame {
			return fmt.Errorf("end_frame must equal start_frame + frame_count")
		}
		overlay.FrameCount = overlay.EndFrame - overlay.StartFrame
		return nil
	}
	if overlay.FrameCount <= 0 {
		return fmt.Errorf("frame_count or end_frame must be positive")
	}
	overlay.EndFrame = overlay.StartFrame + overlay.FrameCount
	if overlay.EndFrame <= overlay.StartFrame {
		return fmt.Errorf("end_frame overflows the frame window")
	}
	return nil
}
