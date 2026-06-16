package jobs

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-shared/payload"

	"github.com/google/uuid"
)

func buildSingleJob(data map[string]interface{}) (string, map[string]interface{}, string) {
	normalized := make(map[string]interface{})

	for k, v := range data {
		normalized[k] = v
	}

	jobFingerprint := fingerprintPayload(data)
	normalized["job_fingerprint"] = jobFingerprint

	jobID, _ := data["job_id"].(string)
	if jobID == "" {
		jobID = uuid.New().String()
	}
	normalized["job_id"] = jobID
	jobRunID, _ := data["job_run_id"].(string)
	if jobRunID == "" {
		jobRunID, _ = data["run_id"].(string)
	}
	if jobRunID == "" {
		jobRunID = uuid.New().String()
	}
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID

	projectID := getStringOrEmpty(data, "project_id", "project_name", "youtube_group")
	if projectID != "" {
		normalized["project_id"] = projectID
	}

	now := time.Now().Unix()
	if _, ok := normalized["created_at"]; !ok {
		normalized["created_at"] = now
	}
	normalized["status"] = "PENDING"
	normalized["submitted_via"] = "api_v1_go"

	if _, ok := data["start_clips"]; ok {
		normalized["start_clips_urls"] = payload.NormalizeList(data["start_clips"])
	}
	if _, ok := data["intro_clips"]; ok {
		normalized["intro_clips_urls"] = payload.NormalizeList(data["intro_clips"])
	}
	if _, ok := data["intro_clip_paths"]; ok {
		normalized["intro_clip_paths"] = payload.NormalizeList(data["intro_clip_paths"])
	}
	if _, ok := data["middle_clips"]; ok {
		normalized["middle_clips_urls"] = payload.NormalizeList(data["middle_clips"])
	}
	if _, ok := data["end_clips"]; ok {
		normalized["end_clips_urls"] = payload.NormalizeList(data["end_clips"])
	}
	if _, ok := data["stock_clips"]; ok {
		normalized["stock_clips_urls"] = payload.NormalizeList(data["stock_clips"])
	}
	if _, ok := data["stock_clip_paths"]; ok {
		normalized["stock_clip_paths"] = payload.NormalizeList(data["stock_clip_paths"])
	}
	if _, ok := data["clip_segments"]; ok {
		normalized["clip_segments"] = data["clip_segments"]
	}
	if _, ok := data["voiceovers"]; ok {
		normalized["voiceovers_urls"] = payload.NormalizeList(data["voiceovers"])
	}

	if bg, ok := data["background"]; ok {
		normalized["background_path_url"] = strings.TrimSpace(fmt.Sprintf("%v", bg))
	}
	if bgm, ok := data["background_music"]; ok {
		normalized["background_music_urls"] = strings.TrimSpace(fmt.Sprintf("%v", bgm))
	}

	if ent, ok := data["entities"]; ok {
		switch v := ent.(type) {
		case map[string]interface{}, []interface{}:
			dataBytes, _ := json.Marshal(v)
			normalized["json_entities"] = string(dataBytes)
		case string:
			normalized["json_entities"] = v
		}
	}

	startClips := payload.NormalizeListToArray(data["start_clips"])
	introClips := payload.NormalizeListToArray(data["intro_clips"])
	if len(introClips) == 0 {
		introClips = payload.NormalizeListToArray(data["intro_clip_paths"])
	}
	middleClips := payload.NormalizeListToArray(data["middle_clips"])
	endClips := payload.NormalizeListToArray(data["end_clips"])
	stockClips := payload.NormalizeListToArray(data["stock_clips"])
	voiceovers := payload.NormalizeListToArray(data["voiceovers"])
	clipSegments, _ := data["clip_segments"].([]interface{})

	var stockTS []interface{}
	if ts, ok := data["stock_clips_timestamps"]; ok {
		switch v := ts.(type) {
		case []interface{}:
			stockTS = v
		case map[string]interface{}:
			stockTS = []interface{}{v}
		}
	}

	videoStyle, _ := data["video_style"].(string)
	if videoStyle == "" {
		videoStyle = "Rap"
	}

	outputVideoID, _ := data["output_video_id"].(string)
	outputDir, _ := data["output_directory"].(string)
	youtubeGroup, _ := data["youtube_group"].(string)
	if youtubeGroup == "" && projectID != "" {
		youtubeGroup = projectID
	}

	slotData := map[string]interface{}{
		"video_name":                getStringOrEmpty(normalized, "video_name"),
		"video_style":               videoStyle,
		"start_clips":               startClips,
		"intro_clips":               introClips,
		"middle_clips":              middleClips,
		"stock_clips":               stockClips,
		"end_clips":                 endClips,
		"voiceovers":                voiceovers,
		"clip_segments":             clipSegments,
		"start_clips_urls":          getStringOrEmpty(normalized, "start_clips_urls"),
		"intro_clips_urls":          getStringOrEmpty(normalized, "intro_clips_urls"),
		"intro_clip_paths":          getStringOrEmpty(normalized, "intro_clip_paths"),
		"middle_clips_urls":         getStringOrEmpty(normalized, "middle_clips_urls"),
		"stock_clips_urls":          getStringOrEmpty(normalized, "stock_clips_urls"),
		"stock_clip_paths":          getStringOrEmpty(normalized, "stock_clip_paths"),
		"end_clips_urls":            getStringOrEmpty(normalized, "end_clips_urls"),
		"voiceovers_urls":           getStringOrEmpty(normalized, "voiceovers_urls"),
		"background_path":           getStringOrEmpty(data, "background"),
		"background_path_url":       getStringOrEmpty(normalized, "background_path_url"),
		"background_music_paths":    []string{},
		"background_music_urls":     getStringOrEmpty(normalized, "background_music_urls"),
		"stock_clips_timestamps":    stockTS,
		"youtube_group":             youtubeGroup,
		"voiceover_channel_mapping": data["voiceover_channel_mapping"],
		"output_video_mapping":      data["output_video_mapping"],
		"output_video_id":           outputVideoID,
		"output_directory":          outputDir,
		"json_entities":             getStringOrEmpty(normalized, "json_entities"),
		"pre_associated_entities":   data["pre_associated_entities"],
		"raw_entities":              data["raw_entities"],
		"project_id":                projectID,
		"video_mode":                getStringOrEmpty(data, "video_mode"),
		"drive_output_folder":       getStringOrEmpty(data, "drive_output_folder", "output_directory"),
	}

	if _, ok := slotData["voiceover_channel_mapping"].(map[string]interface{}); !ok {
		slotData["voiceover_channel_mapping"] = map[string]interface{}{}
	}
	if _, ok := slotData["output_video_mapping"].(map[string]interface{}); !ok {
		slotData["output_video_mapping"] = map[string]interface{}{}
	}
	if _, ok := slotData["video_mode"].(string); !ok {
		slotData["video_mode"] = ""
	}

	normalized["slot_data"] = slotData

	return jobID, normalized, jobFingerprint
}
