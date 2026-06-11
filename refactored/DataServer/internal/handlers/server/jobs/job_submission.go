package jobs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"velox-shared/payload"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

type JobSubmissionHandler struct {
	cfg   *config.Config
	fileQ *queue.FileQueue
}

func NewJobSubmissionHandler(cfg *config.Config, fileQ *queue.FileQueue) *JobSubmissionHandler {
	return &JobSubmissionHandler{
		cfg:   cfg,
		fileQ: fileQ,
	}
}

func (h *JobSubmissionHandler) CreateJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var jobData map[string]interface{}
		if err := c.ShouldBindJSON(&jobData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload", "detail": err.Error()})
			return
		}

		if len(jobData) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Empty payload"})
			return
		}

		isValid, validationErrors, validationDetails := validateJobPayload(jobData)
		if !isValid {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "VALIDAZIONE FALLITA",
				"errors":  validationErrors,
				"details": validationDetails,
				"hint":    "Il job richiede almeno 1 clip E almeno 1 voiceover per essere processato",
			})
			return
		}

		splitByVoiceover, _ := jobData["split_by_voiceover"].(bool)

		var jobPayloads []map[string]interface{}
		if splitByVoiceover {
			jobPayloads = splitByVoiceoverJobs(jobData)
		} else {
			jobPayloads = []map[string]interface{}{jobData}
		}

		createdIDs := []string{}
		dedupedIDs := []string{}

		for _, payload := range jobPayloads {
			jobID, normalized, fingerprint := buildSingleJob(payload)

			dupID := h.findDuplicate(fingerprint, normalized)
			if dupID != "" {
				dedupedIDs = append(dedupedIDs, dupID)
				continue
			}

			if err := h.fileQ.SubmitJob(c.Request.Context(), jobID, normalized); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			createdIDs = append(createdIDs, jobID)
		}

		if splitByVoiceover {
			c.JSON(http.StatusOK, gin.H{
				"status":             "PENDING",
				"job_ids":            createdIDs,
				"deduplicated_ids":   dedupedIDs,
				"count_created":      len(createdIDs),
				"count_deduplicated": len(dedupedIDs),
				"split_by_voiceover": true,
			})
			return
		}

		if len(createdIDs) > 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":       "PENDING",
				"job_id":       createdIDs[0],
				"deduplicated": false,
			})
			return
		}

		if len(dedupedIDs) > 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":       "PENDING",
				"job_id":       dedupedIDs[0],
				"deduplicated": true,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":       "PENDING",
			"job_id":       uuid.New().String(),
			"deduplicated": false,
		})
	}
}

func validateJobPayload(payload map[string]interface{}) (bool, []string, map[string]interface{}) {
	errors := []string{}
	details := make(map[string]interface{})

	videoName, _ := payload["video_name"].(string)
	if videoName == "" || strings.TrimSpace(videoName) == "" {
		errors = append(errors, "❌ video_name mancante o vuoto")
	} else {
		if len(videoName) > 50 {
			details["video_name"] = videoName[:50]
		} else {
			details["video_name"] = videoName
		}
	}

	clipFields := []struct {
		listField string
		urlField  string
		display   string
	}{
		{"intro_clips", "intro_clips_urls", "intro_clips"},
		{"intro_clip_paths", "intro_clip_paths", "intro_clip_paths"},
		{"start_clips", "start_clips_urls", "start_clips"},
		{"middle_clips", "middle_clips_urls", "middle_clips"},
		{"end_clips", "end_clips_urls", "end_clips"},
		{"stock_clips", "stock_clips_urls", "stock_clips"},
		{"stock_clip_paths", "stock_clip_paths", "stock_clip_paths"},
	}

	hasAnyClip := false
	clipsDetail := []string{}
	for _, cf := range clipFields {
		if listVal, ok := payload[cf.listField].([]interface{}); ok && len(listVal) > 0 {
			clipsDetail = append(clipsDetail, fmt.Sprintf("✅ %s: %d clip(s)", cf.display, len(listVal)))
			hasAnyClip = true
			details[cf.display] = fmt.Sprintf("%d clips", len(listVal))
		}
		if urlVal, ok := payload[cf.urlField].(string); ok && strings.TrimSpace(urlVal) != "" {
			urlCount := len(strings.Split(strings.TrimSpace(urlVal), "\n"))
			clipsDetail = append(clipsDetail, fmt.Sprintf("✅ %s: %d URL(s)", cf.display, urlCount))
			hasAnyClip = true
			details[cf.display] = fmt.Sprintf("%d URLs", urlCount)
		}
	}

	if !hasAnyClip {
		if segments, ok := payload["clip_segments"].([]interface{}); ok && len(segments) > 0 {
			hasAnyClip = true
			details["clip_segments"] = fmt.Sprintf("%d segments", len(segments))
		}
	}

	if !hasAnyClip {
		errors = append(errors, "❌ NESSUN CLIP PRESENTE - Servono clip in intro/start/middle/end/stock oppure clip_segments")
	} else {
		details["clips"] = clipsDetail
	}

	hasAnyVoiceover := false
	voiceoverDetail := []string{}

	if voList, ok := payload["voiceovers"].([]interface{}); ok && len(voList) > 0 {
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("✅ voiceovers: %d item(s)", len(voList)))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceovers", len(voList))
	} else if voStr, ok := payload["voiceovers"].(string); ok && strings.TrimSpace(voStr) != "" {
		urlCount := len(strings.Split(strings.TrimSpace(voStr), "\n"))
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("✅ voiceovers: %d URL(s)", urlCount))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceover URLs", urlCount)
	}

	if voURLs, ok := payload["voiceovers_urls"].(string); ok && strings.TrimSpace(voURLs) != "" {
		urlCount := len(strings.Split(strings.TrimSpace(voURLs), "\n"))
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("✅ voiceovers_urls: %d URL(s)", urlCount))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceover URLs", urlCount)
	}

	if voItems, ok := payload["voiceover_items"].([]interface{}); ok && len(voItems) > 0 {
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("✅ voiceover_items: %d item(s)", len(voItems)))
		hasAnyVoiceover = true
		details["voiceover_items"] = fmt.Sprintf("%d items", len(voItems))
	}

	if !hasAnyVoiceover {
		errors = append(errors, "❌ NESSUN VOICEOVER PRESENTE - Servono voiceover")
		errors = append(errors, "   ℹ️ Usa: voiceovers (lista), voiceovers_urls (stringa), o voiceover_items (lista di dict)")
	} else {
		details["voiceover_detail"] = voiceoverDetail
	}

	return len(errors) == 0, errors, details
}

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

func (h *JobSubmissionHandler) findDuplicate(fingerprint string, normalized map[string]interface{}) string {
	if h.fileQ == nil {
		return ""
	}

	jobs, err := h.fileQ.GetAllJobs(nil)
	if err != nil {
		return ""
	}

	outputVideoID := getStringOrEmpty(normalized, "output_video_id")
	if outputVideoID == "" {
		if slotData, ok := normalized["slot_data"].(map[string]interface{}); ok {
			outputVideoID = getStringOrEmpty(slotData, "output_video_id")
		}
	}

	for existingID, existing := range jobs {
		if existing.Status != queue.StatusPending && existing.Status != queue.StatusProcessing {
			continue
		}

		if outputVideoID != "" {
			existingOutID := getStringOrEmpty(existing.Payload, "output_video_id")
			if existingOutID == "" {
				if existing.SlotData != nil {
					existingOutID = getStringOrEmpty(existing.SlotData, "output_video_id")
				}
			}
			if existingOutID != "" && existingOutID == outputVideoID {
				return existingID
			}
		}

		if existing.JobFingerprint == fingerprint {
			return existingID
		}
	}

	return ""
}
