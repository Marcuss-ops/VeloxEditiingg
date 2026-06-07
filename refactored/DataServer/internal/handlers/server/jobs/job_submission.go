package jobs

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

// JobSubmissionHandler handles job creation with validation and normalization
type JobSubmissionHandler struct {
	cfg   *config.Config
	fileQ *queue.FileQueue
}

// NewJobSubmissionHandler creates a new job submission handler
func NewJobSubmissionHandler(cfg *config.Config, fileQ *queue.FileQueue) *JobSubmissionHandler {
	return &JobSubmissionHandler{
		cfg:   cfg,
		fileQ: fileQ,
	}
}

// CreateJobHandler handles POST /jobs (submit new job)
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

		// Validate payload
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

		// Check for split by voiceover
		splitByVoiceover, _ := jobData["split_by_voiceover"].(bool)

		var jobPayloads []map[string]interface{}
		if splitByVoiceover {
			jobPayloads = splitByVoiceoverJobs(jobData)
		} else {
			jobPayloads = []map[string]interface{}{jobData}
		}

		// Submit jobs
		createdIDs := []string{}
		dedupedIDs := []string{}

		for _, payload := range jobPayloads {
			jobID, normalized, fingerprint := buildSingleJob(payload)

			// Check for duplicates
			dupID := h.findDuplicate(fingerprint, normalized)
			if dupID != "" {
				dedupedIDs = append(dedupedIDs, dupID)
				continue
			}

			// Submit to queue
			if err := h.fileQ.SubmitJob(c.Request.Context(), jobID, normalized); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			createdIDs = append(createdIDs, jobID)
		}

		// Response
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

// validateJobPayload validates the job payload before queuing
func validateJobPayload(payload map[string]interface{}) (bool, []string, map[string]interface{}) {
	errors := []string{}
	details := make(map[string]interface{})

	// Check video_name
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

	// Check clips
	clipFields := []struct {
		listField string
		urlField  string
		display   string
	}{
		{"start_clips", "start_clips_urls", "start_clips"},
		{"middle_clips", "middle_clips_urls", "middle_clips"},
		{"end_clips", "end_clips_urls", "end_clips"},
		{"stock_clips", "stock_clips_urls", "stock_clips"},
	}

	hasAnyClip := false
	clipsDetail := []string{}
	for _, cf := range clipFields {
		// Check list
		if listVal, ok := payload[cf.listField].([]interface{}); ok && len(listVal) > 0 {
			clipsDetail = append(clipsDetail, fmt.Sprintf("✅ %s: %d clip(s)", cf.display, len(listVal)))
			hasAnyClip = true
			details[cf.display] = fmt.Sprintf("%d clips", len(listVal))
		}
		// Check URL string
		if urlVal, ok := payload[cf.urlField].(string); ok && strings.TrimSpace(urlVal) != "" {
			urlCount := len(strings.Split(strings.TrimSpace(urlVal), "\n"))
			clipsDetail = append(clipsDetail, fmt.Sprintf("✅ %s: %d URL(s)", cf.display, urlCount))
			hasAnyClip = true
			details[cf.display] = fmt.Sprintf("%d URLs", urlCount)
		}
	}

	if !hasAnyClip {
		errors = append(errors, "❌ NESSUN CLIP PRESENTE - Servono clip in start/middle/end/stock")
	} else {
		details["clips"] = clipsDetail
	}

	// Check voiceovers
	hasAnyVoiceover := false
	voiceoverDetail := []string{}

	// Check voiceovers (list or string)
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

	// Check voiceovers_urls
	if voURLs, ok := payload["voiceovers_urls"].(string); ok && strings.TrimSpace(voURLs) != "" {
		urlCount := len(strings.Split(strings.TrimSpace(voURLs), "\n"))
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("✅ voiceovers_urls: %d URL(s)", urlCount))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceover URLs", urlCount)
	}

	// Check voiceover_items
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

// fingerprintPayload generates a unique fingerprint for deduplication
func fingerprintPayload(data map[string]interface{}) string {
	payload := map[string]string{
		"video_name":       getStringOrEmpty(data, "video_name"),
		"voiceovers":       getStringOrEmpty(data, "voiceovers_urls", "voiceovers"),
		"start_clips":      getStringOrEmpty(data, "start_clips_urls", "start_clips"),
		"middle_clips":     getStringOrEmpty(data, "middle_clips_urls", "middle_clips"),
		"end_clips":        getStringOrEmpty(data, "end_clips_urls", "end_clips"),
		"stock_clips":      getStringOrEmpty(data, "stock_clips_urls", "stock_clips"),
		"background":       getStringOrEmpty(data, "background_path_url", "background"),
		"background_music": getStringOrEmpty(data, "background_music_urls", "background_music"),
		"entities":         getStringOrEmpty(data, "json_entities", "entities"),
		"output_video_id":  getStringOrEmpty(data, "output_video_id"),
	}

	// Sort keys and serialize
	dataBytes, _ := json.Marshal(payload)
	hash := sha256.Sum256(dataBytes)
	return fmt.Sprintf("%x", hash)
}

// getStringOrEmpty gets a string value from multiple possible keys
func getStringOrEmpty(data map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := data[key]; ok {
			switch v := val.(type) {
			case string:
				return strings.TrimSpace(v)
			case []interface{}:
				// Convert list to newline-separated string
				var parts []string
				for _, item := range v {
					if s, ok := item.(string); ok {
						parts = append(parts, strings.TrimSpace(s))
					}
				}
				return strings.Join(parts, "\n")
			}
		}
	}
	return ""
}

// normalizeList converts a list to a newline-separated string
func normalizeList(val interface{}) string {
	switch v := val.(type) {
	case []interface{}:
		var parts []string
		for _, item := range v {
			if s, ok := item.(string); ok {
				parts = append(parts, strings.TrimSpace(s))
			}
		}
		return strings.Join(parts, "\n")
	case string:
		return strings.TrimSpace(v)
	}
	return ""
}

// normalizeListToArray converts various formats to a string array
func normalizeListToArray(val interface{}) []string {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []interface{}:
		var result []string
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				result = append(result, strings.TrimSpace(s))
			}
		}
		return result
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil
		}
		if strings.Contains(s, "\n") {
			var result []string
			for _, line := range strings.Split(s, "\n") {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					result = append(result, trimmed)
				}
			}
			return result
		}
		return []string{s}
	}
	return nil
}

// extractDriveID extracts Google Drive ID from URL
func extractDriveID(url string) string {
	s := strings.TrimSpace(url)
	if s == "" {
		return ""
	}

	patterns := []string{
		`/d/([A-Za-z0-9_-]{10,})`,
		`id=([A-Za-z0-9_-]{10,})`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		if m := re.FindStringSubmatch(s); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

// buildSingleJob normalizes and builds a single job payload
func buildSingleJob(data map[string]interface{}) (string, map[string]interface{}, string) {
	normalized := make(map[string]interface{})

	// Copy all fields
	for k, v := range data {
		normalized[k] = v
	}

	// Generate fingerprint
	jobFingerprint := fingerprintPayload(data)
	normalized["job_fingerprint"] = jobFingerprint

	// Generate or use existing job_id
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

	// Normalize project_id
	projectID := getStringOrEmpty(data, "project_id", "project_name", "youtube_group")
	if projectID != "" {
		normalized["project_id"] = projectID
	}

	// Set defaults
	now := time.Now().Unix()
	if _, ok := normalized["created_at"]; !ok {
		normalized["created_at"] = now
	}
	normalized["status"] = "PENDING"
	normalized["submitted_via"] = "api_v1_go"

	// Normalize clip URLs
	if _, ok := data["start_clips"]; ok {
		normalized["start_clips_urls"] = normalizeList(data["start_clips"])
	}
	if _, ok := data["middle_clips"]; ok {
		normalized["middle_clips_urls"] = normalizeList(data["middle_clips"])
	}
	if _, ok := data["end_clips"]; ok {
		normalized["end_clips_urls"] = normalizeList(data["end_clips"])
	}
	if _, ok := data["stock_clips"]; ok {
		normalized["stock_clips_urls"] = normalizeList(data["stock_clips"])
	}
	if _, ok := data["voiceovers"]; ok {
		normalized["voiceovers_urls"] = normalizeList(data["voiceovers"])
	}

	// Normalize background
	if bg, ok := data["background"]; ok {
		normalized["background_path_url"] = strings.TrimSpace(fmt.Sprintf("%v", bg))
	}
	if bgm, ok := data["background_music"]; ok {
		normalized["background_music_urls"] = strings.TrimSpace(fmt.Sprintf("%v", bgm))
	}

	// Normalize entities
	if ent, ok := data["entities"]; ok {
		switch v := ent.(type) {
		case map[string]interface{}, []interface{}:
			dataBytes, _ := json.Marshal(v)
			normalized["json_entities"] = string(dataBytes)
		case string:
			normalized["json_entities"] = v
		}
	}

	// Build slot_data
	startClips := normalizeListToArray(data["start_clips"])
	middleClips := normalizeListToArray(data["middle_clips"])
	endClips := normalizeListToArray(data["end_clips"])
	stockClips := normalizeListToArray(data["stock_clips"])
	voiceovers := normalizeListToArray(data["voiceovers"])

	// Process stock_clips_timestamps
	var stockTS []interface{}
	if ts, ok := data["stock_clips_timestamps"]; ok {
		switch v := ts.(type) {
		case []interface{}:
			stockTS = v
		case map[string]interface{}:
			stockTS = []interface{}{v}
		}
	}

	// Get video_style
	videoStyle, _ := data["video_style"].(string)
	if videoStyle == "" {
		videoStyle = "Rap"
	}

	// Get output_video_id
	outputVideoID, _ := data["output_video_id"].(string)

	// Get output_directory
	outputDir, _ := data["output_directory"].(string)

	// Get youtube_group
	youtubeGroup, _ := data["youtube_group"].(string)
	if youtubeGroup == "" && projectID != "" {
		youtubeGroup = projectID
	}

	// Build slot_data
	slotData := map[string]interface{}{
		"video_name":                getStringOrEmpty(normalized, "video_name"),
		"video_style":               videoStyle,
		"start_clips":               startClips,
		"middle_clips":              middleClips,
		"stock_clips":               stockClips,
		"end_clips":                 endClips,
		"voiceovers":                voiceovers,
		"start_clips_urls":          getStringOrEmpty(normalized, "start_clips_urls"),
		"middle_clips_urls":         getStringOrEmpty(normalized, "middle_clips_urls"),
		"stock_clips_urls":          getStringOrEmpty(normalized, "stock_clips_urls"),
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
	}

	// Ensure voiceover_channel_mapping and output_video_mapping are maps
	if _, ok := slotData["voiceover_channel_mapping"].(map[string]interface{}); !ok {
		slotData["voiceover_channel_mapping"] = map[string]interface{}{}
	}
	if _, ok := slotData["output_video_mapping"].(map[string]interface{}); !ok {
		slotData["output_video_mapping"] = map[string]interface{}{}
	}

	normalized["slot_data"] = slotData

	return jobID, normalized, jobFingerprint
}

// splitByVoiceoverJobs splits a job into multiple jobs by voiceover
func splitByVoiceoverJobs(data map[string]interface{}) []map[string]interface{} {
	voiceoverList := normalizeListToArray(data["voiceovers"])
	if len(voiceoverList) == 0 {
		voiceoverList = normalizeListToArray(data["voiceovers_urls"])
	}

	if len(voiceoverList) == 0 {
		return []map[string]interface{}{data}
	}

	mapping, _ := data["output_video_mapping"].(map[string]interface{})

	result := make([]map[string]interface{}, 0)
	for _, vo := range voiceoverList {
		payload := deepCopyMap(data)
		payload["voiceovers"] = []string{vo}
		payload["voiceovers_urls"] = vo

		// Pick mapping for this voiceover
		if len(mapping) > 0 {
			outID, info := pickMappingForVoiceover(mapping, vo)
			if outID != "" {
				payload["output_video_id"] = outID
				payload["output_video_mapping"] = map[string]interface{}{outID: info}
			}
		}

		// Reduce voiceover_channel_mapping
		if vmap, ok := payload["voiceover_channel_mapping"].(map[string]interface{}); ok && len(vmap) > 0 {
			voBase := vo
			if idx := strings.LastIndex(vo, "/"); idx >= 0 {
				voBase = vo[idx+1:]
			}
			if idx := strings.Index(voBase, "?"); idx >= 0 {
				voBase = voBase[:idx]
			}
			if mappedVal, exists := vmap[voBase]; exists {
				payload["voiceover_channel_mapping"] = map[string]interface{}{voBase: mappedVal}
			}
		}

		result = append(result, payload)
	}

	return result
}

// deepCopyMap creates a deep copy of a map
func deepCopyMap(m map[string]interface{}) map[string]interface{} {
	dataBytes, _ := json.Marshal(m)
	var result map[string]interface{}
	json.Unmarshal(dataBytes, &result)
	return result
}

// pickMappingForVoiceover finds the output video mapping for a voiceover
func pickMappingForVoiceover(mapping map[string]interface{}, voiceoverURL string) (string, map[string]interface{}) {
	voID := extractDriveID(voiceoverURL)
	voBase := voiceoverURL
	if idx := strings.LastIndex(voiceoverURL, "/"); idx >= 0 {
		voBase = voiceoverURL[idx+1:]
	}
	if idx := strings.Index(voBase, "?"); idx >= 0 {
		voBase = voBase[:idx]
	}

	for outID, info := range mapping {
		infoMap, ok := info.(map[string]interface{})
		if !ok {
			continue
		}

		voName, _ := infoMap["voiceover_name"].(string)
		voPath, _ := infoMap["voiceover_path"].(string)

		if voiceoverURL != "" && voiceoverURL == voPath {
			return outID, infoMap
		}
		if voID != "" && voID == extractDriveID(voPath) {
			return outID, infoMap
		}
		if voBase != "" && voBase == voName {
			return outID, infoMap
		}
	}

	return "", nil
}

// findDuplicate checks for existing duplicate jobs
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

		// Check by output_video_id
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

		// Check by fingerprint
		if existing.JobFingerprint == fingerprint {
			return existingID
		}
	}

	return ""
}

// BulkDeleteJobsHandler handles POST /jobs/bulk_delete
func (h *JobSubmissionHandler) BulkDeleteJobsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobIDs []string `json:"job_ids"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if len(body.JobIDs) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "job_ids must be a non-empty list"})
			return
		}

		removed := []string{}
		for _, jobID := range body.JobIDs {
			if err := h.fileQ.DeleteJob(c.Request.Context(), jobID); err == nil {
				removed = append(removed, jobID)
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"status":        "ok",
			"removed_count": len(removed),
			"removed_ids":   removed,
		})
	}
}

// RetryJobHandler handles POST /jobs/:id/retry
func (h *JobSubmissionHandler) RetryJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")

		job, err := h.fileQ.GetJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
			return
		}

		// Only retry error jobs
		if job.Status != queue.StatusError && job.Status != queue.StatusFailed {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Can only retry ERROR or FAILED jobs"})
			return
		}

		// Reset job to pending
		jobs, _ := h.fileQ.GetAllJobs(c.Request.Context())
		if j, exists := jobs[jobID]; exists {
			j.Status = queue.StatusPending
			j.LastError = ""
			j.ErrorMessage = ""
			j.AssignedTo = ""
			j.ClaimedBy = ""
			now := time.Now().Unix()
			j.UpdatedAt = now
			j.History = append(j.History, queue.JobHistoryEntry{
				Status:    "PENDING",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Message:   "Job manually retried",
			})
			jobs[jobID] = j
		}

		c.JSON(http.StatusOK, gin.H{
			"status":  "PENDING",
			"job_id":  jobID,
			"message": "Job queued for retry",
		})
	}
}

// GetJobsDashboardHandler handles GET /jobs/dashboard - optimized for dashboard
func (h *JobSubmissionHandler) GetJobsDashboardHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 50
		if l := c.Query("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n >= 1 && n <= 200 {
				limit = n
			}
		}

		stats, err := h.fileQ.Stats(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		jobs, _ := h.fileQ.GetAllJobs(c.Request.Context())

		// Build recent jobs per status
		recent := map[string][]map[string]interface{}{
			"PENDING":    {},
			"PROCESSING": {},
			"COMPLETED":  {},
			"ERROR":      {},
		}

		keepFields := []string{"job_id", "video_name", "status", "created_at", "updated_at",
			"completed_at", "started_at", "processing_at", "assigned_at",
			"assigned_to", "assigned_worker_ip", "error", "error_message",
			"last_error", "drive_url", "remote_status", "last_drive_upload_result"}

		for _, job := range jobs {
			status := string(job.Status)
			if status == "FAILED" {
				status = "ERROR"
			}

			if arr, ok := recent[status]; ok && len(arr) < limit {
				trimmed := map[string]interface{}{
					"job_id":     job.JobID,
					"video_name": job.VideoName,
					"status":     status,
					"created_at": job.CreatedAt,
					"updated_at": job.UpdatedAt,
				}
				for _, f := range keepFields {
					switch f {
					case "assigned_to":
						trimmed[f] = job.AssignedTo
					case "last_error":
						trimmed[f] = job.LastError
					case "drive_url":
						trimmed[f] = job.DriveURL
					}
				}
				// Add video_name from slot_data if not present
				if trimmed["video_name"] == "" && job.SlotData != nil {
					if vn, ok := job.SlotData["video_name"].(string); ok {
						trimmed["video_name"] = vn
					}
				}
				recent[status] = append(arr, trimmed)
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"counts":    stats,
			"recent":    recent,
			"timestamp": time.Now().Unix(),
		})
	}
}
