package script

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"velox-server/internal/config"
	scenevideo "velox-server/internal/handlers/server/video"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

const scriptSceneMode = "scene_image"

// ScriptHandlers exposes the script-with-images workflow.
type ScriptHandlers struct {
	queue    *queue.FileQueue
	sqliteDB *store.SQLiteStore
	dataDir  string
}

func NewScriptHandlers(cfg *config.Config, q *queue.FileQueue, sqliteDB *store.SQLiteStore) *ScriptHandlers {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.DataDir)
	}
	return &ScriptHandlers{
		queue:    q,
		sqliteDB: sqliteDB,
		dataDir:  dataDir,
	}
}

// RegisterRoutes wires the public script routes on the given group.
func RegisterRoutes(group gin.IRoutes, cfg *config.Config, q *queue.FileQueue, sqliteDB *store.SQLiteStore) *ScriptHandlers {
	handlers := NewScriptHandlers(cfg, q, sqliteDB)
	group.POST("/generate-with-images", handlers.GenerateWithImagesHandler(cfg))
	group.GET("/jobs/:job_id", handlers.ScriptJobHandler(false))
	group.GET("/jobs/:job_id/full", handlers.ScriptJobHandler(true))
	group.GET("/:script_id", handlers.ScriptByIDHandler())
	return handlers
}

// GenerateWithImagesHandler accepts a job payload built from scenes or images,
// then enqueues a process_video job for the remote worker.
func (h *ScriptHandlers) GenerateWithImagesHandler(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.queue == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "queue unavailable"})
			return
		}

		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON body"})
			return
		}
		if payload == nil {
			payload = map[string]interface{}{}
		}

		normalized, err := h.buildSceneImagePayload(cfg, payload)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		response, err := scenevideo.EnqueueSceneVideoJob(c.Request.Context(), h.queue, normalized)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(strings.ToLower(err.Error()), "queue unavailable") {
				status = http.StatusServiceUnavailable
			}
			c.JSON(status, gin.H{"ok": false, "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":                  true,
			"job_id":              response["job_id"],
			"job_run_id":          response["job_run_id"],
			"correlation_id":      response["correlation_id"],
			"job_type":            response["job_type"],
			"status":              response["status"],
			"video_mode":          scriptSceneMode,
			"video_name":          normalized["video_name"],
			"output_path":         normalized["output_path"],
			"drive_output_folder": normalized["drive_output_folder"],
			"scene_count":         response["scene_count"],
			"voiceover_count":     response["voiceover_count"],
			"scene_image_paths":   normalized["scene_image_paths"],
			"enqueue":             response,
		})
	}
}

func (h *ScriptHandlers) ScriptJobHandler(full bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("job_id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}
		job, err := h.loadJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job not found"})
			return
		}
		c.JSON(http.StatusOK, renderJobResponse(job, full))
	}
}

func (h *ScriptHandlers) ScriptByIDHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		scriptID := strings.TrimSpace(c.Param("script_id"))
		if scriptID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "script_id required"})
			return
		}
		job, err := h.loadJob(c.Request.Context(), scriptID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "script not found"})
			return
		}
		c.JSON(http.StatusOK, renderJobResponse(job, true))
	}
}

func (h *ScriptHandlers) loadJob(ctx context.Context, jobID string) (map[string]interface{}, error) {
	if h.sqliteDB == nil {
		return nil, nil
	}
	job, err := h.sqliteDB.GetJob(ctx, jobID)
	if err != nil {
		return nil, nil
	}
	return job, nil
}

func (h *ScriptHandlers) buildSceneImagePayload(cfg *config.Config, payload map[string]interface{}) (map[string]interface{}, error) {
	videoName := firstNonEmptyString(payload, "video_name", "title", "topic")
	if videoName == "" {
		videoName = sanitizeVideoName(firstNonEmptyString(payload, "topic", "source_text"))
	}
	if videoName == "" {
		videoName = "script_with_images_" + time.Now().UTC().Format("20060102_150405")
	}

	scriptText := firstNonEmptyString(payload, "script_text")
	if scriptText == "" {
		scriptText = buildScriptText(payload)
	}

	sceneEntries, sceneImagePaths, err := normalizeScenesPayload(payload)
	if err != nil {
		return nil, err
	}
	if len(sceneEntries) == 0 {
		return nil, fmt.Errorf("at least one scene or image is required")
	}

	voiceoverPaths := normalizeStringList(payload, "voiceover_paths", "voiceover_path", "audio_path", "source_media", "source_media_url", "audio_source")
	if len(voiceoverPaths) == 0 {
		if src := firstNonEmptyString(payload, "source_text"); isLikelyMediaSource(src) {
			voiceoverPaths = []string{src}
		}
	}
	if len(voiceoverPaths) == 0 {
		return nil, fmt.Errorf("voiceover_path or source_media is required")
	}

	sceneCount := len(sceneEntries)
	_ = intFromPayload(payload, 1, "scene_count") // accepted for backwards compatibility

	totalDuration := floatFromPayload(payload, 0, "total_duration_secs", "duration_secs", "video_duration_secs")
	perSceneDuration := floatFromPayload(payload, 0, "scene_duration_secs", "image_duration_secs")
	if perSceneDuration <= 0 && totalDuration > 0 {
		perSceneDuration = totalDuration / float64(sceneCount)
	}
	if perSceneDuration <= 0 {
		perSceneDuration = 5
	}
	if totalDuration <= 0 {
		totalDuration = perSceneDuration * float64(sceneCount)
	}

	outputPath := firstNonEmptyString(payload, "output_path")
	if outputPath == "" {
		outputPath = h.defaultOutputPath(cfg, videoName)
	}

	jobID := firstNonEmptyString(payload, "job_id", "script_id")
	if jobID == "" {
		jobID = "scriptimg_" + uuid.NewString()
	}
	jobRunID := firstNonEmptyString(payload, "job_run_id", "run_id")
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := firstNonEmptyString(payload, "correlation_id")
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}

	now := time.Now().UTC().Format(time.RFC3339)
	audioLanguage := firstNonEmptyString(payload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(payload)+24)
	for k, v := range payload {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["version"] = "v1"
	normalized["created_at"] = ensureRFC3339(firstNonEmptyString(payload, "created_at"), now)
	normalized["updated_at"] = ensureRFC3339(firstNonEmptyString(payload, "updated_at"), now)
	normalized["video_name"] = videoName
	normalized["title"] = videoName
	normalized["script_text"] = scriptText
	normalized["scenes"] = sceneEntries
	normalized["scenes_json"] = mustJSON(sceneEntries)
	normalized["voiceover_paths"] = voiceoverPaths
	normalized["voiceover_path"] = voiceoverPaths[0]
	normalized["audio_path"] = voiceoverPaths[0]
	normalized["audio_language_for_srt"] = audioLanguage
	normalized["video_mode"] = scriptSceneMode
	normalized["output_path"] = outputPath
	normalized["drive_output_folder"] = firstNonEmptyString(payload, "drive_output_folder", "output_directory")
	normalized["scene_count"] = sceneCount
	normalized["voiceover_count"] = len(voiceoverPaths)
	normalized["total_duration_secs"] = totalDuration
	normalized["scene_duration_secs"] = perSceneDuration
	normalized["scene_image_paths"] = sceneImagePaths
	normalized["priority"] = ensureInt(payload["priority"], 1)
	normalized["timeout_secs"] = ensureInt(payload["timeout_secs"], 3600)
	normalized["submitted_via"] = "api_script_generate_with_images"
	normalized["source"] = "script_generate_with_images"

	normalized["parameters"] = map[string]interface{}{
		"version":                "v1",
		"job_id":                 jobID,
		"job_run_id":             jobRunID,
		"run_id":                 jobRunID,
		"correlation_id":         correlationID,
		"job_type":               "process_video",
		"video_name":             videoName,
		"script_text":            scriptText,
		"scenes_json":            normalized["scenes_json"],
		"scenes":                 sceneEntries,
		"voiceover_paths":        voiceoverPaths,
		"voiceover_path":         voiceoverPaths[0],
		"audio_path":             voiceoverPaths[0],
		"audio_language_for_srt": audioLanguage,
		"video_mode":             scriptSceneMode,
		"output_path":            outputPath,
		"drive_output_folder":    normalized["drive_output_folder"],
		"scene_count":            sceneCount,
		"voiceover_count":        len(voiceoverPaths),
		"total_duration_secs":    totalDuration,
		"scene_duration_secs":    perSceneDuration,
		"scene_image_paths":      sceneImagePaths,
		"priority":               normalized["priority"],
		"timeout_secs":           normalized["timeout_secs"],
		"submitted_via":          "api_script_generate_with_images",
		"source":                 "script_generate_with_images",
	}

	return normalized, nil
}

func (h *ScriptHandlers) defaultOutputPath(cfg *config.Config, videoName string) string {
	base := ""
	if cfg != nil {
		base = strings.TrimSpace(cfg.VideosDir)
	}
	if base == "" {
		if h.dataDir != "" {
			base = filepath.Join(h.dataDir, "generated_videos")
		} else {
			base = filepath.Join(".", "generated_videos")
		}
	}
	slug := sanitizeVideoName(videoName)
	if slug == "" {
		slug = "script_with_images"
	}
	return filepath.Join(base, "script_with_images", slug+".mp4")
}

func normalizeScenesPayload(payload map[string]interface{}) ([]map[string]interface{}, []string, error) {
	if scenes := normalizeSceneArray(payload["scenes"]); len(scenes) > 0 {
		sceneEntries := make([]map[string]interface{}, 0, len(scenes))
		sceneImagePaths := make([]string, 0, len(scenes))
		fallbacks := collectSceneImageCandidates(scenes)
		for idx, scene := range scenes {
			normalized := normalizeSceneEntry(scene)
			if image, ok := normalized["image_link"].(string); !ok || strings.TrimSpace(image) == "" {
				if len(fallbacks) > 0 {
					fallback := fallbacks[idx%len(fallbacks)]
					normalized["image_link"] = fallback
					normalized["image_links"] = []string{fallback}
				}
			}
			if image := firstSceneImageLink(normalized); image != "" {
				sceneImagePaths = append(sceneImagePaths, image)
			}
			if duration := normalizedDuration(normalized["duration_seconds"]); duration <= 0 {
				normalized["duration_seconds"] = 5.0
			}
			sceneEntries = append(sceneEntries, normalized)
		}
		return sceneEntries, dedupeStrings(sceneImagePaths), nil
	}

	if raw := firstNonEmptyString(payload, "scenes_json"); raw != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, nil, fmt.Errorf("invalid scenes_json: %w", err)
		}
		return normalizeScenesPayload(map[string]interface{}{"scenes": scenes})
	}

	images := normalizeStringList(payload, "images", "image_links", "image_urls", "image_paths")
	if len(images) == 0 {
		return nil, nil, fmt.Errorf("scenes or images are required")
	}
	sceneCount := intFromPayload(payload, len(images), "scene_count")
	if sceneCount <= 0 {
		sceneCount = len(images)
	}
	perSceneDuration := floatFromPayload(payload, 5, "scene_duration_secs", "image_duration_secs")
	totalDuration := floatFromPayload(payload, 0, "total_duration_secs", "duration_secs", "video_duration_secs")
	if totalDuration > 0 {
		perSceneDuration = totalDuration / float64(sceneCount)
	}

	sceneEntries := make([]map[string]interface{}, 0, sceneCount)
	sceneImagePaths := make([]string, 0, sceneCount)
	for i := 0; i < sceneCount; i++ {
		img := images[i%len(images)]
		scene := map[string]interface{}{
			"text":             fmt.Sprintf("Scene %d", i+1),
			"image_link":       img,
			"image_links":      []string{img},
			"duration_seconds": perSceneDuration,
			"zoom": map[string]interface{}{
				"type":        "light_zoom_in",
				"start_scale": 1.0,
				"end_scale":   1.08,
			},
		}
		sceneEntries = append(sceneEntries, scene)
		sceneImagePaths = append(sceneImagePaths, img)
	}
	return sceneEntries, dedupeStrings(sceneImagePaths), nil
}

func normalizeSceneArray(value interface{}) []map[string]interface{} {
	switch scenes := value.(type) {
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, normalizeSceneEntry(scene))
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, item := range scenes {
			scene, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			out = append(out, normalizeSceneEntry(scene))
		}
		return out
	default:
		return nil
	}
}

func normalizeSceneEntry(scene map[string]interface{}) map[string]interface{} {
	normalized := make(map[string]interface{}, len(scene)+4)
	for k, v := range scene {
		normalized[k] = v
	}
	if text := firstNonEmptyString(scene, "text"); text != "" {
		normalized["text"] = text
	}
	if image := firstNonEmptyString(scene, "image_link", "image_url", "image"); image != "" {
		normalized["image_link"] = image
	}
	if links := normalizeStringList(scene, "image_links"); len(links) > 0 {
		normalized["image_links"] = links
	} else if image := firstNonEmptyString(scene, "image_link"); image != "" {
		normalized["image_links"] = []string{image}
	}
	if duration := normalizedDuration(normalized["duration_seconds"]); duration <= 0 {
		normalized["duration_seconds"] = 5.0
	}
	return normalized
}

func collectSceneImageCandidates(scenes []map[string]interface{}) []string {
	out := make([]string, 0, len(scenes))
	for _, scene := range scenes {
		if image := firstSceneImageLink(scene); image != "" {
			out = append(out, image)
		}
	}
	return dedupeStrings(out)
}

func firstSceneImageLink(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if image := firstNonEmptyString(scene, "image_link", "image_url", "image"); image != "" {
		return image
	}
	if links := normalizeStringList(scene, "image_links"); len(links) > 0 {
		return links[0]
	}
	return ""
}

func normalizeStringList(source map[string]interface{}, keys ...string) []string {
	if source == nil {
		return nil
	}
	var values []string
	for _, key := range keys {
		v, ok := source[key]
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case []string:
			values = append(values, vv...)
		case []interface{}:
			for _, item := range vv {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					values = append(values, strings.TrimSpace(s))
				}
			}
		case string:
			for _, line := range strings.Split(vv, "\n") {
				if s := strings.TrimSpace(line); s != "" {
					values = append(values, s)
				}
			}
		}
	}
	return dedupeStrings(values)
}

func firstNonEmptyString(source map[string]interface{}, keys ...string) string {
	if source == nil {
		return ""
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case string:
				if s := strings.TrimSpace(vv); s != "" {
					return s
				}
			case fmt.Stringer:
				if s := strings.TrimSpace(vv.String()); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

func floatFromPayload(source map[string]interface{}, fallback float64, keys ...string) float64 {
	if source == nil {
		return fallback
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case float64:
				if vv > 0 {
					return vv
				}
			case float32:
				if vv > 0 {
					return float64(vv)
				}
			case int:
				if vv > 0 {
					return float64(vv)
				}
			case int64:
				if vv > 0 {
					return float64(vv)
				}
			case json.Number:
				if f, err := vv.Float64(); err == nil && f > 0 {
					return f
				}
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(vv), 64); err == nil && f > 0 {
					return f
				}
			}
		}
	}
	return fallback
}

func intFromPayload(source map[string]interface{}, fallback int, key string) int {
	if source == nil {
		return fallback
	}
	if v, ok := source[key]; ok {
		switch vv := v.(type) {
		case int:
			if vv > 0 {
				return vv
			}
		case int64:
			if vv > 0 {
				return int(vv)
			}
		case float64:
			if vv > 0 {
				return int(vv)
			}
		case json.Number:
			if n, err := vv.Int64(); err == nil && n > 0 {
				return int(n)
			}
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil && n > 0 {
				return n
			}
		}
	}
	return fallback
}

func ensureInt(value interface{}, fallback int) int {
	switch v := value.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			return int(n)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func normalizedDuration(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return f
	default:
		return 0
	}
}

func ensureRFC3339(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	if _, err := time.Parse(time.RFC3339, value); err == nil {
		return value
	}
	return fallback
}

func sanitizeVideoName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('_')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func buildScriptText(payload map[string]interface{}) string {
	var parts []string
	if s := firstNonEmptyString(payload, "topic", "title"); s != "" {
		parts = append(parts, s)
	}
	if s := firstNonEmptyString(payload, "source_text"); s != "" {
		parts = append(parts, s)
	}
	if len(parts) == 0 {
		parts = append(parts, "script with images")
	}
	return strings.Join(parts, " - ")
}

func isLikelyMediaSource(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	return strings.HasPrefix(value, "http://") ||
		strings.HasPrefix(value, "https://") ||
		strings.HasPrefix(value, "file://") ||
		strings.HasSuffix(value, ".mp4") ||
		strings.HasSuffix(value, ".mov") ||
		strings.HasSuffix(value, ".mkv") ||
		strings.HasSuffix(value, ".webm") ||
		strings.HasSuffix(value, ".mp3") ||
		strings.HasSuffix(value, ".wav") ||
		strings.HasSuffix(value, ".m4a")
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func mustJSON(v interface{}) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func renderJobResponse(job map[string]interface{}, full bool) map[string]interface{} {
	if job == nil {
		return map[string]interface{}{"ok": false}
	}
	response := map[string]interface{}{
		"ok":                  true,
		"job_id":              firstString(job, "job_id"),
		"script_id":           firstString(job, "job_id", "script_id"),
		"status":              firstString(job, "status"),
		"video_name":          firstString(job, "video_name", "title"),
		"job_run_id":          firstString(job, "job_run_id", "run_id"),
		"run_id":              firstString(job, "run_id", "job_run_id"),
		"created_at":          job["created_at"],
		"updated_at":          job["updated_at"],
		"started_at":          job["started_at"],
		"completed_at":        job["completed_at"],
		"output_path":         firstString(job, "output_path"),
		"drive_output_folder": firstString(job, "drive_output_folder"),
		"scene_count":         job["scene_count"],
		"voiceover_count":     job["voiceover_count"],
		"video_mode":          firstString(job, "video_mode"),
	}
	if errMsg := firstString(job, "error", "last_error", "error_message"); errMsg != "" {
		response["error"] = errMsg
	}
	if result := job["result"]; result != nil {
		response["result"] = result
	}
	if full {
		response["job"] = job
		response["request"] = job["request"]
	}
	return response
}

func firstString(source map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case string:
				if strings.TrimSpace(vv) != "" {
					return strings.TrimSpace(vv)
				}
			case fmt.Stringer:
				if strings.TrimSpace(vv.String()) != "" {
					return strings.TrimSpace(vv.String())
				}
			}
		}
	}
	return ""
}
