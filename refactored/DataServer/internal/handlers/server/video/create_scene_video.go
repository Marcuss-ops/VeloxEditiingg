package video

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

// CreateFromScenes accepts POST /api/v1/video/create-scenes and enqueues a
// process_video job built from a script plus per-scene image links.
func CreateFromScenes(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid JSON"})
			return
		}
		if payload == nil {
			payload = make(map[string]interface{})
		}

		normalized, err := normalizeSceneVideoPayload(payload)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		if cfg != nil && cfg.MasterServerURL != "" && isDraftScenePayload(normalized) {
			url := strings.TrimSuffix(cfg.MasterServerURL, "/") + "/api/v1/video/create-scenes"
			body, marshalErr := json.Marshal(normalized)
			if marshalErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": marshalErr.Error()})
				return
			}
			ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Minute)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": "Proxy to master: " + err.Error()})
				return
			}
			defer resp.Body.Close()
			var proxyRes map[string]interface{}
			_ = json.NewDecoder(resp.Body).Decode(&proxyRes)
			if proxyRes == nil {
				proxyRes = make(map[string]interface{})
			}
			c.JSON(resp.StatusCode, proxyRes)
			return
		}

		response, err := EnqueueSceneVideoJob(c.Request.Context(), q, normalized)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(strings.ToLower(err.Error()), "queue unavailable") {
				status = http.StatusServiceUnavailable
			}
			c.JSON(status, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, response)
	}
}

// EnqueueSceneVideoJob normalizes a scene-video payload and persists it to the queue.
// It returns the same response shape as CreateFromScenes.
func EnqueueSceneVideoJob(ctx context.Context, q *queue.FileQueue, payload map[string]interface{}) (map[string]interface{}, error) {
	if q == nil {
		return nil, fmt.Errorf("queue unavailable")
	}

	normalized, err := normalizeSceneVideoPayload(payload)
	if err != nil {
		return nil, err
	}

	jobID, _ := normalized["job_id"].(string)
	if jobID == "" {
		jobID = uuid.NewString()
		normalized["job_id"] = jobID
	}

	if err := q.SubmitJob(ctx, jobID, normalized); err != nil {
		return nil, err
	}

	return buildSceneVideoResponse(normalized), nil
}

func normalizeSceneVideoPayload(payload map[string]interface{}) (map[string]interface{}, error) {
	title := strings.TrimSpace(firstString(payload, "video_name", "title", "project_name"))
	if title == "" {
		return nil, &validationError{field: "video_name", message: "is required"}
	}

	scriptText := strings.TrimSpace(firstString(payload, "script_text", "script"))
	if scriptText == "" {
		return nil, &validationError{field: "script_text", message: "is required"}
	}

	scenesValue, scenesJSON, err := normalizeScenes(payload)
	if err != nil {
		return nil, err
	}
	if len(scenesValue) == 0 {
		return nil, &validationError{field: "scenes", message: "at least one scene is required"}
	}

	voiceovers := normalizeVoiceoverList(payload)
	if len(voiceovers) == 0 {
		return nil, &validationError{field: "voiceover_paths", message: "at least one voiceover path is required"}
	}
	clipPaths := extractSceneClipPaths(scenesValue)

	now := time.Now().UTC().Format(time.RFC3339)
	jobID := strings.TrimSpace(firstString(payload, "job_id", "id"))
	if jobID == "" {
		jobID = "scene_" + uuid.NewString()
	}
	jobRunID := strings.TrimSpace(firstString(payload, "job_run_id", "run_id"))
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := strings.TrimSpace(firstString(payload, "correlation_id"))
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}
	jobFingerprint := sceneVideoFingerprint(
		jobID,
		title,
		scriptText,
		scenesJSON,
		voiceovers,
		firstString(payload, "youtube_group"),
		firstString(payload, "output_path"),
		firstString(payload, "audio_language_for_srt", "audio_lang"),
	)

	normalized := make(map[string]interface{}, len(payload)+16)
	for k, v := range payload {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["created_at"] = ensureRFC3339(firstString(payload, "created_at"), now)
	normalized["updated_at"] = ensureRFC3339(firstString(payload, "updated_at"), now)
	normalized["video_name"] = title
	normalized["title"] = title
	normalized["script_text"] = scriptText
	normalized["scenes"] = scenesValue
	normalized["scenes_json"] = scenesJSON
	normalized["voiceover_paths"] = voiceovers
	normalized["voiceover_path"] = voiceovers[0]
	normalized["audio_path"] = voiceovers[0]
	normalized["stock_clip_paths"] = clipPaths
	normalized["priority"] = ensureInt(payload["priority"], 1)
	normalized["timeout_secs"] = ensureInt(payload["timeout_secs"], 3600)
	normalized["scene_count"] = len(scenesValue)
	normalized["voiceover_count"] = len(voiceovers)
	normalized["submitted_via"] = "api_v1_scene_video"
	normalized["source"] = "scene_video_api"
	normalized["job_fingerprint"] = jobFingerprint
	normalized["version"] = "v1"
	normalized["render_plan_version"] = "v1"
	normalized["parameters"] = map[string]interface{}{
		"version":             "v1",
		"render_plan_version": "v1",
		"job_id":              jobID,
		"job_run_id":          jobRunID,
		"run_id":              jobRunID,
		"correlation_id":      correlationID,
		"job_type":            "process_video",
		"video_name":          title,
		"script_text":         scriptText,
		"scenes_json":         scenesJSON,
		"scenes":              scenesValue,
		"voiceover_paths":     voiceovers,
		"audio_path":          voiceovers[0],
		"stock_clip_paths":    clipPaths,
		"youtube_group":       firstString(payload, "youtube_group"),
		"output_path":         firstString(payload, "output_path"),
		"job_fingerprint":     jobFingerprint,
		"submitted_via":       "api_v1_scene_video",
		"source":              "scene_video_api",
		"scene_count":         len(scenesValue),
		"voiceover_count":     len(voiceovers),
		"priority":            ensureInt(payload["priority"], 1),
		"timeout_secs":        ensureInt(payload["timeout_secs"], 3600),
	}

	if v := strings.TrimSpace(firstString(payload, "youtube_group")); v != "" {
		normalized["youtube_group"] = v
	}
	if v := strings.TrimSpace(firstString(payload, "output_video_id")); v != "" {
		normalized["output_video_id"] = v
	}
	if v := strings.TrimSpace(firstString(payload, "audio_language_for_srt", "audio_lang")); v != "" {
		normalized["audio_language_for_srt"] = v
		normalized["parameters"].(map[string]interface{})["audio_language_for_srt"] = v
	}
	if v := strings.TrimSpace(firstString(payload, "output_path")); v != "" {
		normalized["output_path"] = v
		normalized["parameters"].(map[string]interface{})["output_path"] = v
	}

	return normalized, nil
}

func extractSceneClipPaths(scenes []map[string]interface{}) []string {
	seen := make(map[string]struct{})
	paths := make([]string, 0, len(scenes))
	for _, scene := range scenes {
		candidates := []string{
			firstString(scene, "image_link", "image_url", "image"),
		}
		if v, ok := scene["image_links"]; ok {
			candidates = append(candidates, normalizeToStrings(v)...)
		}
		for _, candidate := range candidates {
			trimmed := strings.TrimSpace(candidate)
			if trimmed == "" {
				continue
			}
			if _, exists := seen[trimmed]; exists {
				continue
			}
			seen[trimmed] = struct{}{}
			paths = append(paths, trimmed)
		}
	}
	return paths
}

func buildSceneVideoResponse(normalized map[string]interface{}) map[string]interface{} {
	jobID, _ := normalized["job_id"].(string)
	jobRunID := strings.TrimSpace(firstString(normalized, "job_run_id", "run_id"))
	correlationID := strings.TrimSpace(firstString(normalized, "correlation_id"))
	jobFingerprint := strings.TrimSpace(firstString(normalized, "job_fingerprint"))

	return map[string]interface{}{
		"ok":                true,
		"job_id":            jobID,
		"job_run_id":        jobRunID,
		"correlation_id":    correlationID,
		"job_type":          "process_video",
		"status":            "PENDING",
		"enqueue_confirmed": true,
		"dispatch_status":   "queued_for_workers",
		"scene_count":       sceneCountFromPayload(normalized),
		"voiceover_count":   voiceoverCountFromPayload(normalized),
		"job_fingerprint":   jobFingerprint,
	}
}

func normalizeScenes(payload map[string]interface{}) ([]map[string]interface{}, string, error) {
	if v, ok := payload["scenes"]; ok {
		switch scenes := v.(type) {
		case []interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				result = append(result, normalizeSceneEntry(m))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		case []map[string]interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				result = append(result, normalizeSceneEntry(item))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		}
	}

	if s, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err != nil {
			return nil, "", err
		}
		for i := range scenes {
			scenes[i] = normalizeSceneEntry(scenes[i])
		}
		data, err := json.Marshal(scenes)
		if err != nil {
			return nil, "", err
		}
		return scenes, string(data), nil
	}

	return nil, "", nil
}

func normalizeSceneEntry(scene map[string]interface{}) map[string]interface{} {
	normalized := map[string]interface{}{}
	for k, v := range scene {
		normalized[k] = v
	}
	if text, ok := scene["text"].(string); ok {
		normalized["text"] = strings.TrimSpace(text)
	}
	if link := strings.TrimSpace(firstString(scene, "image_link", "image_url", "image")); link != "" {
		normalized["image_link"] = link
	}
	if links, ok := scene["image_links"].([]interface{}); ok && len(links) > 0 {
		clean := make([]string, 0, len(links))
		for _, item := range links {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				clean = append(clean, strings.TrimSpace(s))
			}
		}
		normalized["image_links"] = clean
	} else if link := strings.TrimSpace(firstString(scene, "image_link")); link != "" {
		normalized["image_links"] = []string{link}
	}
	return normalized
}

func normalizeVoiceoverList(payload map[string]interface{}) []string {
	candidates := []string{
		firstString(payload, "voiceover_path", "voiceover", "unified_voiceover_link"),
	}
	if v, ok := payload["voiceover_paths"]; ok {
		candidates = append(candidates, normalizeToStrings(v)...)
	}
	if v, ok := payload["voiceovers"]; ok {
		candidates = append(candidates, normalizeToStrings(v)...)
	}
	if v, ok := payload["voiceovers_urls"]; ok {
		candidates = append(candidates, normalizeToStrings(v)...)
	}

	result := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, item := range candidates {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			if _, exists := seen[trimmed]; exists {
				continue
			}
			seen[trimmed] = struct{}{}
			result = append(result, trimmed)
		}
	}
	return result
}

func normalizeToStrings(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return nil
		}
		if strings.Contains(s, "\n") {
			lines := strings.Split(s, "\n")
			out := make([]string, 0, len(lines))
			for _, line := range lines {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					out = append(out, trimmed)
				}
			}
			return out
		}
		return []string{s}
	default:
		return nil
	}
}

func firstString(payload map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := payload[key]; ok {
			if s, ok := val.(string); ok {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
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
	}
	return fallback
}

func sceneCountFromPayload(payload map[string]interface{}) int {
	if scenes, ok := payload["scenes"].([]interface{}); ok {
		return len(scenes)
	}
	if scenes, ok := payload["scenes"].([]map[string]interface{}); ok {
		return len(scenes)
	}
	if s, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err == nil {
			return len(scenes)
		}
	}
	return 0
}

func voiceoverCountFromPayload(payload map[string]interface{}) int {
	if arr, ok := payload["voiceover_paths"].([]string); ok {
		return len(arr)
	}
	if arr, ok := payload["voiceover_paths"].([]interface{}); ok {
		return len(arr)
	}
	return len(normalizeVoiceoverList(payload))
}

func sceneVideoFingerprint(parts ...interface{}) string {
	h := sha256.New()
	for _, part := range parts {
		switch v := part.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				h.Write([]byte(trimmed))
			}
		case []string:
			for _, item := range v {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					h.Write([]byte(trimmed))
				}
			}
		default:
			if part == nil {
				continue
			}
			if data, err := json.Marshal(part); err == nil {
				h.Write(data)
			}
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func isDraftScenePayload(payload map[string]interface{}) bool {
	return strings.TrimSpace(firstString(payload, "submission_mode")) == "draft"
}

type validationError struct {
	field   string
	message string
}

func (e *validationError) Error() string {
	return e.field + ": " + e.message
}
