package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	scenevideo "velox-server/internal/handlers/server/video"
	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
)

var remoteEngineClient *remoteengine.Client

// InitRemoteEngine initializes the remote engine client
func InitRemoteEngine(cfg *config.Config) {
	remoteEngineClient = remoteengine.NewClient(remoteengine.Config{
		URL:       cfg.RemoteEngineURL,
		Token:     cfg.RemoteEngineToken,
		TimeoutMS: cfg.RemoteEngineTimeoutMS,
		Retries:   cfg.RemoteEngineRetries,
	})
}

// PipelineGenerate handles POST /api/remote/pipeline/generate
func PipelineGenerate(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.StartPipeline(c.Request.Context(), payload)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			response := gin.H{}
			for k, v := range result {
				response[k] = v
			}

			if shouldForwardPipelineResult(result) {
				if forwarded, forwardErr := forwardPipelineResultToWorker(c.Request.Context(), q, result); forwardErr != nil {
					response["worker_forwarded"] = false
					response["worker_forward_error"] = forwardErr.Error()
				} else {
					response["worker_forwarded"] = true
					response["worker_forward_result"] = forwarded
				}
			} else {
				response["worker_forwarded"] = false
				response["worker_forward_error"] = "pipeline result is not complete enough for worker handoff"
			}

			c.JSON(http.StatusOK, response)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

func shouldForwardPipelineResult(result map[string]interface{}) bool {
	if result == nil {
		return false
	}
	flat := flattenPipelineResult(result)
	status := strings.ToLower(strings.TrimSpace(firstStringFromMap(flat, "status")))
	if status != "" && status != "completed" && status != "succeeded" && status != "done" {
		return false
	}
	if firstStringFromMap(flat, "scenes_json", "json_path") == "" && firstStringFromMap(flat, "scenes") == "" {
		return false
	}
	if len(extractVoiceoverPaths(flat)) == 0 {
		return false
	}
	return true
}

func forwardPipelineResultToWorker(ctx context.Context, q *queue.FileQueue, result map[string]interface{}) (map[string]interface{}, error) {
	payload, err := buildSceneVideoPayloadFromPipelineResult(result)
	if err != nil {
		return nil, err
	}
	return scenevideo.EnqueueSceneVideoJob(ctx, q, payload)
}

func buildSceneVideoPayloadFromPipelineResult(result map[string]interface{}) (map[string]interface{}, error) {
	if result == nil {
		return nil, fmt.Errorf("pipeline result is empty")
	}

	flat := flattenPipelineResult(result)

	title := firstStringFromMap(flat, "video_name", "title", "script_title", "name")
	if title == "" {
		title = firstMetadataTitle(flat)
	}

	scriptText := firstStringFromMap(flat, "script_text", "script", "generated_script", "text")
	if scriptText == "" {
		if markdownPath := firstStringFromMap(flat, "markdown_path"); markdownPath != "" {
			if data, readErr := os.ReadFile(markdownPath); readErr == nil {
				scriptText = strings.TrimSpace(string(data))
			}
		}
	}

	scenesJSON := firstStringFromMap(flat, "scenes_json")
	if scenesJSON == "" {
		if scenesValue, ok := flat["scenes"]; ok {
			if data, marshalErr := json.Marshal(scenesValue); marshalErr == nil {
				scenesJSON = string(data)
			}
		}
	}
	if scenesJSON == "" {
		if jsonPath := firstStringFromMap(flat, "json_path"); jsonPath != "" {
			if extracted, extractErr := extractScenesJSONFromFile(jsonPath); extractErr == nil {
				scenesJSON = extracted
			}
		}
	}

	voiceovers := extractVoiceoverPaths(flat)
	if len(voiceovers) == 0 {
		return nil, fmt.Errorf("voiceover path missing from pipeline result")
	}
	if title == "" {
		return nil, fmt.Errorf("video title missing from pipeline result")
	}
	if scriptText == "" {
		return nil, fmt.Errorf("script text missing from pipeline result")
	}
	if scenesJSON == "" {
		return nil, fmt.Errorf("scenes payload missing from pipeline result")
	}

	payload := map[string]interface{}{
		"job_id":                 firstStringFromMap(flat, "job_id", "script_id", "trace_id"),
		"job_run_id":             firstStringFromMap(flat, "job_run_id", "run_id", "trace_id"),
		"run_id":                 firstStringFromMap(flat, "run_id", "job_run_id", "trace_id"),
		"correlation_id":         firstStringFromMap(flat, "correlation_id", "trace_id"),
		"video_name":             title,
		"title":                  title,
		"script_text":            scriptText,
		"scenes_json":            scenesJSON,
		"voiceover_paths":        voiceovers,
		"voiceover_path":         voiceovers[0],
		"audio_path":             voiceovers[0],
		"output_path":            firstStringFromMap(flat, "output_path", "output_dir"),
		"youtube_group":          firstStringFromMap(flat, "youtube_group"),
		"audio_language_for_srt": firstStringFromMap(flat, "audio_language_for_srt", "audio_lang"),
		"job_type":               "process_video",
		"render_plan_version":    "v1",
		"submitted_via":          "pipeline_generate_with_images",
		"source":                 "pipeline_generate_with_images",
		"priority":               1,
		"timeout_secs":           3600,
	}

	if jobID := strings.TrimSpace(firstStringFromMap(flat, "job_id", "script_id", "trace_id")); jobID != "" {
		payload["job_id"] = jobID
		payload["id"] = jobID
	}

	if runID := strings.TrimSpace(firstStringFromMap(flat, "job_run_id", "run_id", "trace_id")); runID != "" {
		payload["job_run_id"] = runID
		payload["run_id"] = runID
	}

	if corrID := strings.TrimSpace(firstStringFromMap(flat, "correlation_id", "trace_id")); corrID != "" {
		payload["correlation_id"] = corrID
	}

	if len(voiceovers) > 0 {
		payload["voiceover_path"] = voiceovers[0]
		payload["audio_path"] = voiceovers[0]
	}

	return payload, nil
}

func flattenPipelineResult(result map[string]interface{}) map[string]interface{} {
	flat := make(map[string]interface{}, len(result)+8)
	for k, v := range result {
		flat[k] = v
	}
	if nested, ok := result["result"].(map[string]interface{}); ok {
		for k, v := range nested {
			flat[k] = v
		}
	}
	return flat
}

func firstStringFromMap(payload map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := payload[key]; ok {
			switch v := val.(type) {
			case string:
				if trimmed := strings.TrimSpace(v); trimmed != "" {
					return trimmed
				}
			case fmt.Stringer:
				if trimmed := strings.TrimSpace(v.String()); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func firstMetadataTitle(payload map[string]interface{}) string {
	metadata, ok := payload["metadata"]
	if !ok {
		return ""
	}
	switch v := metadata.(type) {
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				if title := firstStringFromMap(m, "title", "name"); title != "" {
					return title
				}
			}
		}
	case []map[string]interface{}:
		for _, item := range v {
			if title := firstStringFromMap(item, "title", "name"); title != "" {
				return title
			}
		}
	}
	return ""
}

func extractVoiceoverPaths(payload map[string]interface{}) []string {
	var candidates []string

	if s := firstStringFromMap(payload, "voiceover_path", "audio_path", "voiceover"); s != "" {
		candidates = append(candidates, s)
	}
	if v, ok := payload["voiceover_paths"]; ok {
		candidates = append(candidates, normalizeStringList(v)...)
	}

	if voiceover, ok := payload["voiceover"].(map[string]interface{}); ok {
		candidates = append(candidates,
			firstStringFromMap(voiceover, "local_path", "path", "drive_link", "url"),
		)
	}
	if nested, ok := payload["voiceover_info"].(map[string]interface{}); ok {
		candidates = append(candidates,
			firstStringFromMap(nested, "local_path", "path", "drive_link", "url"),
		)
	}

	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func normalizeStringList(v interface{}) []string {
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

func extractScenesJSONFromFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	var raw interface{}
	if err := json.Unmarshal(bytes.TrimSpace(data), &raw); err != nil {
		return "", err
	}

	switch v := raw.(type) {
	case map[string]interface{}:
		for _, key := range []string{"scenes_json", "scenes", "scene_plan", "scene_json"} {
			if value, ok := v[key]; ok {
				data, err := json.Marshal(value)
				if err != nil {
					return "", err
				}
				return string(data), nil
			}
		}
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(data), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
}

// PipelineStatus handles GET /api/remote/pipeline/status/<trace_id>
func PipelineStatus(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GetPipelineStatus(c.Request.Context(), traceID)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, result)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":       false,
			"trace_id": traceID,
			"error":    "remote engine not configured",
			"hint":     "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

// ScriptSimple handles POST /api/script-simple
func ScriptSimple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.SimpleScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateSimpleScript(c.Request.Context(), req)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, result)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

// ScriptMultiple handles POST /api/script-multiple
func ScriptMultiple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.BatchScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateBatchScripts(c.Request.Context(), req)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, result)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}
