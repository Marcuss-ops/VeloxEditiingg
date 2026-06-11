// Package video fornisce handler HTTP per la creazione e l'inoltro di job video
// (process_video). Include normalizzazione dei payload, fingerprinting per deduplicazione
// e proxy verso master server per job draft.
//
// Endpoint:
//   POST /api/v1/video/create-scene → CreateFromScenes
package video

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"velox-shared/contract"
	"velox-shared/payload"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

// CreateFromScenes accetta POST /api/v1/video/create-scenes e inoltra un job
// process_video costruito da un payload con script + scene + voiceover.
// Supporta modalità "draft" (proxy a master server) e enqueue diretto.
func CreateFromScenes(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payloadMap map[string]interface{}
		if err := c.ShouldBindJSON(&payloadMap); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid JSON"})
			return
		}
		if payloadMap == nil {
			payloadMap = make(map[string]interface{})
		}

		normalized, err := normalizeSceneVideoPayload(payloadMap)
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
			if err := json.NewDecoder(resp.Body).Decode(&proxyRes); err != nil {
				log.Printf("create_scene_video: failed to decode proxy response: %v", err)
			}
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

// EnqueueSceneVideoJob normalizza un payload scene-video e lo persiste nella coda.
// Restituisce la stessa struttura di risposta di CreateFromScenes.
// Può essere chiamato direttamente da altri handler (es. script handler).
func EnqueueSceneVideoJob(ctx context.Context, q *queue.FileQueue, payloadMap map[string]interface{}) (map[string]interface{}, error) {
	if q == nil {
		return nil, fmt.Errorf("queue unavailable")
	}

	normalized, err := normalizeSceneVideoPayload(payloadMap)
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

// normalizeSceneVideoPayload normalizza e valida un payload per un job scene video.
// Richiede: video_name, script_text, almeno una scena, almeno un voiceover_path.
// Genera job_id, run_id, correlation_id se non forniti e calcola fingerprint.
func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	title := strings.TrimSpace(firstString(payloadMap, "video_name", "title", "project_name"))
	if title == "" {
		return nil, &validationError{field: "video_name", message: "is required"}
	}

	scriptText := strings.TrimSpace(firstString(payloadMap, "script_text", "script"))
	if scriptText == "" {
		return nil, &validationError{field: "script_text", message: "is required"}
	}

	scenesValue, scenesJSON, err := normalizeScenes(payloadMap)
	if err != nil {
		return nil, err
	}
	if len(scenesValue) == 0 {
		return nil, &validationError{field: "scenes", message: "at least one scene is required"}
	}

	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 {
		return nil, &validationError{field: "voiceover_paths", message: "at least one voiceover path is required"}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	jobID := strings.TrimSpace(firstString(payloadMap, "job_id", "id"))
	if jobID == "" {
		jobID = "scene_" + uuid.NewString()
	}
	jobRunID := strings.TrimSpace(firstString(payloadMap, "job_run_id", "run_id"))
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := strings.TrimSpace(firstString(payloadMap, "correlation_id"))
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}
	jobFingerprint := sceneVideoFingerprint(
		jobID,
		title,
		scriptText,
		scenesJSON,
		voiceovers,
		firstString(payloadMap, "youtube_group"),
		firstString(payloadMap, "output_path"),
		firstString(payloadMap, "audio_language_for_srt", "audio_lang"),
	)

	normalized := make(map[string]interface{}, len(payloadMap)+16)
	for k, v := range payloadMap {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["created_at"] = ensureRFC3339(firstString(payloadMap, "created_at"), now)
	normalized["updated_at"] = ensureRFC3339(firstString(payloadMap, "updated_at"), now)
	normalized["video_name"] = title
	normalized["title"] = title
	normalized["script_text"] = scriptText
	normalized["scenes"] = scenesValue
	normalized["scenes_json"] = scenesJSON
	normalized["voiceover_paths"] = voiceovers
	normalized["voiceover_path"] = voiceovers[0]
	normalized["audio_path"] = voiceovers[0]
	normalized["priority"] = ensureInt(payloadMap["priority"], 1)
	normalized["timeout_secs"] = ensureInt(payloadMap["timeout_secs"], 3600)
	normalized["scene_count"] = len(scenesValue)
	normalized["voiceover_count"] = len(voiceovers)
	normalized["submitted_via"] = "api_v1_scene_video"
	normalized["source"] = "scene_video_api"
	normalized["job_fingerprint"] = jobFingerprint
	normalized["version"] = "v1"
	normalized["parameters"] = map[string]interface{}{
		"version":         "v1",
		"job_id":          jobID,
		"job_run_id":      jobRunID,
		"run_id":          jobRunID,
		"correlation_id":  correlationID,
		"job_type":        "process_video",
		"video_name":      title,
		"script_text":     scriptText,
		"scenes_json":     scenesJSON,
		"scenes":          scenesValue,
		"voiceover_paths": voiceovers,
		"audio_path":      voiceovers[0],
		"youtube_group":   firstString(payloadMap, "youtube_group"),
		"output_path":     firstString(payloadMap, "output_path"),
		"job_fingerprint": jobFingerprint,
		"submitted_via":   "api_v1_scene_video",
		"source":          "scene_video_api",
		"scene_count":     len(scenesValue),
		"voiceover_count": len(voiceovers),
		"priority":        ensureInt(payloadMap["priority"], 1),
		"timeout_secs":    ensureInt(payloadMap["timeout_secs"], 3600),
	}

	if v := strings.TrimSpace(firstString(payloadMap, "youtube_group")); v != "" {
		normalized["youtube_group"] = v
	}
	if v := strings.TrimSpace(firstString(payloadMap, "output_video_id")); v != "" {
		normalized["output_video_id"] = v
	}
	if v := strings.TrimSpace(firstString(payloadMap, "audio_language_for_srt", "audio_lang")); v != "" {
		normalized["audio_language_for_srt"] = v
		normalized["parameters"].(map[string]interface{})["audio_language_for_srt"] = v
	}
	if v := strings.TrimSpace(firstString(payloadMap, "output_path")); v != "" {
		normalized["output_path"] = v
		normalized["parameters"].(map[string]interface{})["output_path"] = v
	}
	if v := strings.TrimSpace(firstString(payloadMap, "scene_image_paths")); v != "" {
		normalized["scene_image_paths"] = v
		normalized["parameters"].(map[string]interface{})["scene_image_paths"] = v
	}

	return normalized, nil
}

// extractSceneClipPaths estrae tutti i path/clip URL unici da un array di scene.
// Cerca in image_link, image_url, image e image_links.
func extractSceneClipPaths(scenes []map[string]interface{}) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(scenes))
	for _, scene := range scenes {
		candidates := []string{
			firstString(scene, "image_link", "image_url", "image"),
		}
		if v, ok := scene["image_links"]; ok {
			candidates = append(candidates, payload.NormalizeToStrings(v)...)
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
			result = append(result, trimmed)
		}
	}
	return result
}

// buildSceneVideoResponse costruisce una risposta HTTP standard per un job scene video.
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

// normalizeScenes estrae e normalizza le scene da un payload.
// Supporta: scenes ([]interface{} o []map[string]interface{}) e scenes_json (string).
// Restituisce le scene normalizzate e la loro rappresentazione JSON.
func normalizeScenes(payloadMap map[string]interface{}) ([]map[string]interface{}, string, error) {
	if v, ok := payloadMap["scenes"]; ok {
		switch scenes := v.(type) {
		case []interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				result = append(result, contract.NormalizeSceneEntry(m))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		case []map[string]interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				result = append(result, contract.NormalizeSceneEntry(item))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		}
	}

	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err != nil {
			return nil, "", err
		}
		for i := range scenes {
			scenes[i] = contract.NormalizeSceneEntry(scenes[i])
		}
		data, err := json.Marshal(scenes)
		if err != nil {
			return nil, "", err
		}
		return scenes, string(data), nil
	}

	return nil, "", nil
}

// normalizeVoiceoverList raccoglie e deduplica i path voiceover da un payload.
// Cerca in voiceover_paths, voiceover_path, voiceover, voiceovers, voiceovers_urls e unified_voiceover_link.
func normalizeVoiceoverList(payloadMap map[string]interface{}) []string {
	candidates := []string{
		firstString(payloadMap, "voiceover_path", "voiceover", "unified_voiceover_link"),
	}
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers_urls"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
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

func firstString(source map[string]interface{}, keys ...string) string {
	return payload.FirstString(source, keys...)
}

func ensureRFC3339(value, fallback string) string {
	return payload.EnsureRFC3339(value, fallback)
}

func ensureInt(value interface{}, fallback int) int {
	return payload.EnsureInt(value, fallback)
}

// sceneCountFromPayload conta le scene in un payload, supportando
// scenes (array) e scenes_json (string).
func sceneCountFromPayload(payloadMap map[string]interface{}) int {
	if scenes, ok := payloadMap["scenes"].([]interface{}); ok {
		return len(scenes)
	}
	if scenes, ok := payloadMap["scenes"].([]map[string]interface{}); ok {
		return len(scenes)
	}
	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err == nil {
			return len(scenes)
		}
	}
	return 0
}

// voiceoverCountFromPayload conta i voiceover in un payload.
func voiceoverCountFromPayload(payloadMap map[string]interface{}) int {
	if arr, ok := payloadMap["voiceover_paths"].([]string); ok {
		return len(arr)
	}
	if arr, ok := payloadMap["voiceover_paths"].([]interface{}); ok {
		return len(arr)
	}
	return len(normalizeVoiceoverList(payloadMap))
}

// sceneVideoFingerprint genera un fingerprint SHA-256 parziale (32 hex char) di un job
// per deduplicazione. Combina jobID, title, script, scenes, voiceovers, youtube_group,
// output_path e audio_language.
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

// isDraftScenePayload verifica se un payload è in modalità bozza (submission_mode == "draft"),
// nel qual caso viene inoltrato al master server invece di essere accodato localmente.
func isDraftScenePayload(payloadMap map[string]interface{}) bool {
	return strings.TrimSpace(firstString(payloadMap, "submission_mode")) == "draft"
}

// validationError rappresenta un errore di validazione campo-specifico.
type validationError struct {
	field   string
	message string
}

func (e *validationError) Error() string {
	return e.field + ": " + e.message
}
