package master

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
)

const placeholderScriptPrefix1 = "[SCRIPT WILL BE GENERATED"
const placeholderScriptPrefix2 = "SCRIPT WILL BE GENERATED"
const placeholderScriptPrefix3 = "[VOICEOVER ONLY"

// CreateMaster accepts POST /api/video/create-master: multi-title, validation, optional proxy, enqueue to SQLite-backed queue.
func CreateMaster(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid JSON"})
			return
		}
		if payload == nil {
			payload = make(map[string]interface{})
		}

		// Multi-title: payload has "titles" array
		if titles, _ := payload["titles"].([]interface{}); len(titles) > 0 {
			results := make([]gin.H, 0, len(titles))
			successCount := 0
			for i, titleItem := range titles {
				idx := i + 1
				singleTitle := extractTitle(titleItem)
				if singleTitle == "" {
					results = append(results, gin.H{"index": idx, "title": "", "error": "empty title"})
					continue
				}
				singlePayload := clonePayloadWithoutTitles(payload)
				singlePayload["video_name"] = singleTitle
				if m, ok := titleItem.(map[string]interface{}); ok {
					for k, v := range m {
						if k != "title" && k != "video_name" && singlePayload[k] == nil {
							singlePayload[k] = v
						}
					}
				}
				body, statusCode := processSingleCreateMaster(c.Request.Context(), q, singlePayload)
				if statusCode == http.StatusOK && isOK(body) {
					successCount++
					results = append(results, gin.H{"index": idx, "title": singleTitle, "result": body})
				} else {
					results = append(results, gin.H{"index": idx, "title": singleTitle, "error": body["error"], "result": body})
				}
			}
			c.JSON(http.StatusOK, gin.H{
				"ok":            true,
				"mode":          "multi_title",
				"total":         len(titles),
				"processed":     len(results),
				"success_count": successCount,
				"error_count":   len(results) - successCount,
				"results":       results,
			})
			return
		}

	// Proxy draft to remote master when configured
	if cfg.Workers.MasterServerURL != "" && isDraft(payload) {
		url := strings.TrimSuffix(cfg.Workers.MasterServerURL, "/") + "/api/video/create-master"
			proxyPayload := clonePayload(payload)
			proxyPayload["save_to_docs"] = false
			proxyPayload["gdocs"] = false
			proxyPayload["fast_test"] = false
			proxyPayload["skip_gdocs"] = true
			body, err := json.Marshal(proxyPayload)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
				return
			}
			ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Minute)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
			// Ensure ok when remote confirms enqueue (same as Python)
			if resp.StatusCode == 200 || resp.StatusCode == 201 {
				if _, hasOK := proxyRes["ok"]; !hasOK && hasEnqueueConfirmation(proxyRes) {
					proxyRes["ok"] = true
				}
			}
			c.JSON(resp.StatusCode, proxyRes)
			return
		}

		// Single title
		body, statusCode := processSingleCreateMaster(c.Request.Context(), q, payload)
		c.JSON(statusCode, body)
	}
}

func processSingleCreateMaster(ctx context.Context, q *queue.FileQueue, payload map[string]interface{}) (gin.H, int) {
	clipCount := countClips(payload)
	voiceoverCount := countVoiceovers(payload)
	hasScript := hasScript(payload)

	if clipCount <= 0 {
		return gin.H{
			"ok":      false,
			"error":   "Slot non valido: serve almeno 1 clip",
			"details": gin.H{"clips": clipCount, "voiceovers": voiceoverCount},
			"hint":    "Seleziona almeno 1 clip (Start/Middle/End o Stock timestamp).",
		}, http.StatusBadRequest
	}
	if voiceoverCount <= 0 && !hasScript {
		return gin.H{
			"ok":      false,
			"error":   "Slot non valido: servono voiceover o uno script per generarli",
			"details": gin.H{"clips": clipCount, "voiceovers": voiceoverCount, "has_script": false},
			"hint":    "Genera voiceover o includi uno script per la generazione remota.",
		}, http.StatusBadRequest
	}

	jobID, _ := payload["job_id"].(string)
	if jobID == "" {
		jobID = genJobID()
	}
	payload["job_id"] = jobID
	payload["id"] = jobID

	if err := q.SubmitJob(ctx, jobID, payload); err != nil {
		return gin.H{"ok": false, "error": err.Error()}, http.StatusInternalServerError
	}
	return gin.H{
		"ok":                true,
		"job_id":            jobID,
		"status":            "PENDING",
		"enqueue_confirmed": true,
		"dispatch_status":   "queued_for_workers",
	}, http.StatusOK
}

func countClips(payload map[string]interface{}) int {
	keys := []string{
		"start_clips", "middle_clips", "end_clips", "stock_clips",
		"start_clips_urls", "middle_clips_urls", "end_clips_urls", "stock_clips_urls",
	}
	n := 0
	for _, k := range keys {
		n += countNonEmptyItems(payload[k])
	}
	for _, k := range []string{"stock_clips_timestamps", "stock_clips_timestamps_items", "stock_clips_timestamps_list"} {
		v := payload[k]
		if list, ok := v.([]interface{}); ok {
			for _, it := range list {
				if m, ok := it.(map[string]interface{}); ok {
					if str(m["stock_urls"]) != "" || str(m["stock_url"]) != "" {
						n++
					}
				}
			}
		}
	}
	return n
}

func countVoiceovers(payload map[string]interface{}) int {
	if items, ok := payload["voiceover_items"].([]interface{}); ok {
		n := 0
		for _, it := range items {
			if m, ok := it.(map[string]interface{}); ok {
				if str(m["url"]) != "" {
					n++
				}
			} else if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
				n++
			}
		}
		if n > 0 {
			return n
		}
	}
	return countNonEmptyItems(payload["voiceovers"]) + countNonEmptyItems(payload["voiceovers_urls"])
}

func countNonEmptyItems(v interface{}) int {
	if v == nil {
		return 0
	}
	if list, ok := v.([]interface{}); ok {
		n := 0
		for _, x := range list {
			if strings.TrimSpace(str(x)) != "" {
				n++
			}
		}
		return n
	}
	if s, ok := v.(string); ok {
		n := 0
		for _, line := range strings.Split(s, "\n") {
			if strings.TrimSpace(line) != "" {
				n++
			}
		}
		return n
	}
	return 0
}

func str(v interface{}) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func hasScript(payload map[string]interface{}) bool {
	script := str(payload["script_text"])
	if script == "" || len(script) < 10 {
		return false
	}
	up := strings.ToUpper(script)
	return !strings.Contains(up, placeholderScriptPrefix1) &&
		!strings.Contains(up, placeholderScriptPrefix2) &&
		!strings.Contains(up, placeholderScriptPrefix3)
}

func isDraft(payload map[string]interface{}) bool {
	script := str(payload["script_text"])
	up := strings.ToUpper(script)
	placeholder := script == "" || len(script) < 10 ||
		strings.Contains(up, placeholderScriptPrefix1) ||
		strings.Contains(up, placeholderScriptPrefix2) ||
		strings.Contains(up, placeholderScriptPrefix3)
	voItems, _ := payload["voiceover_items"].([]interface{})
	noVoiceovers := len(voItems) == 0
	return placeholder && noVoiceovers
}

func hasEnqueueConfirmation(m map[string]interface{}) bool {
	if str(m["job_id"]) != "" || str(m["jobId"]) != "" {
		return true
	}
	if str(m["queue_id"]) != "" || str(m["queueId"]) != "" {
		return true
	}
	status := strings.ToUpper(str(m["status"]))
	return status == "PENDING" || status == "RUNNING"
}

func isOK(m gin.H) bool {
	v, _ := m["ok"].(bool)
	return v
}

func extractTitle(item interface{}) string {
	if s, ok := item.(string); ok {
		return strings.TrimSpace(s)
	}
	if m, ok := item.(map[string]interface{}); ok {
		return str(m["title"]) + str(m["video_name"])
	}
	return strings.TrimSpace(str(item))
}

func clonePayload(p map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}

func clonePayloadWithoutTitles(p map[string]interface{}) map[string]interface{} {
	out := clonePayload(p)
	delete(out, "titles")
	return out
}

func genJobID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
