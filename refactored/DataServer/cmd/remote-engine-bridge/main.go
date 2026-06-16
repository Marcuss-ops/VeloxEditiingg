package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type bridgeJob struct {
	ID        string                 `json:"id"`
	Status    string                 `json:"status"`
	Progress  int                    `json:"progress"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
	Result    map[string]interface{} `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt string                 `json:"created_at,omitempty"`
	UpdatedAt string                 `json:"updated_at,omitempty"`
}

type bridgeState struct {
	mu   sync.Mutex
	jobs map[string]*bridgeJob
}

func newBridgeState() *bridgeState {
	return &bridgeState{jobs: make(map[string]*bridgeJob)}
}

func (s *bridgeState) createJob(payload map[string]interface{}) *bridgeJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := newBridgeID()
	now := time.Now().UTC().Format(time.RFC3339)
	job := &bridgeJob{
		ID:        id,
		Status:    "completed",
		Progress:  100,
		Payload:   payload,
		Result:    buildBridgeResult(id, payload),
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.jobs[id] = job
	return job
}

func (s *bridgeState) getJob(id string) (*bridgeJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return job, true
}

func newBridgeID() string {
	return "bridge_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + strconv.FormatInt(rand.Int63(), 16)
}

func firstString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func buildBridgeResult(jobID string, payload map[string]interface{}) map[string]interface{} {
	topic := firstString(payload, "topic", "title", "source_text")
	if topic == "" {
		topic = "bridge test"
	}
	languages := toStringSlice(payload["languages"])
	if len(languages) == 0 {
		languages = []string{firstString(payload, "language", "audio_language_for_srt")}
	}
	if len(languages) == 0 {
		languages = []string{"it", "en", "es"}
	}
	sceneCount := 3
	if n, ok := payload["scene_count"].(float64); ok && n > 0 {
		sceneCount = int(n)
	}
	if n, ok := payload["scene_count"].(int); ok && n > 0 {
		sceneCount = n
	}

	scenes := make([]map[string]interface{}, 0, sceneCount)
	for i := 0; i < sceneCount; i++ {
		scenes = append(scenes, map[string]interface{}{
			"text":             topic + " - scena " + strconv.Itoa(i+1),
			"image_link":       "https://example.com/bridge-image-" + strconv.Itoa(i+1) + ".jpg",
			"image_links":      []string{"https://example.com/bridge-image-" + strconv.Itoa(i+1) + ".jpg"},
			"duration_seconds": 5,
		})
	}

	voiceovers := make([]string, 0, len(languages))
	for _, lang := range languages {
		voiceovers = append(voiceovers, "https://example.com/bridge-voiceover-"+lang+".mp3")
	}
	if len(voiceovers) == 0 {
		voiceovers = []string{"https://example.com/bridge-voiceover-it.mp3"}
	}

	scenesJSON, _ := json.Marshal(scenes)
	return map[string]interface{}{
		"ok":              true,
		"job_id":          jobID,
		"trace_id":        jobID,
		"status":          "completed",
		"progress":        100,
		"doc_url":         "https://example.com/docs/" + jobID,
		"script_text":     "Bridge generated script for " + topic,
		"scenes_json":     string(scenesJSON),
		"scenes":          scenes,
		"voiceover_path":  voiceovers[0],
		"voiceover_paths": voiceovers,
		"voiceover_url":   voiceovers[0],
		"voiceovers":      voiceovers,
		"title":           topic,
	}
}

func toStringSlice(v interface{}) []string {
	switch vv := v.(type) {
	case []string:
		return vv
	case []interface{}:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	default:
		return nil
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	port := os.Getenv("REMOTE_ENGINE_BRIDGE_PORT")
	if strings.TrimSpace(port) == "" {
		port = "8081"
	}

	state := newBridgeState()
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":     true,
			"status": "healthy",
		})
	})

	mux.HandleFunc("/api/script/generate-with-images", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, `{"ok":false,"error":"invalid json"}`, http.StatusBadRequest)
			return
		}
		job := state.createJob(payload)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":       true,
			"job_id":   job.ID,
			"trace_id": job.ID,
			"status":   "completed",
			"progress": 100,
			"result":   job.Result,
		})
	})

	mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
		job, ok := state.getJob(id)
		if !ok {
			http.Error(w, `{"ok":false,"error":"job not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job": job,
		})
	})

	addr := ":" + port
	log.Printf("[BRIDGE] remote engine bridge listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
