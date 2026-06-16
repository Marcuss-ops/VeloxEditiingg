package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type bridgeAsset struct {
	ContentType string
	Body        []byte
}

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
	mu     sync.Mutex
	jobs   map[string]*bridgeJob
	assets map[string]bridgeAsset
}

func newBridgeState() *bridgeState {
	return &bridgeState{
		jobs:   make(map[string]*bridgeJob),
		assets: make(map[string]bridgeAsset),
	}
}

func (s *bridgeState) createJob(payload map[string]interface{}) *bridgeJob {
	id := newBridgeID()
	now := time.Now().UTC().Format(time.RFC3339)
	result := buildBridgeResult(id, payload, s)
	job := &bridgeJob{
		ID:        id,
		Status:    "completed",
		Progress:  100,
		Payload:   payload,
		Result:    result,
		CreatedAt: now,
		UpdatedAt: now,
	}

	s.mu.Lock()
	s.jobs[id] = job
	s.mu.Unlock()
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

func (s *bridgeState) putAsset(path string, contentType string, body []byte) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assets[path] = bridgeAsset{ContentType: contentType, Body: body}
	return path
}

func (s *bridgeState) getAsset(path string) (bridgeAsset, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	asset, ok := s.assets[path]
	return asset, ok
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

func buildBridgeResult(jobID string, payload map[string]interface{}, state *bridgeState) map[string]interface{} {
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

	baseURL := firstString(payload, "bridge_base_url", "public_base_url")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8081"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	scenes := make([]map[string]interface{}, 0, sceneCount)
	for i := 0; i < sceneCount; i++ {
		imagePath := fmt.Sprintf("/assets/%s/scene-%d.png", jobID, i+1)
		imageURL := baseURL + imagePath
		state.putAsset(imagePath, "image/png", makeBridgePNG(topic, i))
		scenes = append(scenes, map[string]interface{}{
			"text":             topic + " - scena " + strconv.Itoa(i+1),
			"image_link":       imageURL,
			"image_links":      []string{imageURL},
			"duration_seconds": 5,
		})
	}

	voiceovers := make([]string, 0, len(languages))
	for _, lang := range languages {
		audioPath := fmt.Sprintf("/assets/%s/voiceover-%s.wav", jobID, sanitizeAssetName(lang))
		audioURL := baseURL + audioPath
		state.putAsset(audioPath, "audio/wav", makeBridgeWAV(float64(1100+len(lang)*40), 16000, 1*time.Second))
		voiceovers = append(voiceovers, audioURL)
	}
	if len(voiceovers) == 0 {
		audioPath := fmt.Sprintf("/assets/%s/voiceover-it.wav", jobID)
		audioURL := baseURL + audioPath
		state.putAsset(audioPath, "audio/wav", makeBridgeWAV(1100, 16000, 1*time.Second))
		voiceovers = []string{audioURL}
	}

	scenesJSON, _ := json.Marshal(scenes)
	return map[string]interface{}{
		"ok":                     true,
		"job_id":                 jobID,
		"trace_id":               jobID,
		"status":                 "completed",
		"progress":               100,
		"doc_url":                baseURL + "/docs/" + jobID,
		"script_text":            "Bridge generated script for " + topic,
		"scenes_json":            string(scenesJSON),
		"scenes":                 scenes,
		"voiceover_path":         voiceovers[0],
		"voiceover_paths":        voiceovers,
		"voiceover_url":          voiceovers[0],
		"voiceovers":             voiceovers,
		"title":                  topic,
		"youtube_group":          firstString(payload, "youtube_group"),
		"audio_language_for_srt": firstString(payload, "audio_language_for_srt", "language"),
		"language":               firstString(payload, "language", "audio_language_for_srt"),
		"drive_output_folder":    firstString(payload, "drive_output_folder", "output_directory"),
		"output_path":            firstString(payload, "output_path"),
		"video_mode":             firstString(payload, "video_mode"),
	}
}

func sanitizeAssetName(input string) string {
	if strings.TrimSpace(input) == "" {
		return "it"
	}
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(input)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('_')
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "it"
	}
	return out
}

func makeBridgePNG(topic string, idx int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, 1280, 720))
	base := color.RGBA{R: uint8(32 + (idx*33)%120), G: uint8(58 + (idx*19)%120), B: uint8(88 + (idx*27)%120), A: 255}
	accent := color.RGBA{R: 240, G: 210, B: 160, A: 255}
	for y := 0; y < 720; y++ {
		for x := 0; x < 1280; x++ {
			if y > 560 {
				img.SetRGBA(x, y, accent)
				continue
			}
			img.SetRGBA(x, y, base)
		}
	}
	// Simple high-contrast stripe to make frames visually distinct.
	for y := 60; y < 120; y++ {
		for x := 80; x < 1200; x++ {
			img.SetRGBA(x, y, color.RGBA{R: 250, G: 250, B: 250, A: 255})
		}
	}
	// Encode without text to keep the dependency footprint small.
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes()
}

func makeBridgeWAV(frequencyHz float64, sampleRate int, duration time.Duration) []byte {
	samples := int(float64(sampleRate) * duration.Seconds())
	const bitsPerSample = 16
	const numChannels = 1
	byteRate := sampleRate * numChannels * bitsPerSample / 8
	blockAlign := numChannels * bitsPerSample / 8
	dataSize := samples * numChannels * bitsPerSample / 8
	riffSize := 36 + dataSize

	var buf bytes.Buffer
	writeString := func(s string) {
		_, _ = buf.WriteString(s)
	}
	writeU32 := func(v uint32) {
		_ = binary.Write(&buf, binary.LittleEndian, v)
	}
	writeU16 := func(v uint16) {
		_ = binary.Write(&buf, binary.LittleEndian, v)
	}

	writeString("RIFF")
	writeU32(uint32(riffSize))
	writeString("WAVE")
	writeString("fmt ")
	writeU32(16)
	writeU16(1)
	writeU16(numChannels)
	writeU32(uint32(sampleRate))
	writeU32(uint32(byteRate))
	writeU16(uint16(blockAlign))
	writeU16(bitsPerSample)
	writeString("data")
	writeU32(uint32(dataSize))

	// 1 second sine wave, gently faded to avoid clicks.
	for i := 0; i < samples; i++ {
		t := float64(i) / float64(sampleRate)
		envelope := 1.0
		if t < 0.02 {
			envelope = t / 0.02
		} else if remaining := duration.Seconds() - t; remaining < 0.02 {
			envelope = remaining / 0.02
		}
		if envelope < 0 {
			envelope = 0
		}
		sample := int16(0.28 * envelope * 32767 * math.Sin(2*math.Pi*frequencyHz*t))
		_ = binary.Write(&buf, binary.LittleEndian, sample)
	}
	return buf.Bytes()
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

	mux.HandleFunc("/assets/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := r.URL.Path
		asset, ok := state.getAsset(path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", asset.ContentType)
		w.Header().Set("Content-Length", strconv.Itoa(len(asset.Body)))
		_, _ = io.Copy(w, bytes.NewReader(asset.Body))
	})

	mux.HandleFunc("/docs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("bridge document placeholder: " + r.URL.Path))
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
