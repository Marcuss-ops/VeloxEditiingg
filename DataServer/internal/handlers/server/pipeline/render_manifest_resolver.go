package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const maxRenderManifestBytes int64 = 4 << 20

// ResolveRenderManifestRef downloads, verifies, validates, and substitutes a
// velox.render-manifest.v1 document into a SubmitJobRequest. The returned
// request is the canonical inline form consumed by the existing SubmitJob
// validation, SSRF, quota, resolver, and enqueue path.
func (h *Handlers) ResolveRenderManifestRef(ctx context.Context, req SubmitJobRequest) (SubmitJobRequest, *SubmitJobValidationError) {
	if req.ManifestRef == nil {
		return req, nil
	}
	body, vErr := h.fetchRenderManifest(ctx, req.ManifestRef.URL)
	if vErr != nil {
		return req, vErr
	}
	rawHash := sha256.Sum256(body)
	rawHex := hex.EncodeToString(rawHash[:])
	if rawHex != req.ManifestRef.SHA256 {
		return req, manifestValidationError(gin.H{
			"path":     "manifest_ref.sha256",
			"issue":    "mismatch",
			"observed": rawHex,
			"expected": req.ManifestRef.SHA256,
		})
	}

	var manifest map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&manifest); err != nil {
		return req, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "invalid_json",
		})
	}
	var extra interface{}
	if err := dec.Decode(&extra); err != io.EOF {
		return req, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "invalid_json",
		})
	}
	resolved, details := renderManifestToSubmitRequest(req, manifest)
	if len(details) > 0 {
		return req, manifestValidationError(details...)
	}
	resolved.ResolvedManifest = cloneJSONMap(manifest)
	resolved.ResolvedManifestRef = map[string]interface{}{
		"schema_version": req.ManifestRef.SchemaVersion,
		"url":            strings.TrimSpace(req.ManifestRef.URL),
		"sha256":         req.ManifestRef.SHA256,
	}
	resolved.ResolvedManifestSHA256 = req.ManifestRef.SHA256
	resolved.ManifestRef = nil
	return resolved, nil
}

func (h *Handlers) fetchRenderManifest(ctx context.Context, rawURL string) ([]byte, *SubmitJobValidationError) {
	rawURL = strings.TrimSpace(rawURL)
	if strings.HasPrefix(strings.ToLower(rawURL), "velox-asset://") {
		return nil, manifestValidationError(gin.H{
			"path":     "manifest_ref.url",
			"issue":    "unsupported_scheme",
			"observed": rawURL,
			"allowed":  []string{"https://", "http://"},
		})
	}
	if err := ValidateExternalURL(rawURL, h.configuredAllowedDomains(), h.configuredAllowLoopbackHTTP()); err != nil {
		if se, ok := err.(*SSRFValidationError); ok {
			return nil, &SubmitJobValidationError{
				Code:    "ssrf_rejected",
				Reason:  se.Reason,
				Message: "manifest_ref.url failed the egress policy",
				Details: []gin.H{{
					"path":   "manifest_ref.url",
					"url":    se.URL,
					"reason": se.Reason,
				}},
			}
		}
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return nil, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "malformed",
		})
	}
	client := &http.Client{Timeout: 30 * time.Second}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "malformed",
		})
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "fetch_failed",
		})
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, manifestValidationError(gin.H{
			"path":     "manifest_ref.url",
			"issue":    "fetch_status",
			"observed": resp.StatusCode,
		})
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRenderManifestBytes+1))
	if err != nil {
		return nil, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "fetch_failed",
		})
	}
	if int64(len(body)) > maxRenderManifestBytes {
		return nil, manifestValidationError(gin.H{
			"path":     "manifest_ref.url",
			"issue":    "max_bytes",
			"max":      maxRenderManifestBytes,
			"observed": len(body),
		})
	}
	return body, nil
}

func (h *Handlers) configuredAllowedDomains() []string {
	if h == nil || h.cfg == nil {
		return nil
	}
	return h.cfg.AllowedExternalDomains
}

func (h *Handlers) configuredAllowLoopbackHTTP() bool {
	return h != nil && h.cfg != nil && h.cfg.Runtime.AllowLoopbackAdminAuthDev
}

func renderManifestToSubmitRequest(base SubmitJobRequest, manifest map[string]interface{}) (SubmitJobRequest, []gin.H) {
	var details []gin.H
	requiredTop := []string{"schema_version", "manifest_id", "created_at", "source", "video", "script", "scenes", "delivery_plan", "integrity"}
	allowedOptional := []string{"audio_tracks"}
	for _, key := range requiredTop {
		if _, ok := manifest[key]; !ok {
			details = append(details, gin.H{"path": key, "issue": "missing"})
		}
	}
	for _, key := range sortedMapKeys(manifest) {
		if !containsString(requiredTop, key) && !containsString(allowedOptional, key) {
			details = append(details, gin.H{"path": key, "issue": "unexpected"})
		}
	}
	if sv := stringField(manifest, "schema_version"); sv != "velox.render-manifest.v1" {
		details = append(details, gin.H{
			"path":     "schema_version",
			"issue":    "unsupported_value",
			"observed": sv,
			"allowed":  []string{"velox.render-manifest.v1"},
		})
	}
	if created := stringField(manifest, "created_at"); created != "" {
		if _, err := time.Parse(time.RFC3339, created); err != nil {
			details = append(details, gin.H{"path": "created_at", "issue": "malformed"})
		}
	}

	video := objectField(manifest, "video")
	script := objectField(manifest, "script")
	integrity := objectField(manifest, "integrity")
	if video == nil {
		details = append(details, gin.H{"path": "video", "issue": "type"})
	}
	if script == nil {
		details = append(details, gin.H{"path": "script", "issue": "type"})
	}
	if integrity == nil {
		details = append(details, gin.H{"path": "integrity", "issue": "type"})
	}

	if integrity != nil {
		if alg := stringField(integrity, "algorithm"); alg != "sha256" {
			details = append(details, gin.H{"path": "integrity.algorithm", "issue": "unsupported_value", "observed": alg})
		}
		if got := stringField(integrity, "manifest_sha256"); !manifestRefSHA256Regexp.MatchString(got) {
			details = append(details, gin.H{"path": "integrity.manifest_sha256", "issue": "malformed"})
		} else if expected, err := canonicalManifestIntegritySHA256(manifest); err != nil {
			details = append(details, gin.H{"path": "integrity.manifest_sha256", "issue": "hash_failed"})
		} else if got != expected {
			details = append(details, gin.H{
				"path":     "integrity.manifest_sha256",
				"issue":    "mismatch",
				"observed": got,
				"expected": expected,
			})
		}
	}

	scenes, sceneDetails := manifestScenesToSubmitScenes(manifest["scenes"])
	details = append(details, sceneDetails...)
	plan, planDetails := manifestDeliveryPlanToSubmit(manifest["delivery_plan"])
	details = append(details, planDetails...)
	audioTracks, audioDetails := manifestAudioTracksToSubmit(manifest["audio_tracks"])
	details = append(details, audioDetails...)

	if integrity != nil {
		if got := int64Field(integrity, "scene_count"); got != int64(len(scenes)) {
			details = append(details, gin.H{
				"path":     "integrity.scene_count",
				"issue":    "mismatch",
				"observed": got,
				"expected": len(scenes),
			})
		}
		var total int64
		for _, s := range scenes {
			total += int64(s.DurationSeconds * 1000)
		}
		if got := int64Field(integrity, "total_duration_ms"); got != total {
			details = append(details, gin.H{
				"path":     "integrity.total_duration_ms",
				"issue":    "mismatch",
				"observed": got,
				"expected": total,
			})
		}
	}

	base.VideoName = stringField(video, "name")
	base.ScriptText = stringField(script, "text")
	base.Scenes = scenes
	base.DeliveryPlan = plan
	base.AudioTracks = audioTracks
	base.VoiceoverPaths = nil
	base.SubtitleTracks = nil
	base.Layers = nil
	return base, details
}

func manifestScenesToSubmitScenes(raw interface{}) ([]SubmitScene, []gin.H) {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, []gin.H{{"path": "scenes", "issue": "empty"}}
	}
	out := make([]SubmitScene, 0, len(arr))
	var details []gin.H
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d", i), "issue": "type"})
			continue
		}
		durationMS := int64Field(m, "duration_ms")
		scene := SubmitScene{
			SceneID:         stringField(m, "scene_id"),
			Index:           int64Field(m, "index"),
			Kind:            stringField(m, "kind"),
			Text:            stringField(m, "text"),
			DurationSeconds: float64(durationMS) / 1000.0,
		}
		if scene.SceneID == "" {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d.scene_id", i), "issue": "empty"})
		}
		if scene.Kind == "" {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d.kind", i), "issue": "empty"})
		}
		if durationMS < 0 {
			details = append(details, gin.H{"path": fmt.Sprintf("scenes.%d.duration_ms", i), "issue": "out_of_range"})
		}
		if clip := objectField(m, "clip"); clip != nil {
			scene.Clip = &SubmitClip{
				AssetID:     stringField(clip, "asset_id"),
				DriveFileID: stringField(clip, "drive_file_id"),
				URL:         stringField(clip, "url"),
				SHA256:      stringField(clip, "sha256"),
				StartMS:     int64Field(clip, "start_ms"),
				EndMS:       int64Field(clip, "end_ms"),
				DurationMS:  int64Field(clip, "duration_ms"),
			}
		}
		if vo := objectField(m, "voiceover"); vo != nil {
			scene.Voiceover = &SubmitVoiceover{
				AssetID:     stringField(vo, "asset_id"),
				DriveFileID: stringField(vo, "drive_file_id"),
				URL:         stringField(vo, "url"),
				SHA256:      stringField(vo, "sha256"),
				DurationMS:  int64Field(vo, "duration_ms"),
				Language:    stringField(vo, "language"),
			}
		}
		if sub := objectField(m, "subtitles"); sub != nil {
			scene.Subtitles = &SubmitSubtitles{
				AssetID:  stringField(sub, "asset_id"),
				Format:   stringField(sub, "format"),
				URL:      stringField(sub, "url"),
				SHA256:   stringField(sub, "sha256"),
				Language: stringField(sub, "language"),
			}
		}
		out = append(out, scene)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, details
}

func manifestDeliveryPlanToSubmit(raw interface{}) ([]SubmitDeliveryPlanEntry, []gin.H) {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, []gin.H{{"path": "delivery_plan", "issue": "empty"}}
	}
	out := make([]SubmitDeliveryPlanEntry, 0, len(arr))
	var details []gin.H
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			details = append(details, gin.H{"path": fmt.Sprintf("delivery_plan.%d", i), "issue": "type"})
			continue
		}
		entry := SubmitDeliveryPlanEntry{
			DestinationID: stringField(m, "destination_id"),
			Priority:      int(int64Field(m, "priority")),
			Metadata:      objectField(m, "metadata"),
		}
		if _, ok := m["retry_budget"]; ok {
			rb := int(int64Field(m, "retry_budget"))
			entry.RetryBudget = &rb
		}
		out = append(out, entry)
	}
	return out, details
}

// manifestAudioTracksToSubmit parses the optional top-level audio_tracks array
// from a velox.render-manifest.v1 document into []SubmitAudioTrack. An absent
// or null audio_tracks key returns an empty slice with no validation errors.
func manifestAudioTracksToSubmit(raw interface{}) ([]SubmitAudioTrack, []gin.H) {
	if raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, []gin.H{{"path": "audio_tracks", "issue": "type"}}
	}
	if len(arr) == 0 {
		return nil, nil
	}
	out := make([]SubmitAudioTrack, 0, len(arr))
	var details []gin.H
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			details = append(details, gin.H{"path": fmt.Sprintf("audio_tracks.%d", i), "issue": "type"})
			continue
		}
		track := SubmitAudioTrack{
			AssetID:         stringField(m, "asset_id"),
			SourceURL:       stringField(m, "source_url"),
			Role:            stringField(m, "role"),
			Volume:          float64Field(m, "volume"),
			StartTimeOffset: float64Field(m, "start_time_offset"),
			DurationSeconds: float64Field(m, "duration_seconds"),
		}
		// At least one of asset_id or source_url must be present.
		if track.AssetID == "" && track.SourceURL == "" {
			details = append(details, gin.H{
				"path":  fmt.Sprintf("audio_tracks.%d", i),
				"issue": "missing_source",
			})
		}
		out = append(out, track)
	}
	return out, details
}

func canonicalManifestIntegritySHA256(manifest map[string]interface{}) (string, error) {
	body := cloneJSONMap(manifest)
	delete(body, "integrity")
	canonical, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func manifestValidationError(details ...gin.H) *SubmitJobValidationError {
	return &SubmitJobValidationError{
		Code:    "invalid_payload",
		Reason:  "manifest_ref_invalid",
		Message: fmt.Sprintf("manifest_ref has %d validation failure(s) (see details)", len(details)),
		Details: details,
	}
}

func objectField(m map[string]interface{}, key string) map[string]interface{} {
	if m == nil {
		return nil
	}
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

func stringField(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	switch v := m[key].(type) {
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func int64Field(m map[string]interface{}, key string) int64 {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		i, _ := v.Int64()
		return i
	default:
		return 0
	}
}

// float64Field extracts a float64 value from a map, handling json.Number.
func float64Field(m map[string]interface{}, key string) float64 {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	default:
		return 0
	}
}

func sortedMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func cloneJSONMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = cloneJSONValue(v)
	}
	return out
}

func cloneJSONValue(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return cloneJSONMap(x)
	case []interface{}:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = cloneJSONValue(x[i])
		}
		return out
	default:
		return x
	}
}
