// Package contract — payload_v2.go defines the V2 single-shape envelope
// for the scene-video (process_video) request payload.
//
// refactor/payload-v2-single-shape: the canonical middle shape between any
// ingress point (script/generate-with-images, pipeline, calendar, smoke)
// and the storage layer. One typed struct, one marshaled form, one
// canonical map — no `parameters` mirror, no legacy `id`/`run_id`/`title`/
// `voiceover_path`/`audio_path` alias writes.
//
// Readers continue to tolerate the `parameters` sub-map and the legacy
// aliases only as a FALLBACK for legacy SQLite rows written before the
// migration. New writes go through JobPayloadV2 and produce only the
// canonical flat shape.
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-shared/payload"
)

// ContractVersionV2 is the canonical contract_version stamped on every
// JobPayloadV2 envelope. Readers must accept this version (and the
// no-version legacy rows) and may reject other versions.
const ContractVersionV2 = 2

// JobPayloadV2 is the single, canonical, top-level typed shape for any
// process_video job payload going through the enqueue boundary.
//
// JSON field order is deliberately stable — IDs and lifecycle fields come
// first, then business fields, then aggregate counts and routing metadata
// — so MarshalJSON produces diffable blobs across writers.
type JobPayloadV2 struct {
	// Lifecycle / canonical identity
	ContractVersion int    `json:"contract_version"`
	JobID           string `json:"job_id"`
	JobRunID        string `json:"job_run_id"`
	CorrelationID   string `json:"correlation_id"`
	JobType         string `json:"job_type"`
	Version         string `json:"version"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`

	// Business fields
	VideoName       string           `json:"video_name"`
	ScriptText      string           `json:"script_text"`
	ScenesJSON      string           `json:"scenes_json,omitempty"`
	Scenes          []map[string]any `json:"scenes,omitempty"`
	VoiceoverPaths  []string         `json:"voiceover_paths,omitempty"`
	AudioLanguage   string           `json:"audio_language_for_srt,omitempty"`
	VideoMode       string           `json:"video_mode,omitempty"`
	OutputPath      string           `json:"output_path,omitempty"`
	DriveOutput     string           `json:"drive_output_folder,omitempty"`
	YoutubeGroup    string           `json:"youtube_group,omitempty"`
	ChannelID       string           `json:"channel_id,omitempty"`
	OutputVideoID   string           `json:"output_video_id,omitempty"`
	SceneImagePaths []string         `json:"scene_image_paths,omitempty"`
	ImageSourceMap  string           `json:"image_source_map,omitempty"`

	// Numeric metadata (sent as JSON numbers)
	Priority          int     `json:"priority"`
	TimeoutSecs       int     `json:"timeout_secs"`
	SceneCount        int     `json:"scene_count,omitempty"`
	VoiceoverCount    int     `json:"voiceover_count,omitempty"`
	TotalDurationSecs float64 `json:"total_duration_secs,omitempty"`
	SceneDurationSecs float64 `json:"scene_duration_secs,omitempty"`

	// Routing / audit
	SubmittedVia   string `json:"submitted_via,omitempty"`
	Source         string `json:"source,omitempty"`
	JobFingerprint string `json:"job_fingerprint,omitempty"`
	Status         string `json:"status,omitempty"`
}

// NewJobPayloadV2 reads a raw map (typically from JSON deserialization at
// the HTTP/service edge) and returns a populated JobPayloadV2. It enforces
// the canonical field names and STRIPS the legacy alias keys
// (id/run_id/title/voiceover_path/audio_path) so they cannot leak into
// the canonical map produced by ToMap.
//
// The returned struct's ContractVersion is always ContractVersionV2.
// Missing fields fall back to documented V2 defaults (job_type="process_video",
// priority=1, timeout_secs=3600, status="PENDING").
func NewJobPayloadV2(raw map[string]any) *JobPayloadV2 {
	if raw == nil {
		raw = map[string]any{}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	p := &JobPayloadV2{
		ContractVersion: ContractVersionV2,
		JobID:           payload.FirstString(raw, "job_id", "script_id"),
		JobRunID:        payload.FirstString(raw, "job_run_id", "run_id"),
		CorrelationID:   payload.FirstString(raw, "correlation_id"),
		JobType:         "process_video",
		Version:         "v2",
		CreatedAt:       payload.EnsureRFC3339(payload.FirstString(raw, "created_at"), now),
		UpdatedAt:       payload.EnsureRFC3339(payload.FirstString(raw, "updated_at"), now),
		VideoName:       payload.FirstString(raw, "video_name", "title", "project_name"),
		ScriptText:      payload.FirstString(raw, "script_text", "script", "source_text"),
		ScenesJSON:      payload.FirstString(raw, "scenes_json"),
		VoiceoverPaths:  append([]string{}, payload.NormalizeStringList(raw, "voiceover_paths", "voiceover_path", "audio_path", "source_media", "source_media_url", "audio_source")...),
		AudioLanguage:   payload.FirstString(raw, "audio_language_for_srt", "audio_lang", "language"),
		VideoMode:       payload.FirstString(raw, "video_mode"),
		OutputPath:      payload.FirstString(raw, "output_path"),
		DriveOutput:     payload.FirstString(raw, "drive_output_folder", "output_directory"),
		YoutubeGroup:    payload.FirstString(raw, "youtube_group", "channel_id"),
		ChannelID:       payload.FirstString(raw, "channel_id", "youtube_group"),
		OutputVideoID:   payload.FirstString(raw, "output_video_id"),
		SceneImagePaths: append([]string{}, payload.NormalizeStringList(raw, "scene_image_paths")...),
		Priority:        payload.EnsureInt(raw["priority"], 1),
		TimeoutSecs:     payload.EnsureInt(raw["timeout_secs"], 3600),
		SubmittedVia:    payload.FirstString(raw, "submitted_via"),
		Source:          payload.FirstString(raw, "source"),
		Status:          "PENDING",
	}
	if scenesVal, ok := raw["scenes"]; ok {
		switch s := scenesVal.(type) {
		case []map[string]any:
			p.Scenes = append([]map[string]any{}, s...)
		case []any:
			out := make([]map[string]any, 0, len(s))
			for _, item := range s {
				if m, ok := item.(map[string]any); ok {
					out = append(out, m)
				}
			}
			p.Scenes = out
		}
	}
	if p.JobID == "" {
		p.JobID = "scriptimg_" + uuid.NewString()
	}
	if p.JobRunID == "" {
		p.JobRunID = "run_" + uuid.NewString()
	}
	if p.CorrelationID == "" {
		p.CorrelationID = "corr_" + uuid.NewString()
	}
	if p.SceneCount == 0 && len(p.Scenes) > 0 {
		p.SceneCount = len(p.Scenes)
	}
	if p.VoiceoverCount == 0 && len(p.VoiceoverPaths) > 0 {
		p.VoiceoverCount = len(p.VoiceoverPaths)
	}
	return p
}

// ToMap returns the canonical map representation of the payload for
// downstream consumers (HTTP responses, asset rewrite passes, task-spec
// embedding). Constructed directly from the typed struct fields so that
// Go types are preserved (`[]string` stays `[]string`, `[]map[string]any`
// stays `[]map[string]any`) — this matches what the original manual
// `normalized` map writers used to produce, while still respecting the
// `omitempty` discipline on every optional field.
//
// Guaranteed properties:
//   - NO `parameters` sub-map mirror
//   - NO legacy alias keys (id/run_id/title/voiceover_path/audio_path)
//   - Field presence matches MarshalJSON with `omitempty`
//   - Slice / array element types preserved (no JSON-roundtrip erasure)
//
// NOTE: keep this method in sync with the struct's json tags if a new
// field is added — both this projection AND the struct's MarshalJSON
// must agree on keys + omitempty semantics.
func (p *JobPayloadV2) ToMap() (map[string]any, error) {
	if p == nil {
		return map[string]any{}, nil
	}
	out := map[string]any{
		"contract_version": p.ContractVersion,
		"job_id":           p.JobID,
		"job_run_id":       p.JobRunID,
		"correlation_id":   p.CorrelationID,
		"job_type":         p.JobType,
		"version":          p.Version,
		"created_at":       p.CreatedAt,
		"updated_at":       p.UpdatedAt,
		"video_name":       p.VideoName,
		"script_text":      p.ScriptText,
		"priority":         p.Priority,
		"timeout_secs":     p.TimeoutSecs,
	}
	if p.ScenesJSON != "" {
		out["scenes_json"] = p.ScenesJSON
	}
	if len(p.Scenes) > 0 {
		out["scenes"] = p.Scenes
	}
	if len(p.VoiceoverPaths) > 0 {
		out["voiceover_paths"] = p.VoiceoverPaths
	}
	if p.AudioLanguage != "" {
		out["audio_language_for_srt"] = p.AudioLanguage
	}
	if p.VideoMode != "" {
		out["video_mode"] = p.VideoMode
	}
	if p.OutputPath != "" {
		out["output_path"] = p.OutputPath
	}
	if p.DriveOutput != "" {
		out["drive_output_folder"] = p.DriveOutput
	}
	if p.YoutubeGroup != "" {
		out["youtube_group"] = p.YoutubeGroup
	}
	if p.ChannelID != "" {
		// Always emit channel_id when set, even if equal to youtube_group.
		// The legacy writer mirrored both keys verbatim; legacy readers
		// (calendar, smoke, calendar handlers) tolerate the duplicate.
		out["channel_id"] = p.ChannelID
	}
	if p.OutputVideoID != "" {
		out["output_video_id"] = p.OutputVideoID
	}
	if len(p.SceneImagePaths) > 0 {
		out["scene_image_paths"] = p.SceneImagePaths
	}
	if p.ImageSourceMap != "" {
		out["image_source_map"] = p.ImageSourceMap
	}
	if p.SceneCount > 0 {
		out["scene_count"] = p.SceneCount
	}
	if p.VoiceoverCount > 0 {
		out["voiceover_count"] = p.VoiceoverCount
	}
	if p.TotalDurationSecs > 0 {
		out["total_duration_secs"] = p.TotalDurationSecs
	}
	if p.SceneDurationSecs > 0 {
		out["scene_duration_secs"] = p.SceneDurationSecs
	}
	if p.SubmittedVia != "" {
		out["submitted_via"] = p.SubmittedVia
	}
	if p.Source != "" {
		out["source"] = p.Source
	}
	if p.JobFingerprint != "" {
		out["job_fingerprint"] = p.JobFingerprint
	}
	if p.Status != "" {
		out["status"] = p.Status
	}
	return out, nil
}

// JobPayloadV2FromJSON parses a JSON byte slice back into a typed struct.
// This is the canonical reader; legacy maps lacking contract_version
// are returned with ContractVersion=0 (readers may treat 0 as legacy).
func JobPayloadV2FromJSON(data []byte) (*JobPayloadV2, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var p JobPayloadV2
	if err := json.Unmarshal([]byte(trimmed), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// SceneVideoFingerprint computes a deterministic SHA-256 prefix over the
// identity + business fields used by the enqueue boundary for idempotency
// pre-checks. Stable across writers because it draws only on the typed
// struct's fields.
func (p *JobPayloadV2) SceneVideoFingerprint() string {
	parts := []string{
		p.JobID,
		p.VideoName,
		p.ScriptText,
		p.ScenesJSON,
		strings.Join(p.VoiceoverPaths, "|"),
		p.YoutubeGroup,
		p.OutputPath,
		p.AudioLanguage,
	}
	h := sha256Sum(parts)
	return h
}

// SetIdentity applies (jobID, jobRunID, correlationID) if they are empty.
// Same semantics as the legacy UUID-defaulting block but expressed over
// the typed struct.
func (p *JobPayloadV2) SetIdentity(jobID, jobRunID, correlationID string) {
	if p == nil {
		return
	}
	if strings.TrimSpace(jobID) != "" {
		p.JobID = jobID
	}
	if strings.TrimSpace(jobRunID) != "" {
		p.JobRunID = jobRunID
	}
	if strings.TrimSpace(correlationID) != "" {
		p.CorrelationID = correlationID
	}
	if p.JobID == "" {
		p.JobID = "scene_" + uuid.NewString()
	}
	if p.JobRunID == "" {
		p.JobRunID = "run_" + uuid.NewString()
	}
	if p.CorrelationID == "" {
		p.CorrelationID = "corr_" + uuid.NewString()
	}
}

// ComputeJobFingerprint sets JobFingerprint from the canonical fields.
func (p *JobPayloadV2) ComputeJobFingerprint() {
	if p == nil {
		return
	}
	p.JobFingerprint = p.SceneVideoFingerprint()
}

// ============================================================================
// Internal helpers
// ============================================================================

// sha256Sum returns the first 32 hex characters of the SHA-256 over a
// sequence of parts joined by a NUL byte. Stable across writers because
// the iteration order is fixed.
func sha256Sum(parts []string) string {
	h := sha256.New()
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			h.Write([]byte(trimmed))
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
