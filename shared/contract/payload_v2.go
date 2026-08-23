// Package contract — payload_v2.go defines the V2 single-shape envelope
// for the scene-video (process_video) request payload.
//
// The canonical typed model remains in this file. Map/JSON conversion,
// writer validation, and legacy-compatible readers are split into focused
// same-package files without changing the public contract.
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/google/uuid"

	"velox-shared/compatibility"
	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendermanifest"
)

// ContractVersionV2 is the canonical contract_version stamped on every
// JobPayloadV2 envelope.
const ContractVersionV2 = 2

// Payload contract versions identify the shape delivered to workers.
const (
	PayloadContractVersionLegacy    = 1
	PayloadContractVersionCanonical = 2
)

// readCanonicalVoiceoverPaths delegates legacy alias handling to the shared
// compatibility registry. Keeping this boundary in the payload contract makes
// all V2 readers use the same alias lifecycle and telemetry.
func readCanonicalVoiceoverPaths(raw map[string]any) []string {
	return compatibility.ReadStringList(raw, compatibility.VoiceoverPathsKey)
}

// JobPayloadV2 is the single, canonical, top-level typed shape for any
// process_video job payload going through the enqueue boundary.
type JobPayloadV2 struct {
	// Lifecycle / canonical identity
	ContractVersion        int    `json:"contract_version"`
	PayloadContractVersion int    `json:"payload_contract_version"`
	JobID                  string `json:"job_id"`
	JobRunID               string `json:"job_run_id"`
	CorrelationID          string `json:"correlation_id"`
	JobType                string `json:"job_type"`
	TemplateID             string `json:"template_id,omitempty"`
	TemplateVersion        int    `json:"template_version,omitempty"`
	Version                string `json:"version"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`

	// Business fields. These are authoring inputs; RenderManifest is the
	// compiled authority when present.
	VideoName                string                 `json:"video_name"`
	ScriptText               string                 `json:"script_text"`
	RenderManifest           map[string]any         `json:"render_manifest,omitempty"`
	ManifestRef              map[string]any         `json:"manifest_ref,omitempty"`
	ManifestSHA256           string                 `json:"manifest_sha256,omitempty"`
	RenderPlanJSON           string                 `json:"render_plan_json,omitempty"`
	RenderPlanSHA256         string                 `json:"render_plan_sha256,omitempty"`
	CompiledRenderPlanJSON   string                 `json:"compiled_render_plan_json,omitempty"`
	CompiledRenderPlanSHA256 string                 `json:"compiled_render_plan_sha256,omitempty"`
	ScenesJSON               string                 `json:"scenes_json,omitempty"`
	Scenes                   []map[string]any       `json:"scenes,omitempty"`
	Clips                    []map[string]any       `json:"clips,omitempty"`
	CopyOnly                 bool                   `json:"copy_only"`
	Layers                   []rendermanifest.Layer `json:"layers,omitempty"`
	Items                    []map[string]any       `json:"items,omitempty"`
	VoiceoverPaths           []string               `json:"voiceover_paths,omitempty"`
	AudioLanguage            string                 `json:"audio_language_for_srt,omitempty"`
	VideoMode                string                 `json:"video_mode,omitempty"`
	Effect                   string                 `json:"effect,omitempty"`
	Orientation              string                 `json:"orientation,omitempty"`
	OutputPath               string                 `json:"output_path,omitempty"`
	DriveOutput              string                 `json:"drive_output_folder,omitempty"`
	ChannelID                string                 `json:"channel_id,omitempty"`
	OutputVideoID            string                 `json:"output_video_id,omitempty"`
	SceneImagePaths          []string               `json:"scene_image_paths,omitempty"`
	ImageSourceMap           string                 `json:"image_source_map,omitempty"`
	VideoMetadata            map[string]any         `json:"video_metadata,omitempty"`
	Canvas                   rendermanifest.Canvas  `json:"-"`

	// Numeric metadata (sent as JSON numbers)
	Priority          int     `json:"priority"`
	TimeoutSecs       int     `json:"timeout_secs"`
	SceneCount        int     `json:"scene_count,omitempty"`
	VoiceoverCount    int     `json:"voiceover_count,omitempty"`
	TotalDurationSecs float64 `json:"total_duration_secs,omitempty"`
	SceneDurationSecs float64 `json:"scene_duration_secs,omitempty"`

	// Routing / audit
	SubmittedVia   string              `json:"submitted_via,omitempty"`
	Source         string              `json:"source,omitempty"`
	JobFingerprint string              `json:"job_fingerprint,omitempty"`
	Status         InputAssemblyStatus `json:"status,omitempty"`

	// Delivery contract.
	DeliveryPlan        []deliveryplan.Entry `json:"delivery_plan,omitempty"`
	deliveryPlanPresent bool
}

// SceneVideoFingerprint computes a deterministic SHA-256 prefix over the
// identity and business fields used by the enqueue boundary.
func (p *JobPayloadV2) SceneVideoFingerprint() string {
	parts := []string{
		p.JobID,
		p.VideoName,
		p.ScriptText,
		p.ScenesJSON,
		strings.Join(p.VoiceoverPaths, "|"),
		p.OutputPath,
		p.AudioLanguage,
	}
	return sha256Sum(parts)
}

// SetIdentity applies (jobID, jobRunID, correlationID) if they are non-empty.
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

// sha256Sum returns the full stable hexadecimal SHA-256 used by the legacy
// implementation's fingerprint helper.
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
