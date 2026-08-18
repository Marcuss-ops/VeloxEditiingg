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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-shared/compatibility"
	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendermanifest"
	"velox-shared/payload"
)

// ContractVersionV2 is the canonical contract_version stamped on every
// JobPayloadV2 envelope. Readers must accept this version (and the
// no-version legacy rows) and may reject other versions.
const ContractVersionV2 = 2

// Payload contract versions identify the shape delivered to workers.
// Version 1 is the compatibility projection; version 2 is the canonical
// flat payload. The master persists only the canonical version and projects
// version 1 just before offering a task to an old worker.
const (
	PayloadContractVersionLegacy    = 1
	PayloadContractVersionCanonical = 2
)

// JobPayloadV2 is the single, canonical, top-level typed shape for any
// process_video job payload going through the enqueue boundary.
//
// JSON field order is deliberately stable — IDs and lifecycle fields come
// first, then business fields, then aggregate counts and routing metadata
// — so MarshalJSON produces diffable blobs across writers.
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

	// Business fields
	//
	// Authoring-input vs compiled-authority boundary (audit 2026-08): the
	// map/slice fields below (Output, Scenes, Layers, Items, AudioTracks,
	// VideoMetadata) are AUTHORING INPUT — the uncompiled producer envelope
	// consumed by the compilers (shared/contract/rendercompiler and
	// DataServer/internal/renderplan) when `render_manifest` is absent.
	// They are NOT duplicates of the render manifest: render_manifest is
	// the COMPILED authority (rendermanifest.Manifest), and when it is
	// present the compilers ignore the authoring fields and read only the
	// manifest. There is no top-level `assets` field (removed); assets are
	// derived from scenes/voiceovers or carried only inside render_manifest.
	//
	// Do NOT introduce a second typed struct for these authoring fields
	// (e.g. a payload.Layer alongside rendermanifest.Layer): that would
	// recreate the two-SSOT shape the single-shape envelope removed. The
	// only typed authority for the compiled form is rendermanifest.Manifest.
	VideoName        string         `json:"video_name"`
	ScriptText       string         `json:"script_text"`
	Spec             map[string]any `json:"spec,omitempty"`
	Output           map[string]any `json:"output,omitempty"`          // authoring input (canvas/output dims), NOT rendermanifest.Output
	RenderManifest   map[string]any `json:"render_manifest,omitempty"` // compiled authority (rendermanifest.Manifest)
	ManifestRef      map[string]any `json:"manifest_ref,omitempty"`
	ManifestSHA256   string         `json:"manifest_sha256,omitempty"`
	RenderPlanJSON   string         `json:"render_plan_json,omitempty"`
	RenderPlanSHA256 string         `json:"render_plan_sha256,omitempty"`
	// CompiledRenderPlanJSON/SHA carry a producer-owned V2 execution plan.
	// They are deliberately separate fields from the legacy render-plan
	// values so the enqueue normalizer can preserve the exact V2 bytes and
	// hash without compiling or re-serializing them.
	CompiledRenderPlanJSON   string           `json:"compiled_render_plan_json,omitempty"`
	CompiledRenderPlanSHA256 string           `json:"compiled_render_plan_sha256,omitempty"`
	ScenesJSON               string           `json:"scenes_json,omitempty"`
	Scenes                   []map[string]any `json:"scenes,omitempty"`       // authoring input (compiled into manifest tracks)
	Layers                   []map[string]any `json:"layers,omitempty"`       // authoring input (compiled into manifest layers)
	Items                    []map[string]any `json:"items,omitempty"`        // authoring input (compiled clip/stock timeline)
	AudioTracks              []map[string]any `json:"audio_tracks,omitempty"` // authoring input (global audio layers; NOT manifest tracks)
	VoiceoverPaths           []string         `json:"voiceover_paths,omitempty"`
	AudioLanguage            string           `json:"audio_language_for_srt,omitempty"`
	VideoMode                string           `json:"video_mode,omitempty"`
	Effect                   string           `json:"effect,omitempty"`
	Orientation              string           `json:"orientation,omitempty"`
	OutputPath               string           `json:"output_path,omitempty"`
	DriveOutput              string           `json:"drive_output_folder,omitempty"`
	ChannelID                string           `json:"channel_id,omitempty"`
	OutputVideoID            string           `json:"output_video_id,omitempty"`
	SceneImagePaths          []string         `json:"scene_image_paths,omitempty"`
	ImageSourceMap           string           `json:"image_source_map,omitempty"`
	VideoMetadata            map[string]any   `json:"video_metadata,omitempty"`

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
	// Status is the producer-side input assembly state. It is deliberately
	// not JobStatus: "completed" means the handoff envelope is assembled,
	// not that rendering, delivery, or publication finished.
	Status InputAssemblyStatus `json:"status,omitempty"`

	// Delivery contract. The boundary parser accepts legacy wire variants,
	// but the canonical payload stores one typed representation thereafter.
	DeliveryPlan []deliveryplan.Entry `json:"delivery_plan,omitempty"`
	// deliveryPlanPresent preserves an explicit empty delivery_plan for
	// render-only jobs without exposing another wire field.
	deliveryPlanPresent bool
}

// NewJobPayloadV2 reads a raw map (typically from JSON deserialization at
// the HTTP/service edge) and returns a populated JobPayloadV2. It preserves
// unknown status values for compatibility reads; canonical writers should
// use NewJobPayloadV2Checked instead.
//
// It enforces
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
		ContractVersion:        ContractVersionV2,
		PayloadContractVersion: PayloadContractVersionCanonical,
		JobID:                  payload.FirstString(raw, "job_id", "script_id"),
		JobRunID:               payload.FirstString(raw, "job_run_id", "run_id"),
		CorrelationID:          payload.FirstString(raw, "correlation_id"),
		JobType:                payload.FirstString(raw, "job_type"),
		TemplateID:             payload.FirstString(raw, "template_id"),
		TemplateVersion:        payload.EnsureInt(raw["template_version"], 0),
		Version:                "v2",
		CreatedAt:              payload.EnsureRFC3339(payload.FirstString(raw, "created_at"), now),
		UpdatedAt:              payload.EnsureRFC3339(payload.FirstString(raw, "updated_at"), now),
		VideoName:              payload.FirstString(raw, "video_name", "title", "project_name"),
		ScriptText:             payload.FirstString(raw, "script_text", "script", "source_text"),
		ScenesJSON:             payload.FirstString(raw, "scenes_json"),
		VoiceoverPaths:         append([]string{}, compatibility.ReadStringList(raw, compatibility.VoiceoverPathsKey)...),
		AudioLanguage:          payload.FirstString(raw, "audio_language_for_srt", "audio_lang", "language"),
		VideoMode:              payload.FirstString(raw, "video_mode"),
		Effect:                 payload.FirstString(raw, "effect"),
		Orientation:            payload.FirstString(raw, "orientation"),
		OutputPath:             payload.FirstString(raw, "output_path"),
		DriveOutput:            payload.FirstString(raw, "drive_output_folder", "output_directory"),
		ChannelID:              payload.FirstString(raw, "channel_id"),
		SceneImagePaths:        append([]string{}, payload.NormalizeStringList(raw, "scene_image_paths")...),
		Priority:               payload.EnsureInt(raw["priority"], 1),
		TimeoutSecs:            payload.EnsureInt(raw["timeout_secs"], 3600),
		SubmittedVia:           payload.FirstString(raw, "submitted_via"),
		Source:                 payload.FirstString(raw, "source"),
		Status:                 parseInputAssemblyOrLegacy(raw["status"]),
	}
	if value, ok := raw["delivery_plan"]; ok && value != nil {
		p.deliveryPlanPresent = true
	}
	if deliveryPlanInputPresent(raw) {
		if entries, err := deliveryplan.Parse(raw); err == nil {
			p.DeliveryPlan = entries
		} else if isRenderOnlyEmptyDeliveryPlan(raw) {
			p.DeliveryPlan = []deliveryplan.Entry{}
		}
	}
	if p.Status == "" {
		p.Status = InputAssemblyPending
	}
	if p.JobType == "" {
		p.JobType = "process_video"
	}
	if spec, ok := raw["spec"].(map[string]any); ok {
		p.Spec = cloneObject(spec)
	}
	if output, ok := raw["output"].(map[string]any); ok {
		p.Output = cloneObject(output)
	}
	if metadata, ok := raw["video_metadata"].(map[string]any); ok {
		p.VideoMetadata = cloneObject(metadata)
	}
	if manifest, ok := raw["render_manifest"].(map[string]any); ok {
		p.RenderManifest = cloneObject(manifest)
	}
	if manifestRef, ok := raw["manifest_ref"].(map[string]any); ok {
		p.ManifestRef = cloneObject(manifestRef)
	}
	p.ManifestSHA256 = payload.FirstString(raw, "manifest_sha256")
	p.RenderPlanJSON = payload.FirstString(raw, "render_plan_json")
	p.RenderPlanSHA256 = payload.FirstString(raw, "render_plan_sha256")
	p.CompiledRenderPlanJSON = payload.FirstString(raw, PayloadKeyCompiledRenderPlanJSON)
	p.CompiledRenderPlanSHA256 = payload.FirstString(raw, PayloadKeyCompiledRenderPlanSHA)
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
	if layersVal, ok := raw["layers"]; ok {
		p.Layers = normalizeObjectList(layersVal)
	}
	if itemsVal, ok := raw["items"]; ok {
		p.Items = normalizeObjectList(itemsVal)
	}
	if audioTracksVal, ok := raw["audio_tracks"]; ok {
		p.AudioTracks = normalizeObjectList(audioTracksVal)
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

// NewJobPayloadV2Checked constructs a canonical writer payload and rejects
// lifecycle or unknown values in the producer-side status field before they
// can be persisted or dispatched. Missing status defaults to input assembly
// pending, preserving the historical wire value PENDING.
func NewJobPayloadV2Checked(raw map[string]any) (*JobPayloadV2, error) {
	if raw != nil {
		if value, present := raw["status"]; present && value != nil {
			rawStatus, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("contract: payload status must be an input assembly status")
			}
			if strings.TrimSpace(rawStatus) == "COMPLETED" {
				return nil, fmt.Errorf("contract: payload status %q is ambiguous; use lowercase input-assembly value %q", rawStatus, InputAssemblyCompleted)
			}
			status, ok := ParseInputAssemblyStatus(rawStatus)
			if !ok {
				return nil, fmt.Errorf("contract: payload status %q is not an input assembly status", rawStatus)
			}
			copyPayload := make(map[string]any, len(raw)+1)
			for key, item := range raw {
				copyPayload[key] = item
			}
			copyPayload["status"] = string(status)
			raw = copyPayload
		}
	}
	payload := NewJobPayloadV2(raw)
	if deliveryPlanInputPresent(raw) && !isRenderOnlyEmptyDeliveryPlan(raw) {
		entries, err := deliveryplan.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("contract: delivery_plan: %w", err)
		}
		payload.DeliveryPlan = entries
	}
	if _, err := payload.TypedRenderManifest(); err != nil {
		return nil, err
	}
	return payload, nil
}

// TypedRenderManifest is the strict render-manifest boundary for canonical
// writers and the renderer compiler. RenderManifest remains a map only for
// compatibility readers that must inspect legacy SQLite rows; once a payload
// crosses NewJobPayloadV2Checked, this method is the only valid business
// representation and delegates validation to rendermanifest.Parse.
func (p *JobPayloadV2) TypedRenderManifest() (*rendermanifest.Manifest, error) {
	if p == nil || len(p.RenderManifest) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(p.RenderManifest)
	if err != nil {
		return nil, fmt.Errorf("contract: render_manifest encode: %w", err)
	}
	manifest, err := rendermanifest.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("contract: render_manifest: %w", err)
	}
	return manifest, nil
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
		"contract_version":         p.ContractVersion,
		"payload_contract_version": p.PayloadContractVersion,
		"job_id":                   p.JobID,
		"job_run_id":               p.JobRunID,
		"correlation_id":           p.CorrelationID,
		"job_type":                 p.JobType,
		"version":                  p.Version,
		"created_at":               p.CreatedAt,
		"updated_at":               p.UpdatedAt,
		"video_name":               p.VideoName,
		"script_text":              p.ScriptText,
		"priority":                 p.Priority,
		"timeout_secs":             p.TimeoutSecs,
	}
	if p.ScenesJSON != "" {
		out["scenes_json"] = p.ScenesJSON
	}
	if len(p.Spec) > 0 {
		out["spec"] = cloneObject(p.Spec)
	}
	if len(p.Output) > 0 {
		out["output"] = cloneObject(p.Output)
	}
	if p.TemplateID != "" {
		out["template_id"] = p.TemplateID
	}
	if p.TemplateVersion > 0 {
		out["template_version"] = p.TemplateVersion
	}
	if len(p.RenderManifest) > 0 {
		out["render_manifest"] = cloneObject(p.RenderManifest)
	}
	if len(p.ManifestRef) > 0 {
		out["manifest_ref"] = cloneObject(p.ManifestRef)
	}
	if p.ManifestSHA256 != "" {
		out["manifest_sha256"] = p.ManifestSHA256
	}
	if p.RenderPlanJSON != "" {
		out["render_plan_json"] = p.RenderPlanJSON
	}
	if p.RenderPlanSHA256 != "" {
		out["render_plan_sha256"] = p.RenderPlanSHA256
	}
	if p.CompiledRenderPlanJSON != "" {
		out[PayloadKeyCompiledRenderPlanJSON] = p.CompiledRenderPlanJSON
	}
	if p.CompiledRenderPlanSHA256 != "" {
		out[PayloadKeyCompiledRenderPlanSHA] = p.CompiledRenderPlanSHA256
	}
	if len(p.Scenes) > 0 {
		out["scenes"] = p.Scenes
	}
	if len(p.Layers) > 0 {
		out["layers"] = p.Layers
	}
	if len(p.Items) > 0 {
		out["items"] = p.Items
	}
	if len(p.AudioTracks) > 0 {
		out["audio_tracks"] = p.AudioTracks
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
	if p.Effect != "" {
		out["effect"] = p.Effect
	}
	if p.Orientation != "" {
		out["orientation"] = p.Orientation
	}
	if p.OutputPath != "" {
		out["output_path"] = p.OutputPath
	}
	if p.DriveOutput != "" {
		out["drive_output_folder"] = p.DriveOutput
	}
	if p.ChannelID != "" {
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
	if len(p.VideoMetadata) > 0 {
		out["video_metadata"] = cloneObject(p.VideoMetadata)
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
		if !p.Status.Valid() {
			return nil, fmt.Errorf("invalid input assembly status %q", p.Status)
		}
		out["status"] = p.Status.WireValue()
	}
	if len(p.DeliveryPlan) > 0 || p.deliveryPlanPresent {
		entries := p.DeliveryPlan
		if entries == nil {
			entries = []deliveryplan.Entry{}
		}
		out["delivery_plan"] = deliveryplan.EntriesToMaps(entries)
	}
	return out, nil
}

// deliveryPlanInputPresent distinguishes a render-only payload from a
// delivery payload that must cross the strict delivery-plan boundary.
func deliveryPlanInputPresent(raw map[string]any) bool {
	if raw == nil {
		return false
	}
	for _, key := range []string{
		"delivery_plan",
		"delivery_destination_ids",
		"destination_ids",
		"delivery_destination_id",
		"destination_id",
	} {
		if value, ok := raw[key]; ok && value != nil {
			return true
		}
	}
	if nested, ok := raw["payload"].(map[string]any); ok {
		return deliveryPlanInputPresent(nested)
	}
	return false
}

func isRenderOnlyEmptyDeliveryPlan(raw map[string]any) bool {
	if raw == nil || raw["render_only"] != true {
		return false
	}
	value, ok := raw["delivery_plan"]
	if !ok || value == nil {
		return false
	}
	switch plan := value.(type) {
	case []any:
		return len(plan) == 0
	case []map[string]any:
		return len(plan) == 0
	case []deliveryplan.Entry:
		return len(plan) == 0
	default:
		return false
	}
}

func parseInputAssemblyOrLegacy(value any) InputAssemblyStatus {
	raw, ok := value.(string)
	if !ok {
		return ""
	}
	status, ok := ParseInputAssemblyStatus(raw)
	if !ok {
		// Preserve an unknown legacy value for a compatibility read; the
		// typed accessor intentionally rejects it until a producer-specific
		// parser validates the domain.
		return InputAssemblyStatus(strings.TrimSpace(raw))
	}
	return status
}

func cloneObject(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func normalizeObjectList(value any) []map[string]any {
	switch items := value.(type) {
	case []map[string]any:
		out := make([]map[string]any, 0, len(items))
		for _, item := range items {
			out = append(out, cloneObject(item))
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(items))
		for _, item := range items {
			if object, ok := item.(map[string]any); ok {
				out = append(out, cloneObject(object))
			}
		}
		return out
	default:
		return nil
	}
}

// UnmarshalJSON keeps direct encoding/json readers compatible with legacy
// rows whose overloaded status contains a lifecycle value. Canonical writes
// still go through NewJobPayloadV2Checked and ToMap.
func (p *JobPayloadV2) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw == nil {
		*p = JobPayloadV2{}
		return nil
	}
	*p = *NewJobPayloadV2(raw)
	return nil
}

// JobPayloadV2FromJSON parses a JSON byte slice back into a typed struct.
// It intentionally reads through a raw map so legacy rows with an overloaded
// lifecycle value in `status` remain readable. Canonical writers must use
// NewJobPayloadV2Checked and ToMap, which reject those values before storage
// or dispatch.
func JobPayloadV2FromJSON(data []byte) (*JobPayloadV2, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, err
	}
	return NewJobPayloadV2(raw), nil
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
