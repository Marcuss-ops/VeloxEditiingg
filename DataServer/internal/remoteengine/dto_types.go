// Package remoteengine: typed DTO for remote pipeline results.
//
// Area 2 — The remote result must NOT be passed directly to the worker.
// It must first be converted into a typed DTO (RemotePipelineResult) so
// the contract between the remote engine and the Velox worker is explicit
// and verified at the adapter boundary, not scattered across handlers
// and the resolver as string-key lookups on a generic map.

package remoteengine

// ── Known remote statuses ────────────────────────────────────────────────────

// knownRemoteStatuses is the closed set of statuses the remote engine may
// return in the initial response and in poll responses. Any status outside
// this set is a contract violation.
var knownRemoteStatuses = map[string]bool{
	"queued":    true,
	"running":   true,
	"completed": true,
	"failed":    true,
	"cancelled": true,
}

// ── Initial response validation ──────────────────────────────────────────────

// InitialResponse is the validated result of a POST /api/script/generate-with-images
// call. The remote engine must return at least a job_id and a known status.
type InitialResponse struct {
	JobID     string
	Status    string
	RawResult map[string]interface{} // the full raw map, preserved for the async polling path
}

// ErrContractMissingJobID is the contract error when the remote response
// does not include a job_id (or trace_id / id fallback).
var ErrContractMissingJobID = &RemoteError{
	Class:   RemoteErrorPermanent,
	Code:    "CONTRACT_MISSING_JOB_ID",
	Message: "remote response missing job_id",
}

// ErrContractUnknownStatus is the contract error when the remote response
// includes a status that is not in knownRemoteStatuses.
var ErrContractUnknownStatus = &RemoteError{
	Class:   RemoteErrorPermanent,
	Code:    "CONTRACT_UNKNOWN_STATUS",
	Message: "remote response has unknown status",
}

// ── Typed DTO ────────────────────────────────────────────────────────────────

// RemotePipelineResult is the typed DTO converted from the remote engine's
// raw response map. It is the canonical shape that flows into the Velox
// worker pipeline — no caller should pass the raw map directly.
//
// Conversion is done by ParseRemotePipelineResult, which extracts and
// validates each sub-component from the flattened remote result.
type RemotePipelineResult struct {
	RemoteJobID string
	Script      ScriptResult
	Scenes      []SceneResult
	Voiceover   VoiceoverResult
	Metadata    VideoMetadata
	Assets      []AssetReference
	// Raw preserves the original map for backward-compatibility with
	// enqueue.BuildPipelinePayload which still operates on maps. Callers
	// that need the typed fields should access them directly; callers
	// that need to feed BuildPipelinePayload can use ToWorkerPayloadChecked.
	Raw map[string]interface{}
}

// ScriptResult holds the generated script text and optional markdown/JSON paths.
type ScriptResult struct {
	Text         string // the script body (markdown or plain text)
	Title        string // video title / name
	MarkdownPath string // optional path to the .md file on the remote engine's disk
	JSONPath     string // optional path to the .json file on the remote engine's disk
}

// ClipAsset is the per-scene clip asset reference typed DTO.
// Mirrors apiwire.SubmitClip (without validate tags — typed-DTO
// layer does not own wire validation, the SubmitJob handler does).
//
// Phase 2 of the render-manifest plan: scene.Clip carries the
// authoritative clip URL directly. Worker reads it from
// scenes_json[i].clip.url — no more positional coupling with
// voiceover_paths[].
type ClipAsset struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	StartMS     int64  `json:"start_ms,omitempty"`
	EndMS       int64  `json:"end_ms,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
}

// VoiceoverAsset is the per-scene voiceover asset reference typed DTO.
// Mirrors apiwire.SubmitVoiceover. The nested form replaces the legacy
// position-coupled voiceover_paths[N] ↔ scenes[N] relationship: a
// single scene carries its own voiceover URL directly. Only this nested
// form is serialized into the renderer payload.
type VoiceoverAsset struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	Language    string `json:"language,omitempty"`
}

// SubtitlesAsset is the per-scene subtitles asset reference typed DTO.
// Mirrors apiwire.SubmitSubtitles.
type SubtitlesAsset struct {
	AssetID  string `json:"asset_id,omitempty"`
	Format   string `json:"format,omitempty"`
	URL      string `json:"url,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Language string `json:"language,omitempty"`
}

// SceneResult holds a single scene with its text and canonical asset references.
// The per-scene Clip / Voiceover / Subtitles nested objects are the source of
// truth for renderer asset URLs; legacy flat aliases are never serialized.
type SceneResult struct {
	Text      string `json:"text"`
	SceneID   string `json:"scene_id,omitempty"`
	Index     int64  `json:"index,omitempty"`
	Kind      string `json:"kind,omitempty"`
	ImageLink string `json:"image_link,omitempty"`
	// ClipLink is an alternative to ImageLink for video-clip-based scenes.
	ClipLink string `json:"clip_link,omitempty"`
	// StockLinks is the ordered pool of visual stock sources for a narrated
	// scene. The worker timeline builder shuffles this pool per scene and
	// loops it until the scene voiceover duration is covered.
	StockLinks    []string `json:"stock_links,omitempty"`
	StockFallback bool     `json:"stock_fallback,omitempty"`
	// DurationSeconds is the intended duration of the scene in seconds.
	// The OpenAPI contract on SubmitScene enforces 0.1 <= duration_seconds
	// <= 86400; the type is float64 so sub-second values (e.g. 0.1) survive
	// the JSON round-trip WITHOUT truncation. An int type would silently
	// turn "0.1" into "0" via the float64->int cast, an explicit
	// cross-package dependency that was neutralised when the SubmitJob
	// contract adopted sub-second durations for fine-grained scene cuts.
	DurationSeconds float64         `json:"duration_seconds,omitempty"`
	Clip            *ClipAsset      `json:"clip,omitempty"`
	Voiceover       *VoiceoverAsset `json:"voiceover,omitempty"`
	Subtitles       *SubtitlesAsset `json:"subtitles,omitempty"`
}

// VoiceoverResult holds voiceover references extracted at ingestion. Paths are
// retained for parsing/validation compatibility but are not worker payload data;
// scenes must carry their own canonical voiceover asset objects.
type VoiceoverResult struct {
	Paths []string // local paths or URLs to voiceover audio files
}

// VideoMetadata holds the social-platform metadata for the finished video.
type VideoMetadata struct {
	Title         string   `json:"title,omitempty"`
	Description   string   `json:"description,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	PrivacyStatus string   `json:"privacy_status,omitempty"`
}

// AssetReference holds a reference to a remote asset (image, clip, etc).
type AssetReference struct {
	Type string `json:"type"` // "image", "clip", "audio", "subtitle"
	URL  string `json:"url"`
	// LocalPath is the path on the remote engine's filesystem (if any).
	LocalPath string `json:"local_path,omitempty"`
}
