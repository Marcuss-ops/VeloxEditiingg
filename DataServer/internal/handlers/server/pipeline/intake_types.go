// Package pipeline — request DTOs for the external job intake contract.
package pipeline

import "velox-shared/contract/assembly"

// MaxSubmitJobBatchItems bounds the number of independent jobs accepted by
// POST /api/v1/jobs/batch. The limit protects request latency and keeps a
// batch's per-item response bounded; each item still has its own idempotency
// key and enqueue transaction.
const MaxSubmitJobBatchItems = 50

type SubmitJobRequest struct {
	// Canonical recipe envelope. Existing scene fields remain the typed
	// projection used by the renderer; spec is accepted only as the input
	// recipe body and is normalized before validation/enqueue.
	JobType         string                 `json:"job_type,omitempty"`
	TemplateID      string                 `json:"template_id,omitempty"`
	TemplateVersion int                    `json:"template_version,omitempty"`
	Spec            map[string]interface{} `json:"spec,omitempty"`
	Output          *SubmitOutput          `json:"output,omitempty"`

	// IdempotencyKey is required. 1..128 bytes after UTF-8 trim, valid
	// UTF-8, no control bytes, no ':' or '%' separators. See
	// ValidateIdempotencyKey in idempotency_validation.go for the
	// byte-level rules and rejection envelopes.
	IdempotencyKey string `json:"idempotency_key"`

	// VideoName is the display name for the resulting video. Capped
	// at MaxVideoNameBytes (300); empty allowed.
	VideoName string `json:"video_name,omitempty"`

	// ScriptText is the plain-text script used for TTS / overlay.
	// Content field — NOT trimmed. Empty allowed. No byte-length cap
	// here; matches the creator path's tolerance.
	ScriptText string `json:"script_text,omitempty"`
	AudioURL   string `json:"audio_url,omitempty"`
	CopyOnly   bool   `json:"copy_only,omitempty"`

	// Scenes is the scene list. Each scene drives one composited
	// segment. At least one scene is required; max MaxScenes (10k).
	Scenes []SubmitScene `json:"scenes"`

	// Layers are independent overlays: title, name, important phrase or
	// additional media. They are not folded into Scenes, so callers can
	// submit a video, images and any combination of overlays together.
	Layers []SubmitLayer `json:"layers,omitempty"`

	// VisualReplacements are already-composited video segments that
	// replace the base visual timeline over an absolute interval. Each
	// entry is a FINISHED video-only MP4; the master swaps it into the
	// timeline as a normal VideoSource (no overlay/compositing) and the
	// audio timeline remains independent. Resolved master-side by
	// VisualTimelineResolver; requires render_manifest + final_audio.
	VisualReplacements []SubmitVisualReplacement `json:"visual_replacements,omitempty"`

	// DeliveryPlan is the ordered list of delivery targets. Empty
	// allowed (defaults to scene.composite.v1's default resolver).
	// It remains backward-compatible with legacy senders. When
	// PublishingTarget is present, the handler resolves that selector into
	// this concrete plan before enqueue and rejects a request that supplies
	// both fields.
	DeliveryPlan []SubmitDeliveryPlanEntry `json:"delivery_plan,omitempty"`

	// PublishingTarget is an optional server-side channel/group selector.
	// It is never serialized into the renderer payload: the resolver expands
	// it into concrete DeliveryPlan entries before the canonical projection.
	PublishingTarget *SubmitPublishingTarget `json:"publishing_target,omitempty"`

	// Publications are the canonical publication intents for the
	// rendered outputs. They are kept separate from the renderer
	// payload and are consumed by the publication/delivery pipeline.
	Publications []SubmitPublication `json:"publications,omitempty"`

	// ManifestRef is OPTIONAL. When present, the Master downloads
	// the manifest JSON at `url`, verifies `sha256`, validates
	// `schema_version`, and uses the manifest-derived payload as
	// the worker input (replacing / overlaying the inline scene
	// list). Shape-level rules (URL scheme allow-list, sha256 hex
	// format, schema_version enum) are enforced by
	// ValidateSubmitJobRequest. Fetch + verification are handled by
	// ResolveRenderManifestRef before enqueue.
	ManifestRef *SubmitManifestRef `json:"manifest_ref,omitempty"`

	// Assembly is control-plane metadata for eager Velox preparation. It is
	// normalized and persisted on TaskSpec; it never enters renderer Payload.
	Assembly *assembly.ExternalAssemblyRequest `json:"assembly,omitempty"`

	// PlacementPinWorkerID is an optional operator/admin field that
	// forces the job to be placed on a specific worker, skipping
	// the normal placement matcher. Used by benchmark harnesses
	// (tests/worker-cert/smoke_one.sh, sequential_bench.sh) to
	// target a single worker without drain/resume. When non-empty,
	// the value is stored in the task spec payload as
	// _placement_pin_worker_id and enforced by the placement
	// matcher at dispatch time.
	PlacementPinWorkerID string `json:"placement_pin_worker_id,omitempty"`

	// ResolvedManifest fields are internal-only. They are populated by
	// ResolveRenderManifestRef after the Master fetches and verifies
	// manifest_ref. They are intentionally ignored by JSON decoding
	// because DisallowUnknownFields would reject these names on the public
	// wire contract; submitRequestToRawPayload copies them into the
	// worker payload after resolution so TaskSpec carries the immutable
	// manifest snapshot.
	ResolvedManifest       map[string]interface{} `json:"-"`
	ResolvedManifestRef    map[string]interface{} `json:"-"`
	ResolvedManifestSHA256 string                 `json:"-"`
}

// SubmitOutput contains renderer output constraints shared by every recipe.
type SubmitOutput struct {
	Width  int    `json:"width,omitempty"`
	Height int    `json:"height,omitempty"`
	FPS    int    `json:"fps,omitempty"`
	Format string `json:"format,omitempty"`
}

// SubmitScene is a single scene in the simplified job submission format.
//
// Field validation rules — submitted scenes MUST satisfy:
//   - Text non-empty (string length > 0 after trim).
//   - DurationSeconds in [MinSceneDurationSeconds,
//     MaxSceneDurationSeconds] (i.e. [0.1, 86400] seconds).
//
// Per-scene enrichment (Phase 2 of the render-manifest plan): the
// Clip / Voiceover / Subtitles nested objects REPLACE the legacy
// position-coupled relationship where a top-level array matched
// `scenes[N]` by index (a fragile contract that broke when a scene
// was reordered or removed). A single scene now carries its own
// clip / voiceover / subtitles assets directly; the worker reads
// them from `scenes_json[i].voiceover.url` (and .clip, .subtitles)
// instead of relying on a top-level positional array.
//
// All three nested objects are POINTERS so that a client that supplies
// `{}` (the parent object with no nested keys) is distinguishable from
// the "scene carries no clip/vo/sub" case (pointer nil). The
// handler-side validator rejects the empty-object case with three
// aggregated 422 violations.
//
// ValidateSubmitJobRequest (in this file) runs the per-scene check
// and aggregates failures into a single 422 with details pointing at
// the offending index.
type SubmitScene struct {
	// Text is the narration / overlay text for this scene. Must be
	// non-empty after trim.
	Text string `json:"text"`

	// SceneID is the canonical client-supplied scene identifier
	// (e.g. "scene-0"). Optional; used by callers that track scene
	// identity across requests.
	SceneID string `json:"scene_id,omitempty"`

	// Index is the scene's position in the video timeline. Optional;
	// the validator does not require continuity (a caller that
	// supplies only every other index is fine). Worker consumers
	// use scenes_json's array order as the canonical timeline
	// regardless of this field's value. Parity: int64 (matches
	// apiwire.SubmitScene.Index; the bridge into
	// remoteengine.SceneResult.Index is uniform, so a future
	// cross-package cast won't trip on the int->int64 widening).
	Index int64 `json:"index,omitempty"`

	// Kind is the scene's role tag (e.g. "intro", "clip", "outro").
	// Free-form string for forward-compatibility; the validator
	// caps it at 32 bytes.
	Kind string `json:"kind,omitempty"`

	// DurationSeconds is the intended duration of the scene. Must be
	// in [MinSceneDurationSeconds, MaxSceneDurationSeconds].
	DurationSeconds float64 `json:"duration_seconds"`

	// Clip is the per-scene clip asset reference (Phase 2 of the
	// render-manifest plan). Pointer nil = "no clip for this scene".
	// Pointer non-nil with empty body = rejected with aggregated 422.
	Clip *SubmitClip `json:"clip,omitempty"`

	// Stock is an optional secondary/fallback visual for recipe-driven
	// composite jobs. It is kept separate from Clip so the compiler can
	// preserve the primary clip and the stock fallback independently.
	Stock *SubmitClip `json:"stock,omitempty"`
	// StockAssets is the internal canonical pool produced when a recipe
	// supplies more than one stock asset. It is deliberately not a second
	// public JSON shape: the worker boundary serializes it as scene.stock[].
	StockAssets   []SubmitClip `json:"-"`
	StockFallback bool         `json:"stock_fallback,omitempty"`

	// Voiceover is the per-scene voiceover asset reference. Same
	// pointer semantics as Clip. The nested form REPLACES the legacy
	// top-level positional coupling.
	Voiceover *SubmitVoiceover `json:"voiceover,omitempty"`

	// Subtitles is the per-scene subtitles asset reference. Same
	// pointer semantics as Clip.
	Subtitles *SubmitSubtitles `json:"subtitles,omitempty"`
}

// SubmitClip is the per-scene clip asset reference nested inside
// SubmitScene. Mirrors apiwire.SubmitClip (no validate tags here —
// the handler-side ValidateSubmitJobRequest runs the shape checks
// when Clip != nil).
type SubmitClip struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	StartMS     int64  `json:"start_ms,omitempty"`
	EndMS       int64  `json:"end_ms,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
}

// SubmitVoiceover is the per-scene voiceover asset reference nested
// inside SubmitScene. Same pointer indirection contract as SubmitClip.
type SubmitVoiceover struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	Language    string `json:"language,omitempty"`
}

// SubmitSubtitles is the per-scene subtitles asset reference nested
// inside SubmitScene.
type SubmitSubtitles struct {
	AssetID  string `json:"asset_id,omitempty"`
	Format   string `json:"format,omitempty"`
	URL      string `json:"url,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Language string `json:"language,omitempty"`
}

// SubmitLayer is the API representation of one independent Chronon layer.
// Type is one of text, image, video or color; Role can distinguish title,
// name and important_phrase without creating separate renderer paths.
type SubmitLayer struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`
	Role            string    `json:"role,omitempty"`
	Text            string    `json:"text,omitempty"`
	Asset           string    `json:"asset,omitempty"`
	Source          string    `json:"source,omitempty"`
	Font            string    `json:"font,omitempty"`
	FontSize        float64   `json:"font_size,omitempty"`
	Position        []float64 `json:"position,omitempty"`
	StartSeconds    float64   `json:"start_seconds,omitempty"`
	DurationSeconds float64   `json:"duration_seconds,omitempty"`
	Preset          string    `json:"preset,omitempty"`
	Animation       string    `json:"animation,omitempty"`
}

// SubmitVisualReplacement is one already-composited video segment that
// replaces the base visual timeline over [timeline_start_us,
// timeline_end_us). It deliberately reuses SubmitClip as the asset
// envelope (one asset format / one resolver / one validator) rather than
// introducing a parallel asset type. It is NOT a SubmitLayer role=overlay:
// a visual replacement is the finished video, not a compositing input.
type SubmitVisualReplacement struct {
	ReplacementID string      `json:"replacement_id"`
	Asset         *SubmitClip `json:"asset"`

	TimelineStartUS int64 `json:"timeline_start_us"`
	TimelineEndUS   int64 `json:"timeline_end_us"`

	ProfileID string `json:"profile_id"`
}

// SubmitPublishingTarget selects one concrete channel or one upstream group.
// Channel selection uses the opaque Velox destination_id returned by the
// publishing catalog; group selection uses the upstream group_id. The
// workspace_id is explicit so the server can fail closed against a catalog
// from another workspace.
//
// Platform is opaque and provider-neutral. It is optional for channel
// selection (derived from the destination registry) but required for group
// selection, because Velox has no local destination to read it from before
// the upstream catalog query is scoped.
type SubmitPublishingTarget struct {
	WorkspaceID   int64  `json:"workspace_id"`
	Type          string `json:"type"`
	DestinationID string `json:"destination_id,omitempty"`
	GroupID       int64  `json:"group_id,omitempty"`
	Platform      string `json:"platform,omitempty"`
}

// SubmitDeliveryPlanEntry is a single destination in the delivery plan.
//
// Field validation rules — submitted entries MUST satisfy:
//   - DestinationID non-empty (string length > 0 after trim).
//   - RetryBudget is a POINTER so that an explicit client-supplied value
//     of 0 round-trips distinctly from "field omitted". A nil pointer
//     means "client did not specify" — submitRequestToRawPayload
//     substitutes the OpenAPI default (DefaultRetryBudget = 3) at
//     normalization time. A pointer-to-0 means "client explicitly
//     wants 0 retries" — preserved verbatim into the worker payload.
//     Without the *int pointer, the Go default for int (0) would
//     silently merge with the omitted-field default and clients could
//     not distinguish "0 explicitly" from "omitted".
type SubmitDeliveryPlanEntry struct {
	DestinationID string `json:"destination_id"`
	Priority      int    `json:"priority,omitempty"`
	// RetryBudget is *int so that an explicit JSON value 0 round-trips
	// distinctly from the omitted case (nil). See the type doc for
	// the contract.
	RetryBudget *int `json:"retry_budget,omitempty"`
	Metadata    any  `json:"metadata,omitempty"`
}

// SubmitPublication describes one concrete publication of a rendered output.
// Its metadata and destinations belong to the delivery pipeline, not to the
// renderer worker payload.
type SubmitPublication struct {
	PublicationID   string                             `json:"publication_id"`
	OutputRef       SubmitPublicationOutputRef         `json:"output_ref"`
	Language        string                             `json:"language,omitempty"`
	DefaultLanguage string                             `json:"default_language,omitempty"`
	Metadata        SubmitPublicationMetadata          `json:"metadata,omitempty"`
	Localizations   map[string]SubmitLocalizedMetadata `json:"localizations,omitempty"`
	Destinations    []SubmitPublicationDestination     `json:"destinations"`
	ProviderOptions map[string]any                     `json:"provider_options,omitempty"`
}

// SubmitPublicationOutputRef selects either a language variant or an artifact
// role produced by rendering. Validation will require exactly one selector.
type SubmitPublicationOutputRef struct {
	VariantID    string `json:"variant_id,omitempty"`
	ArtifactRole string `json:"artifact_role,omitempty"`
}

// SubmitPublicationMetadata contains provider-independent publication fields.
// Destination adapters enforce platform-specific length and capability limits.
type SubmitPublicationMetadata struct {
	Title                  string   `json:"title,omitempty"`
	Description            string   `json:"description,omitempty"`
	Tags                   []string `json:"tags,omitempty"`
	CategoryID             string   `json:"category_id,omitempty"`
	Privacy                string   `json:"privacy,omitempty"`
	PublishAt              string   `json:"publish_at,omitempty"`
	MadeForKids            *bool    `json:"made_for_kids,omitempty"`
	ContainsSyntheticMedia *bool    `json:"contains_synthetic_media,omitempty"`
}

// SubmitLocalizedMetadata contains title and description for one locale.
type SubmitLocalizedMetadata struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// SubmitPublicationDestination is one independently routable destination for
// a publication. RetryBudget is a pointer so explicit zero survives decoding.
type SubmitPublicationDestination struct {
	DestinationID    string                     `json:"destination_id"`
	CredentialRef    string                     `json:"credential_ref,omitempty"`
	Priority         int                        `json:"priority,omitempty"`
	RetryBudget      *int                       `json:"retry_budget,omitempty"`
	MetadataOverride *SubmitPublicationMetadata `json:"metadata_override,omitempty"`
	ProviderOptions  map[string]any             `json:"provider_options,omitempty"`
}

// SubmitManifestRef points to a `velox.render-manifest.v1` JSON the
// client uploaded to a reachable store (Drive, GCS, S3, …). The
// Master fetches the JSON, verifies SHA-256 against the SHA-256 the
// client supplied here, validates the schema_version, and replaces
// the inline scene list with the manifest-derived payload.
//
// Three fields, all required WHEN the parent `manifest_ref` is
// present (the *SubmitManifestRef pointer distinguishes "no
// manifest_ref at all" from "manifest_ref declared but empty" —
// the latter is rejected):
//
//   - SchemaVersion is the closed enum of accepted manifest
//     versions. Today only `velox.render-manifest.v1` is accepted;
//     future versions (`v2`, …) MUST be added to the `oneof` list
//     AND to manifestRefSchemaVersions BEFORE the new resolver is
//     shipped, so the contract and the implementation cannot drift.
//
//   - URL is the canonical pointer to the manifest JSON. MUST be a
//     parseable URL on the http(s) scheme OR on the velox-asset://
//     scheme (the latter only when the asset is reachable through
//     the Master asset-bridge; the resolver owns that policy).
//     The regex is intentionally permissive — the schemagen
//     has no native `format: uri` distinction for velox-asset://
//     so the strict scheme allow-list is enforced by the
//     shape-level helper in ValidateSubmitJobRequest.
//
//   - SHA256 is the lowercase hex SHA-256 of the manifest JSON
//     body. The Master re-downloads the JSON and verifies the
//     SHA-256 against this value BEFORE substituting it into the
//     worker payload (fail-closed).
//
// SubmitJobBatchRequest is the wire shape for POST /api/v1/jobs/batch.
// Items are intentionally full SubmitJobRequest values so delivery_plan and
// publications remain backward-compatible with the single-job endpoint.
type SubmitJobBatchRequest struct {
	BatchID string             `json:"batch_id"`
	Items   []SubmitJobRequest `json:"items"`
}

// SubmitJobBatchItemResult reports one independent batch item outcome.
type SubmitJobBatchItemResult struct {
	Index          int      `json:"index"`
	IdempotencyKey string   `json:"idempotency_key"`
	JobID          string   `json:"job_id,omitempty"`
	Status         string   `json:"status"`
	Errors         []string `json:"errors,omitempty"`
}

// SubmitJobBatchResponse contains an outcome for every submitted item.
// A failed item never changes the status or idempotency semantics of another.
type SubmitJobBatchResponse struct {
	BatchID string                     `json:"batch_id"`
	Items   []SubmitJobBatchItemResult `json:"items"`
}

type SubmitManifestRef struct {
	// SchemaVersion is the closed enum of accepted manifest versions.
	// Today only `velox.render-manifest.v1` is accepted.
	SchemaVersion string `json:"schema_version"`

	// URL is the canonical pointer to the manifest JSON.
	URL string `json:"url"`

	// SHA256 is the lowercase hex SHA-256 of the manifest JSON body.
	SHA256 string `json:"sha256"`
}
