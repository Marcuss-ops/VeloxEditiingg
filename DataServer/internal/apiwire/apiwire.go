// Package apiwire is the canonical Go source-of-truth for the
// inter-service HTTP contract between Velox Master and external
// producers (creator machines on POST /api/v1/creator/jobs and
// external automation on POST /api/v1/jobs).
//
// cmd/api-schema-gen reads the structs in this package via reflect
// (internal/schemagen) and emits the matching
// api/openapi.yaml.components/schemas YAML. The spec is DERIVED from
// this file — never the other way around. Edit a struct's validate:"…"
// tag here, re-run `go run ./cmd/api-schema-gen -apply`, and the
// constraints propagate to the YAML automatically. Eliminates the
// classic duplication where openapi.yaml.creator_push.maxLength=300
// falls out of sync with Go-side MaxVideoNameBytes = 300.
//
// Tag grammar: see the schemagen package doc. Supported rules on
// these types:
//
//   - min=N    → minimum / minLength / minItems depending on Go kind
//   - max=N    → maximum / maxLength / maxItems
//   - required → listed in the schema's required array
//   - oneof=…  → enum
//   - gte/lte=N→ numeric inclusive bounds
//   - url      → format: uri
//
// To add a new schema type, declare the struct here, tag decisively
// (especially `required` vs `omitempty`), and add the type name to
// the registry list in cmd/api-schema-gen.
package apiwire

// ── POST /api/v1/jobs family ────────────────────────────────────────────────

// SubmitJobRequest is the wire shape for POST /api/v1/jobs.
// Flat, intuitive, semantically stable: idempotency_key is the
// dedup handle; inline bodies provide scenes[], while manifest_ref
// bodies may omit scenes because the Master substitutes them from the
// fetched manifest; everything else is optional content + delivery metadata.
//
// Idempotency-key byte-level rules (1..128, no ':' or '%', valid
// UTF-8, no control chars) are enforced separately by
// job_submit.ValidateIdempotencyKey — the MAX here is the
// OpenAPI-level alias for the validator's byte cap.
//
// ManifestRef is OPTIONAL: a client that already uploaded clip /
// voiceover / subtitle assets to a reachable store (Drive, GCS, …)
// and packaged the immutable scene list into a `velox.render-manifest.v1`
// JSON can pass a pointer to that JSON instead of inlining the
// scene list. The Master fetches the JSON, verifies the SHA-256,
// validates the schema_version, and replaces the inline scene list
// with the manifest-derived payload. The byte-level shape is
// enforced by job_submit.ValidateSubmitJobRequest (not at the
// JSON-tag level) because velox-asset:// is not a standard URL
// format and the schemagen doesn't know how to express the
// http(s) + velox-asset:// scheme choice cleanly.
type SubmitJobRequest struct {
	JobType          string                    `json:"job_type,omitempty"`
	TemplateID       string                    `json:"template_id,omitempty"`
	TemplateVersion  int                       `json:"template_version,omitempty"`
	Spec             map[string]any            `json:"spec,omitempty"`
	Output           *SubmitOutput             `json:"output,omitempty"`
	IdempotencyKey   string                    `json:"idempotency_key" validate:"required,min=1,max=128"`
	VideoName        string                    `json:"video_name,omitempty" validate:"omitempty,max=300"`
	ScriptText       string                    `json:"script_text,omitempty"`
	Scenes           []SubmitScene             `json:"scenes,omitempty" validate:"omitempty,max=10000"`
	Layers           []SubmitLayer             `json:"layers,omitempty" validate:"omitempty,dive"`
	AudioTracks      []SubmitAudioTrack        `json:"audio_tracks,omitempty" validate:"omitempty,dive"`
	DeliveryPlan     []SubmitDeliveryPlanEntry `json:"delivery_plan,omitempty" validate:"omitempty,dive"`
	PublishingTarget *SubmitPublishingTarget   `json:"publishing_target,omitempty" validate:"omitempty"`
	Publications     []SubmitPublication       `json:"publications,omitempty" validate:"omitempty,dive"`
	ManifestRef      *SubmitManifestRef        `json:"manifest_ref,omitempty" validate:"omitempty"`

	// PlacementPinWorkerID is an optional operator/admin field that
	// forces the job to be placed on a specific worker, skipping the
	// normal placement matcher. Used by benchmark harnesses
	// (tests/worker-cert/smoke_one.sh, sequential_bench.sh) to
	// target a single worker without drain/resume. When non-empty,
	// the value is stored in the task spec payload as
	// _placement_pin_worker_id and enforced by the placement matcher
	// at dispatch time. Clients polling GET /api/v1/jobs/{id} will
	// see the actual worker_id that executed the job, providing
	// authoritative placement verification.
	PlacementPinWorkerID string `json:"placement_pin_worker_id,omitempty" validate:"omitempty,max=128"`
}

type SubmitOutput struct {
	Width  int    `json:"width,omitempty" validate:"omitempty,gte=1,lte=16384"`
	Height int    `json:"height,omitempty" validate:"omitempty,gte=1,lte=16384"`
	FPS    int    `json:"fps,omitempty" validate:"omitempty,gte=1,lte=240"`
	Format string `json:"format,omitempty" validate:"omitempty,oneof=mp4 mov webm"`
}

// SubmitPublishingTarget carries an InstaEdit-owned channel/group selection
// into the submit adapter. Channel targets use an opaque destination_id;
// group targets use group_id. The workspace scope is explicit so selection
// cannot cross a workspace boundary. Velox validates the transient upstream
// selection but does not own or mirror the catalog.
type SubmitPublishingTarget struct {
	WorkspaceID   int64  `json:"workspace_id" validate:"required,gte=1"`
	Type          string `json:"type" validate:"required,oneof=channel group"`
	DestinationID string `json:"destination_id,omitempty" validate:"omitempty,min=1"`
	GroupID       int64  `json:"group_id,omitempty" validate:"omitempty,gte=1"`
	Platform      string `json:"platform,omitempty" validate:"omitempty,min=1"`
}

// SubmitJobBatchRequest is the wire shape for POST /api/v1/jobs/batch.
// Each item is a complete SubmitJobRequest and retains the legacy
// delivery_plan field alongside the canonical publications field.
type SubmitJobBatchRequest struct {
	BatchID string             `json:"batch_id" validate:"required,min=1,max=128"`
	Items   []SubmitJobRequest `json:"items" validate:"required,min=1,max=50,dive"`
}

// SubmitJobBatchItemResult reports the independent outcome of one batch item.
type SubmitJobBatchItemResult struct {
	Index          int      `json:"index" validate:"gte=0"`
	IdempotencyKey string   `json:"idempotency_key,omitempty"`
	JobID          string   `json:"job_id,omitempty"`
	Status         string   `json:"status" validate:"required"`
	Errors         []string `json:"errors,omitempty"`
}

// SubmitJobBatchResponse contains one result for every submitted item.
type SubmitJobBatchResponse struct {
	BatchID string                     `json:"batch_id"`
	Items   []SubmitJobBatchItemResult `json:"items"`
}

// PublishingCatalogRequest is a retained legacy schema for the retired
// publishing catalog route. Runtime catalog discovery is InstaEdit-owned and
// no Velox handler decodes this type.
type PublishingCatalogRequest struct {
	WorkspaceID int64  `json:"workspace_id" validate:"required,gte=1"`
	Platform    string `json:"platform" validate:"required,min=1"`
}

// PublishingCatalogCapabilities is a retained legacy schema-only capability
// block for the retired catalog contract; runtime capability ownership is
// InstaEdit's.
type PublishingCatalogCapabilities struct {
	UploadVideo  bool `json:"upload_video"`
	SetThumbnail bool `json:"set_thumbnail"`
	Publish      bool `json:"publish"`
	Schedule     bool `json:"schedule"`
}

// PublishingCatalogChannel is a retained legacy schema-only channel shape.
// Runtime channel selection and destination ownership belong to InstaEdit.
type PublishingCatalogChannel struct {
	Type              string                        `json:"type" validate:"required,oneof=channel"`
	DestinationID     string                        `json:"destination_id" validate:"required,min=1"`
	PlatformAccountID int64                         `json:"platform_account_id" validate:"required,gte=1"`
	ChannelID         string                        `json:"channel_id" validate:"required,min=1"`
	Name              string                        `json:"name" validate:"required,min=1"`
	Status            string                        `json:"status" validate:"required,min=1"`
	Enabled           bool                          `json:"enabled"`
	CanPost           bool                          `json:"can_post"`
	Capabilities      PublishingCatalogCapabilities `json:"capabilities" validate:"required"`
	BlockReason       string                        `json:"block_reason,omitempty"`
	TargetErrorCode   string                        `json:"target_error_code,omitempty"`
}

// PublishingCatalogGroup is a retained legacy schema-only group shape.
// Runtime group membership and expansion belong to InstaEdit.
type PublishingCatalogGroup struct {
	Type                   string `json:"type" validate:"required,oneof=group"`
	GroupID                int64  `json:"group_id" validate:"required,gte=1"`
	Name                   string `json:"name" validate:"required,min=1"`
	ParentGroupID          *int64 `json:"parent_group_id,omitempty"`
	MemberCount            int    `json:"member_count" validate:"gte=0"`
	PublishableMemberCount int    `json:"publishable_member_count" validate:"gte=0"`
	Status                 string `json:"status,omitempty"`
	CanPost                bool   `json:"can_post"`
	BlockReason            string `json:"block_reason,omitempty"`
	TargetErrorCode        string `json:"target_error_code,omitempty"`
}

// PublishingCatalogResponse is a retained legacy schema for the retired
// publishing catalog route. It remains registered only for backwards-compatible
// documentation generation; Velox no longer serves this catalog.
type PublishingCatalogResponse struct {
	WorkspaceID int64                      `json:"workspace_id" validate:"required,gte=1"`
	Platform    string                     `json:"platform" validate:"required,min=1"`
	Channels    []PublishingCatalogChannel `json:"channels"`
	Groups      []PublishingCatalogGroup   `json:"groups"`
}

// SubmitPublication describes one concrete publication of a rendered output.
// Publication metadata is intentionally separate from the render worker payload.
type SubmitPublication struct {
	PublicationID   string                             `json:"publication_id" validate:"required,min=1"`
	OutputRef       SubmitPublicationOutputRef         `json:"output_ref" validate:"required"`
	Language        string                             `json:"language,omitempty"`
	DefaultLanguage string                             `json:"default_language,omitempty"`
	Metadata        SubmitPublicationMetadata          `json:"metadata,omitempty"`
	Localizations   map[string]SubmitLocalizedMetadata `json:"localizations,omitempty"`
	Destinations    []SubmitPublicationDestination     `json:"destinations" validate:"required,min=1,dive"`
	ProviderOptions map[string]any                     `json:"provider_options,omitempty"`
}

// SubmitPublicationOutputRef selects either a language variant or an artifact role.
type SubmitPublicationOutputRef struct {
	VariantID    string `json:"variant_id,omitempty"`
	ArtifactRole string `json:"artifact_role,omitempty"`
}

// SubmitPublicationMetadata contains provider-independent publication metadata.
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

// SubmitPublicationDestination is one independently routable publication target.
type SubmitPublicationDestination struct {
	DestinationID    string                     `json:"destination_id" validate:"required,min=1"`
	Priority         int                        `json:"priority,omitempty" validate:"omitempty,gte=0"`
	RetryBudget      *int                       `json:"retry_budget,omitempty" validate:"omitempty,gte=0"`
	MetadataOverride *SubmitPublicationMetadata `json:"metadata_override,omitempty" validate:"omitempty"`
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
//     BEFORE the new resolver is shipped, so the contract and the
//     implementation cannot drift.
//
//   - URL is the canonical pointer to the manifest JSON. MUST be a
//     parseable URL on the http(s) scheme OR on the velox-asset://
//     scheme (the latter only when the asset is reachable through
//     the Master asset-bridge; the resolver owns that policy).
//     The regex below is intentionally permissive — the schemagen
//     has no native `format: uri` distinction for velox-asset://
//     so the strict scheme allow-list is enforced by the
//     shape-level helper in job_submit.ValidateSubmitJobRequest.
//
//   - SHA256 is the lowercase hex SHA-256 of the manifest JSON
//     body. The Master re-downloads the JSON and verifies the
//     SHA-256 against this value BEFORE substituting it into the
//     worker payload (fail-closed).
//
// Drift guard: the `max=2048` byte cap on URL MUST stay in lockstep
// with pipeline.MaxManifestRefURLBytes (handler-side constant).
// Both copies pin the same ceiling — the wire schema advertises
// maxLength:2048 to clients, and the runtime validator enforces the
// same cap with details[].issue="max_length". A future bump MUST
// touch both. The drift-guard test
// TestSubmitManifestRef_MaxLengthMatchesHandlerConstant in
// apiwire_test.go hard-fails if the two diverge.
type SubmitManifestRef struct {
	SchemaVersion string `json:"schema_version" validate:"required,oneof=velox.render-manifest.v1"`
	URL           string `json:"url" validate:"required,min=1,max=2048,regex=^(https?://|velox-asset://).+"`
	SHA256        string `json:"sha256" validate:"required,len=64,regex=^[0-9a-f]{64}$"`
}

// SubmitScene is one composited segment in the simplified job.
// duration_seconds is bounded by [0.1, 86400] server-side; the
// `gte=0.1, lte=86400` rule here mirrors that in the spec, and
// also matches the constants in job_submit.go (MinSceneDurationSeconds
// + MaxSceneDurationSeconds). Drift between the two was the original
// "manual duplication" pain point this package replaces.
//
// Per-scene enrichment (Phase 2 of the render-manifest plan): the
// Clip / Voiceover / Subtitles nested objects REPLACE the legacy
// position-coupled relationship where `voiceover_paths[N]` matched
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
// aggregated 422 violations (URL / format / language checks fire).
type SubmitScene struct {
	Text    string `json:"text" validate:"required,min=1"`
	SceneID string `json:"scene_id,omitempty" validate:"omitempty,max=64"`
	// Index aligned on int64 with StartMS/EndMS/DurationMS precedent
	// and with remoteengine.SceneResult.Index (populated via
	// intFromAnyMap → int64). Closing the drift here keeps the
	// wire-DTO bridge uniform across submit + creator paths.
	Index           int64            `json:"index,omitempty" validate:"omitempty,gte=0"`
	Kind            string           `json:"kind,omitempty" validate:"omitempty,max=32"`
	DurationSeconds float64          `json:"duration_seconds" validate:"required,gte=0.1,lte=86400"`
	Clip            *SubmitClip      `json:"clip,omitempty" validate:"omitempty"`
	Stock           *SubmitClip      `json:"stock,omitempty" validate:"omitempty"`
	StockLinks      []string         `json:"stock_links,omitempty"`
	StockFallback   bool             `json:"stock_fallback,omitempty"`
	Voiceover       *SubmitVoiceover `json:"voiceover,omitempty" validate:"omitempty"`
	Subtitles       *SubmitSubtitles `json:"subtitles,omitempty" validate:"omitempty"`
}

// SubmitClip is the per-scene clip asset reference nested inside
// SubmitScene. Every field is optional individually; the parent's
// pointer (SubmitScene.Clip *SubmitClip) is what makes the whole
// nested object optional on the wire (`json:"clip,omitempty"`).
//
// All fields match the canonical render-manifest.v1 scene.clip
// shape (asset_id / drive_file_id / url / sha256 / start_ms /
// end_ms / duration_ms). The same struct is reused by CreatorScene
// (apiwire) so the creator-machine wire shape and the
// simplified-submit wire shape agree on the per-scene clip envelope.
type SubmitClip struct {
	AssetID     string `json:"asset_id,omitempty" validate:"omitempty,max=128"`
	DriveFileID string `json:"drive_file_id,omitempty" validate:"omitempty,max=128"`
	URL         string `json:"url,omitempty" validate:"omitempty,url,max=2048"`
	SHA256      string `json:"sha256,omitempty" validate:"omitempty,len=64,regex=^[0-9a-f]{64}$"`
	StartMS     int64  `json:"start_ms,omitempty" validate:"omitempty,gte=0"`
	EndMS       int64  `json:"end_ms,omitempty" validate:"omitempty,gte=0"`
	DurationMS  int64  `json:"duration_ms,omitempty" validate:"omitempty,gte=0"`
}

// SubmitVoiceover is the per-scene voiceover asset reference nested
// inside SubmitScene. Same pointer-indirection contract as
// SubmitClip: pointer nil = "no voiceover for this scene" (default);
// pointer non-nil with empty body = rejected with aggregated 422
// (handler validator checks URL + language).
type SubmitVoiceover struct {
	AssetID     string `json:"asset_id,omitempty" validate:"omitempty,max=128"`
	DriveFileID string `json:"drive_file_id,omitempty" validate:"omitempty,max=128"`
	URL         string `json:"url,omitempty" validate:"omitempty,url,max=2048"`
	SHA256      string `json:"sha256,omitempty" validate:"omitempty,len=64,regex=^[0-9a-f]{64}$"`
	DurationMS  int64  `json:"duration_ms,omitempty" validate:"omitempty,gte=0"`
	Language    string `json:"language,omitempty" validate:"omitempty,len=2"`
}

// SubmitSubtitles is the per-scene subtitles asset reference nested
// inside SubmitScene. The `format` enum is intentionally narrow
// (ass / srt / vtt) — the three subtitle flavours the Chronon
// compositor accepts today. A future addition (e.g. `ttml`) requires
// bumping the `oneof` list in lockstep with the renderer support.
type SubmitSubtitles struct {
	AssetID  string `json:"asset_id,omitempty" validate:"omitempty,max=128"`
	Format   string `json:"format,omitempty" validate:"omitempty,oneof=ass srt vtt"`
	URL      string `json:"url,omitempty" validate:"omitempty,url,max=2048"`
	SHA256   string `json:"sha256,omitempty" validate:"omitempty,len=64,regex=^[0-9a-f]{64}$"`
	Language string `json:"language,omitempty" validate:"omitempty,len=2"`
}

// SubmitLayer is one independent Chronon rendering layer. Type +
// Role enums match the Chronon's layer taxonomy.
type SubmitLayer struct {
	ID              string    `json:"id" validate:"required,min=1"`
	Type            string    `json:"type" validate:"required,oneof=text image video color"`
	Role            string    `json:"role,omitempty" validate:"omitempty,oneof=title name important_phrase overlay"`
	Text            string    `json:"text,omitempty"`
	Asset           string    `json:"asset,omitempty"`
	Source          string    `json:"source,omitempty"`
	Font            string    `json:"font,omitempty"`
	FontSize        float64   `json:"font_size,omitempty" validate:"omitempty,gte=0"`
	Position        []float64 `json:"position,omitempty" validate:"omitempty,len=2"`
	StartSeconds    float64   `json:"start_seconds,omitempty" validate:"omitempty,gte=0"`
	DurationSeconds float64   `json:"duration_seconds,omitempty" validate:"omitempty,gte=0"`
	Preset          string    `json:"preset,omitempty"`
	Animation       string    `json:"animation,omitempty"`
}

// SubmitAudioTrack is a top-level audio track mixed into the final render.
// Independent from per-scene voiceover — this is for global audio layers
// such as background music, ambient sound, or narration beds.
type SubmitAudioTrack struct {
	AssetID         string  `json:"asset_id,omitempty" validate:"omitempty,max=128"`
	SourceURL       string  `json:"source_url,omitempty" validate:"omitempty,url,max=2048"`
	Role            string  `json:"role,omitempty" validate:"omitempty,oneof=voiceover scene_clip_audio background_music sfx"`
	Volume          float64 `json:"volume,omitempty" validate:"omitempty,gte=0,lte=2"`
	StartTimeOffset float64 `json:"start_time_offset,omitempty" validate:"omitempty,gte=0"`
	DurationSeconds float64 `json:"duration_seconds,omitempty" validate:"omitempty,gte=0"`
	Loop            bool    `json:"loop,omitempty"`
	FadeInSeconds   float64 `json:"fade_in_seconds,omitempty" validate:"omitempty,gte=0"`
	FadeOutSeconds  float64 `json:"fade_out_seconds,omitempty" validate:"omitempty,gte=0"`
	DuckingEnabled  bool    `json:"ducking_enabled,omitempty"`
}

// SubmitDeliveryPlanEntry is one destination in the delivery plan.
// retry_budget is *int so an explicit client-supplied 0 round-trips
// distinctly from "field omitted" — the boundary contract that
// openapi.yaml alone could not enforce without a hand-written
// patch on the spec.
type SubmitDeliveryPlanEntry struct {
	DestinationID string         `json:"destination_id" validate:"required,min=1"`
	Priority      int            `json:"priority,omitempty" validate:"omitempty,gte=0"`
	RetryBudget   int            `json:"retry_budget,omitempty" validate:"omitempty,gte=0"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

// ── POST /api/v1/creator/jobs family ───────────────────────────────────────

// CreatorPushRequest is the envelope sent by Creator machines to
// push a completed payload directly to the Master. The payload goes
// through the typed RemotePipelineResult DTO via
// remoteengine.ParseRemotePipelineResult; here we mirror only the
// WIRE shape (the DTO conversion is internal).
type CreatorPushRequest struct {
	SourceProvider   string             `json:"source_provider" validate:"required,min=1"`
	SourceJobID      string             `json:"source_job_id,omitempty"`
	TargetExecutorID string             `json:"target_executor_id,omitempty"`
	Payload          CreatorPushPayload `json:"payload" validate:"required"`
}

// CreatorPushPayload is the FLAT wire shape nested inside
// CreatorPushRequest.payload. Distinct from RemotePipelineResult
// (the typed nested DTO documented separately in the spec as a
// parser cross-check; the wire shape here is canonical).
type CreatorPushPayload struct {
	Status         string              `json:"status" validate:"required,oneof=completed completed_with_warnings"`
	JobID          string              `json:"job_id" validate:"required,min=1"`
	VideoName      string              `json:"video_name,omitempty"`
	ScriptText     string              `json:"script_text,omitempty"`
	VoiceoverPaths []string            `json:"voiceover_paths,omitempty" validate:"omitempty,dive"`
	Scenes         []CreatorScene      `json:"scenes,omitempty" validate:"omitempty,dive"`
	DeliveryPlan   []DeliveryPlanEntry `json:"delivery_plan,omitempty" validate:"omitempty,dive"`
}

// CreatorScene is one scene inside CreatorPushPayload.scenes.
// clip_link and clip_path are accepted aliases (the handler maps
// both onto the same DTO field) so the validate rule does not
// constrain the URI scheme here — the SSRF/url-filter checklist is
// enforced server-side outside the wire contract.
//
// Per-scene enrichment parity with SubmitScene: scene_id, index,
// kind, clip{}, voiceover{}, subtitles{} nested objects are
// available on the Creator-machine wire shape too (the same
// SubmitClip / SubmitVoiceover / SubmitSubtitles types are reused).
// The Creator path already carried positional voiceover couplings
// (the typed VoiceoverResult.Paths is top-level), so the per-scene
// enrichment here is purely additive — Creator-machine clients can
// adopt it incrementally without breaking the legacy flat shape.
type CreatorScene struct {
	Text    string `json:"text" validate:"required,min=1"`
	SceneID string `json:"scene_id,omitempty" validate:"omitempty,max=64"`
	// Index parity with SubmitScene: int64 (see SubmitScene.Index
	// comment for rationale: matches StartMS/EndMS/DurationMS
	// precedent and keeps the wire-DTO bridge uniform across the
	// Creator Push and POST /api/v1/jobs submission paths).
	Index           int64            `json:"index,omitempty" validate:"omitempty,gte=0"`
	Kind            string           `json:"kind,omitempty" validate:"omitempty,max=32"`
	ClipLink        string           `json:"clip_link,omitempty"`
	ClipPath        string           `json:"clip_path,omitempty"`
	ImageLink       string           `json:"image_link,omitempty" validate:"omitempty"`
	DurationSeconds float64          `json:"duration_seconds" validate:"required,gte=0.1"`
	Clip            *SubmitClip      `json:"clip,omitempty" validate:"omitempty"`
	Voiceover       *SubmitVoiceover `json:"voiceover,omitempty" validate:"omitempty"`
	Subtitles       *SubmitSubtitles `json:"subtitles,omitempty" validate:"omitempty"`
}

// DeliveryPlanEntry is the Creator-side delivery destination enum.
// The destination_id enum is intentionally narrow: a Creator
// machine only needs to pick from the canonical set of supported
// storages.
type DeliveryPlanEntry struct {
	DestinationID string         `json:"destination_id" validate:"required,oneof=drive gcs s3 youtube local"`
	Priority      int            `json:"priority,omitempty" validate:"omitempty,gte=0"`
	RetryBudget   int            `json:"retry_budget,omitempty" validate:"omitempty,gte=0"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

// CreatorMetadata is the social-platform metadata attached to the
// Creator's final video. privacy_status enum mirrors YouTube's
// canonical set; tags is bounded at 500 per YouTube's quota.
type CreatorMetadata struct {
	Title         string   `json:"title,omitempty"`
	Description   string   `json:"description,omitempty"`
	Tags          []string `json:"tags,omitempty" validate:"omitempty,max=500"`
	PrivacyStatus string   `json:"privacy_status,omitempty" validate:"omitempty,oneof=public unlisted private"`
}

// CreatorAsset references a remote asset the Creator has uploaded.
// Type is a closed enum matching the asset-family vocabulary.
type CreatorAsset struct {
	Type      string `json:"type" validate:"required,oneof=image clip audio subtitle"`
	URL       string `json:"url,omitempty" validate:"omitempty"`
	LocalPath string `json:"local_path,omitempty"`
}
