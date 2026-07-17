package socialclient

// DeliverArtifactRequest is the typed payload Velox POSTs to the
// social_repo's `/internal/v1/deliveries` endpoint.
//
// Opaque-mode wire contract (Residuo 3 of the YouTube→Social closure):
//
//	{
//	  "external_delivery_id":   "delivery_<uuid>",          // Velox job_deliveries.delivery_id
//	  "idempotency_key":        "delivery_<uuid>|dest_X",  // artifact × destination stable
//	  "social_destination_id":  "social_dest_<uuid>",      // opaque, social_repo-resolved
//	  "artifact":               ArtifactPayload,
//	  "metadata":               { ... title/desc/tags/... },  // unknown to Velox, passed through
//	  "publish_at":             "2026-07-20T12:00:00Z",     // optional scheduling
//	  "callback_url":           "https://velox/.../callback"  // optional webhook
//	}
//
// Velox does NOT know the platform, the account, the channel, or any
// other platform-specific concept: the social_repo is the
// authoritative resolver from the opaque SocialDestinationID. Velox
// only forwards what the operator set on
// `delivery_destinations.social_destination_id` (column added by
// migration 091 in the Residuo 2 closure).
//
// The DeliveryRunner hydrates destinations and fails-closed with
// code DESTINATION_UNMAPPED on empty SocialDestinationID — see
// internal/deliveries/runner.go::hydrateDestination — so this
// field is never empty at DeliverArtifact time. The struct tags
// reflect that contract: SocialDestinationID has NO `omitempty` so
// any drift between runner and socialclient is detected at marshal
// time rather than producing a silent malformed JSON POST.
type DeliverArtifactRequest struct {
	ExternalDeliveryID  string `json:"external_delivery_id"`
	IdempotencyKey      string `json:"idempotency_key"`
	SocialDestinationID string `json:"social_destination_id"`

	Artifact ArtifactPayload `json:"artifact"`

	// Metadata is unknown to Velox and passed through verbatim so the
	// social_repo can pick title / description / tags / privacy /
	// publish / etc. without Velox having to grow new fields every
	// time the social_repo adds a feature. map[string]any keeps the
	// JSON encoding close to what the operator authored.
	Metadata map[string]any `json:"metadata,omitempty"`

	// PublishAt is forwarded only if non-empty so the social_repo can
	// schedule the publish outside Velox's loop. RFC3339 string.
	PublishAt string `json:"publish_at,omitempty"`

	// CallbackURL is the per-delivery webhook the social_repo will POST
	// to when the publish completes. Empty omits the field.
	CallbackURL string `json:"callback_url,omitempty"`

	// Deprecated: the three YouTube-specific fields have been replaced
	// by SocialDestinationID (single opaque reference, social-repo
	// resolved). They are kept here with `json:"-"` for one commit so
	// that the provider's buildRequest call sites continue to compile
	// during the ABI-safe transition. They are NOT serialised into the
	// wire JSON (transport logs will not show legacy keys).
	//
	// These fields will be REMOVED entirely in the next atomic commit
	// (provider-cleanup: drop parsePlatformAndAccount + these typed
	// fields). After that, the wire JSON carries ONLY
	// external_delivery_id / idempotency_key / social_destination_id /
	// artifact / metadata / publish_at / callback_url.
	//
	// Deprecated: removed in Residuo 3 provider-cleanup commit.
	Platform  string `json:"-"`
	AccountID string `json:"-"`
	ChannelID string `json:"-"`
}

// ArtifactPayload is the typed view of the artifact reference inside
// DeliverArtifactRequest. All fields are required except DownloadURL,
// which is set when Velox's CallbackBaseURL is configured (the social_repo
// will then download the bytes via this signed URL rather than
// requiring Velox to push again on the delivery callback).
type ArtifactPayload struct {
	ArtifactID  string `json:"artifact_id"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	MimeType    string `json:"mime_type"`
	DownloadURL string `json:"download_url,omitempty"`
}

// DeliverArtifactResponse is the typed view of the social_repo's
// response. The runner persists SocialDeliveryID on
// `job_deliveries.remote_id` so operators can correlate a Velox
// delivery row with the social_repo's record by ID alone.
type DeliverArtifactResponse struct {
	SocialDeliveryID string `json:"social_delivery_id"`
	Status           string `json:"status"`
}
