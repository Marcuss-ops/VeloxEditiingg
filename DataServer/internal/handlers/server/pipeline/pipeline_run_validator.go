package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/store"
)

// ValidationConfig holds the tunable limits for pipeline run requests.
// Zero values mean "no limit" — the handler uses DefaultValidationConfig
// in production; tests can override individual fields.
type ValidationConfig struct {
	MaxSceneCount   int           // 0 = unlimited
	MaxPayloadBytes int           // 0 = unlimited
	MaxPublishLead  time.Duration // 0 = no max (publish_at can be arbitrarily far in the future)
	MinPublishLead  time.Duration // 0 = no min (publish_at can be now)
}

// DefaultValidationConfig returns the production validation limits.
func DefaultValidationConfig() ValidationConfig {
	return ValidationConfig{
		MaxSceneCount:   100,
		MaxPayloadBytes: 512 * 1024,           // 512 KB JSON payload
		MinPublishLead:  1 * time.Minute,      // publish_at must be at least 1 min in the future
		MaxPublishLead:  365 * 24 * time.Hour, // ... and at most 1 year
	}
}

// supportedVideoFormats is the canonical set of video output formats
// the rendering pipeline accepts. The check is case-insensitive.
var supportedVideoFormats = map[string]bool{
	"mp4":  true,
	"webm": true,
	"mov":  true,
	"mkv":  true,
	"avi":  true,
}

// ValidateCreateRequest validates a CreatePipelineRunRequest against the
// business rules BEFORE any remote call is made. It checks:
//
//  1. idempotency_key is non-empty (already enforced by the handler, but
//     defence-in-depth).
//  2. At least one delivery destination is specified and each destination
//     exists + is enabled in delivery_destinations.
//  3. When publish_at is specified, it parses as a valid RFC3339
//     timestamp with a valid timezone, and is in the future (within the
//     configured min/max lead window).
//  5. output.format (when specified) is a supported video format.
//  7. generation.scene_count is within the configured maximum.
//  8. The serialized request payload does not exceed the max payload size.
//
// Returns nil when all checks pass, or a *ValidationError describing the
// first failure. The checks run in order so the caller sees the most
// fundamental issue first.
func ValidateCreateRequest(
	ctx context.Context,
	db *store.SQLiteStore,
	req *CreatePipelineRunRequest,
	cfg ValidationConfig,
) *ValidationError {
	if req == nil {
		return &ValidationError{Field: "request", Code: "NIL_REQUEST", Message: "request body is required"}
	}

	// 1. idempotency_key
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return &ValidationError{Field: "idempotency_key", Code: "REQUIRED", Message: "idempotency_key is required"}
	}

	// 8. Payload size (checked early so a huge payload is rejected before
	// any DB queries). Marshal the request to get the exact JSON size.
	if cfg.MaxPayloadBytes > 0 {
		raw, err := json.Marshal(req)
		if err != nil {
			return &ValidationError{
				Field:   "payload",
				Code:    "PAYLOAD_UNMARSHALABLE",
				Message: "request payload cannot be serialized",
			}
		}
		if len(raw) > cfg.MaxPayloadBytes {
			return &ValidationError{
				Field:   "payload",
				Code:    "PAYLOAD_TOO_LARGE",
				Message: fmt.Sprintf("request payload exceeds maximum size of %d bytes (got %d)", cfg.MaxPayloadBytes, len(raw)),
			}
		}
	}

	// 2. At least one delivery destination.
	if len(req.DeliveryPlan) == 0 {
		return &ValidationError{
			Field:   "delivery_plan",
			Code:    "AT_LEAST_ONE_DESTINATION",
			Message: "at least one delivery destination is required",
		}
	}

	// Validate each destination when a DB store is available.
	if db != nil {
		for i, d := range req.DeliveryPlan {
			prefix := fmt.Sprintf("delivery_plan[%d]", i)

			// Destination must have a provider.
			if strings.TrimSpace(d.Provider) == "" && strings.TrimSpace(d.Destination) == "" {
				return &ValidationError{
					Field:   prefix + ".provider",
					Code:    "REQUIRED",
					Message: "provider or destination is required",
				}
			}

			// When a destination_id is specified, check it exists + is enabled.
			if d.Destination != "" {
				dest, err := db.GetDeliveryDestination(ctx, d.Destination)
				if err != nil {
					return &ValidationError{
						Field:   prefix + ".destination",
						Code:    "DESTINATION_NOT_FOUND",
						Message: fmt.Sprintf("destination %q does not exist", d.Destination),
					}
				}
				if !dest.Enabled {
					return &ValidationError{
						Field:   prefix + ".destination",
						Code:    "DESTINATION_DISABLED",
						Message: fmt.Sprintf("destination %q is disabled", d.Destination),
					}
				}
			}

			// 3. publish_at: valid timestamp + timezone + in the future.
			if d.PublishAt != "" {
				privacy := ""
				if req.VideoMetadata != nil {
					privacy = req.VideoMetadata.PrivacyStatus
				}
				if err := validatePublishAt(d.PublishAt, privacy, cfg); err != nil {
					return &ValidationError{
						Field:   prefix + ".publish_at",
						Code:    err.Code,
						Message: err.Message,
					}
				}
			}
		}
	}

	// 6. output.format.
	if req.Output != nil && req.Output.Format != "" {
		if !supportedVideoFormats[strings.ToLower(req.Output.Format)] {
			return &ValidationError{
				Field:   "output.format",
				Code:    "UNSUPPORTED_FORMAT",
				Message: fmt.Sprintf("format %q is not supported (valid: mp4, webm, mov, mkv, avi)", req.Output.Format),
			}
		}
	}

	// 7. scene_count limit.
	if cfg.MaxSceneCount > 0 && req.Generation != nil && req.Generation.SceneCount > cfg.MaxSceneCount {
		return &ValidationError{
			Field:   "generation.scene_count",
			Code:    "SCENE_COUNT_LIMIT",
			Message: fmt.Sprintf("scene_count %d exceeds maximum of %d", req.Generation.SceneCount, cfg.MaxSceneCount),
		}
	}

	// 7b. Video dimensions sanity (when specified).
	if req.Output != nil {
		if req.Output.Width > 0 && req.Output.Height > 0 {
			if req.Output.Width > 7680 || req.Output.Height > 4320 {
				return &ValidationError{
					Field:   "output.width",
					Code:    "DIMENSION_LIMIT",
					Message: "video dimensions exceed 7680x4320 (8K)",
				}
			}
		}
		if req.Output.FPS > 0 && req.Output.FPS > 120 {
			return &ValidationError{
				Field:   "output.fps",
				Code:    "FPS_LIMIT",
				Message: "fps exceeds maximum of 120",
			}
		}
	}

	return nil
}

// validatePublishAt checks that publish_at is a valid RFC3339 timestamp
// with a timezone, and that it falls within the configured min/max lead
// window relative to now.
func validatePublishAt(publishAt string, privacy string, cfg ValidationConfig) *internalValidationError {
	ts, err := time.Parse(time.RFC3339, publishAt)
	if err != nil {
		return &internalValidationError{
			Code:    "INVALID_TIMESTAMP",
			Message: fmt.Sprintf("publish_at %q is not a valid RFC3339 timestamp: %v", publishAt, err),
		}
	}

	// Timezone: RFC3339 requires a timezone offset ("Z" or "+HH:MM"),
	// so a successful Parse guarantees one. No explicit check needed.

	now := time.Now().UTC()
	utcTs := ts.UTC()

	// publish_at must be in the future (min lead).
	if cfg.MinPublishLead > 0 {
		minAllowed := now.Add(cfg.MinPublishLead)
		if utcTs.Before(minAllowed) {
			return &internalValidationError{
				Code:    "PUBLISH_AT_TOO_SOON",
				Message: fmt.Sprintf("publish_at %s must be at least %s in the future", publishAt, cfg.MinPublishLead),
			}
		}
	}

	// publish_at must not be too far in the future (max lead).
	if cfg.MaxPublishLead > 0 {
		maxAllowed := now.Add(cfg.MaxPublishLead)
		if utcTs.After(maxAllowed) {
			return &internalValidationError{
				Code:    "PUBLISH_AT_TOO_FAR",
				Message: fmt.Sprintf("publish_at %s must be at most %s in the future", publishAt, cfg.MaxPublishLead),
			}
		}
	}

	// Privacy/publish_at compatibility: when publish_at is set, the
	// privacy status must be "private" or "scheduled" — YouTube does
	// not honor scheduling for "public" or "unlisted" videos.
	if privacy != "" {
		p := strings.ToLower(privacy)
		if p == "public" || p == "unlisted" {
			return &internalValidationError{
				Code:    "PRIVACY_INCOMPATIBLE_WITH_SCHEDULE",
				Message: fmt.Sprintf(`privacy_status %q is incompatible with publish_at — use "private" or "scheduled"`, privacy),
			}
		}
	}

	return nil
}

// Note: the executor registrato check from the spec is intentionally
// deferred — the executor is determined server-side by the Resolver
// based on the remote engine's response, not by the client request.
// Adding an executor field to the request would couple the client API
// to the internal routing topology, which the versioned contract is
// designed to avoid.
