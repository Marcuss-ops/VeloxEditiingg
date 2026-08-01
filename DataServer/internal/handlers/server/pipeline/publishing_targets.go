package pipeline

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	targetpublishing "velox-server/internal/publishing"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// PublishingTargetsRequest is the M2M discovery request used by systems that
// submit jobs to Velox. WorkspaceID scopes the account/channel pool;
// PlatformAccountID optionally narrows the response to one linked channel.
type PublishingTargetsRequest struct {
	WorkspaceID       int64  `json:"workspace_id"`
	Platform          string `json:"platform"`
	PlatformAccountID int64  `json:"platform_account_id,omitempty"`
}

// PublishingTargetResponse adds the Velox destination_id generated from the
// opaque InstaEdit external_destination_id. This destination_id is ready to be
// copied verbatim into POST /api/v1/jobs delivery_plan[].destination_id.
type PublishingTargetResponse struct {
	socialclient.PublishingTarget
	DestinationID string `json:"destination_id,omitempty"`
}

// PublishingCatalogError is the optional TOP-LEVEL error block
// added when the InstaeditLogin catalog yielded AT LEAST ONE
// entry but ZERO entries satisfied the publishable predicate
// (can_post=true AND capabilities.upload_video=true). Distinct
// from the per-row target_error_code (which explains WHY one
// specific entry is rejected), this top-level block answers
// WHAT THE PRODUCER SHOULD DO when no row is usable.
//
// §0.3.4 item 4 split (NIT-2): this error code is the
// catalog-side sibling of BLOCKED_VELOX_DISABLED, which is
// emitted on the enqueue-side (POST /api/v1/jobs pre-flight) when
// the producer-picked destination_id is globally disabled on
// Velox. Together they preserve diagnostic granularity in
// operator dashboards. The field is OPTIONAL in the response
// envelope (omitempty) so existing senders that read .targets
// continue to work unchanged.
type PublishingCatalogError struct {
	Code        string `json:"code"`
	BlockReason string `json:"block_reason,omitempty"`
	WorkspaceID int64  `json:"workspace_id,omitempty"`
	Platform    string `json:"platform,omitempty"`
}

type PublishingTargetsResponse struct {
	WorkspaceID int64                      `json:"workspace_id"`
	Platform    string                     `json:"platform"`
	Targets     []PublishingTargetResponse `json:"targets"`
	Error       *PublishingCatalogError    `json:"error,omitempty"`
}

// ListPublishingTargets implements POST /api/v1/publishing/targets.
//
// Flow:
//  1. discover canonical targets from InstaEdit;
//  2. upsert a local social_gateway destination for each opaque destination;
//  3. return the Velox destination_id to the trusted job sender.
//
// No OAuth or provider credential is returned or persisted in job payloads.
func (h *Handlers) ListPublishingTargets() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req PublishingTargetsRequest
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_json",
				"message": err.Error(),
			})
			return
		}
		if req.WorkspaceID <= 0 {
			c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
				"error":   "invalid_payload",
				"details": []gin.H{{"path": "workspace_id", "issue": "must_be_positive"}},
			})
			return
		}
		req.Platform = strings.ToLower(strings.TrimSpace(req.Platform))
		if req.Platform == "" {
			req.Platform = "youtube"
		}
		if req.Platform != "youtube" {
			c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
				"error":   "invalid_payload",
				"details": []gin.H{{"path": "platform", "issue": "unsupported_value", "allowed": []string{"youtube"}}},
			})
			return
		}
		if h.socialClient == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "social_api_not_configured"})
			return
		}
		if h.store == nil || h.targetResolver == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "destination_store_not_configured"})
			return
		}

		resolved, err := h.targetResolver.ResolveCatalog(c.Request.Context(), targetpublishing.CatalogRequest{
			WorkspaceID: req.WorkspaceID,
			Platform:    req.Platform,
		})
		if err != nil {
			status, code := publishingCatalogUpstreamError(err)
			if !errors.Is(err, socialclient.ErrNotConfigured) && !errors.Is(err, socialclient.ErrAuth) && !errors.Is(err, socialclient.ErrRateLimit) && !errors.Is(err, socialclient.ErrPermanent) {
				status = http.StatusBadGateway
				code = "social_catalog_invalid"
			}
			c.AbortWithStatusJSON(status, gin.H{"error": code, "message": err.Error()})
			return
		}
		response := PublishingTargetsResponse{
			WorkspaceID: req.WorkspaceID,
			Platform:    req.Platform,
			Targets:     []PublishingTargetResponse{},
		}
		syncedDestinationIDs := make([]string, 0, len(resolved.Channels))
		for _, channel := range resolved.Channels {
			target := socialclient.PublishingTarget{
				WorkspaceID:           channel.WorkspaceID,
				PlatformAccountID:     channel.PlatformAccountID,
				Platform:              channel.Platform,
				ChannelID:             channel.ChannelID,
				ChannelName:           channel.Name,
				ExternalDestinationID: channel.ExternalDestinationID,
				Status:                channel.Status,
				Enabled:               channel.Eligible,
				CanPost:               channel.CanPost,
				BlockReason:           channel.BlockReason,
				TargetErrorCode:       channel.TargetErrorCode,
				Capabilities: socialclient.PublishingCapabilities{
					UploadVideo:  channel.Capabilities.UploadVideo,
					SetThumbnail: channel.Capabilities.SetThumbnail,
					Publish:      channel.Capabilities.Publish,
					Schedule:     channel.Capabilities.Schedule,
				},
			}
			out := PublishingTargetResponse{PublishingTarget: target}
			if target.ExternalDestinationID != "" {
				out.DestinationID = veloxDestinationID(target.ExternalDestinationID)
				syncedDestinationIDs = append(syncedDestinationIDs, out.DestinationID)
				configuration, marshalErr := json.Marshal(map[string]any{
					"source":                  "instaedit_catalog",
					"workspace_id":            req.WorkspaceID,
					"platform":                target.Platform,
					"platform_account_id":     target.PlatformAccountID,
					"channel_id":              target.ChannelID,
					"external_destination_id": target.ExternalDestinationID,
				})
				if marshalErr != nil {
					c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "destination_configuration_failed"})
					return
				}
				if upsertErr := h.store.UpsertSyncedDeliveryDestination(c.Request.Context(), store.DeliveryDestination{
					DestinationID:         out.DestinationID,
					Provider:              targetpublishing.ProviderSocialGateway,
					ExternalDestinationID: target.ExternalDestinationID,
					Name:                  target.ChannelName,
					Enabled:               channel.Eligible,
					ConfigurationJSON:     string(configuration),
				}); upsertErr != nil {
					c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
						"error":   "destination_sync_failed",
						"message": upsertErr.Error(),
					})
					return
				}
			}
			response.Targets = append(response.Targets, out)
		} // close for-loop

		// A full catalog refresh is authoritative. Disable stale local rows
		// without deleting them, so historical deliveries remain auditable.
		// A filtered request must not disable channels outside its filter.
		if req.PlatformAccountID == 0 {
			if err := h.store.DisableMissingSyncedDeliveryDestinations(c.Request.Context(), syncedDestinationIDs); err != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error":   "destination_sync_failed",
					"message": err.Error(),
				})
				return
			}
		}

		// §0.3.4 item 4 split (NIT-2): if the catalog yielded AT
		// LEAST ONE entry but NONE of the kept (post-platform-filter)
		// entries satisfy the publishable predicate
		// (can_post=true AND capabilities.upload_video=true), surface a
		// top-level error.code=BLOCKED_NO_PUBLISHABLE_CHANNEL.
		//
		// This is the catalog-side sibling of BLOCKED_VELOX_DISABLED
		// (which publishing_targets.go does NOT emit; it is owned by
		// job_submit.go's pre-flight only). Keeping the two split out
		// preserves operator-dashboard diagnostic granularity.
		//
		// Empty ResolvedTargets (catalog returned no rows at all) is a
		// silently-fine 200 with targets:[] — that case is the
		// InstaEdit-side "this workspace/platform has nothing bound"
		// verdict, which the producer should treat as "no channel
		// exists to even attempt"; surfacing BLOCKED_NO_PUBLISHABLE_CHANNEL
		// there would conflate it with "channels exist but none are
		// usable". Leaving targets:[] is correct.
		if len(response.Targets) > 0 {
			publishableCount := 0
			for _, t := range response.Targets {
				if t.CanPost && t.Capabilities.UploadVideo {
					publishableCount++
				}
			}
			if publishableCount == 0 {
				response.Error = &PublishingCatalogError{
					Code:        BlockedCodeNoPublishableChannel,
					BlockReason: "no target with can_post=true AND capabilities.upload_video=true for the requested workspace/platform",
					WorkspaceID: req.WorkspaceID,
					Platform:    req.Platform,
				}
			}
		}

		c.JSON(http.StatusOK, response)
	}
}

// veloxDestinationID maps an opaque InstaEdit destination to one stable Velox
// registry key. The prefix keeps provider namespaces explicit and the mapping
// is reversible for diagnostics.
func veloxDestinationID(externalDestinationID string) string {
	return targetpublishing.DestinationIDForExternal(externalDestinationID)
}
