package pipeline

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

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

type PublishingTargetsResponse struct {
	WorkspaceID int64                      `json:"workspace_id"`
	Platform    string                     `json:"platform"`
	Targets     []PublishingTargetResponse `json:"targets"`
}

// ListPublishingTargets implements POST /api/v1/publishing/targets.
//
// Flow:
//   1. discover canonical targets from InstaEdit;
//   2. upsert a local social_gateway destination for each opaque destination;
//   3. return the Velox destination_id to the trusted job sender.
//
// No OAuth or provider credential is returned or persisted in job payloads.
func (h *Handlers) ListPublishingTargets() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req PublishingTargetsRequest
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "invalid_json",
				"message": err.Error(),
			})
			return
		}
		if req.WorkspaceID <= 0 {
			c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
				"error": "invalid_payload",
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
				"error": "invalid_payload",
				"details": []gin.H{{"path": "platform", "issue": "unsupported_value", "allowed": []string{"youtube"}}},
			})
			return
		}
		if h.socialClient == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "social_api_not_configured"})
			return
		}
		if h.store == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "destination_store_not_configured"})
			return
		}

		catalog, err := h.socialClient.ListPublishingTargets(c.Request.Context(), req.WorkspaceID, req.Platform)
		if err != nil {
			status := http.StatusBadGateway
			code := "social_api_failure"
			switch {
			case errors.Is(err, socialclient.ErrNotConfigured):
				status = http.StatusServiceUnavailable
				code = "social_api_not_configured"
			case errors.Is(err, socialclient.ErrAuth):
				status = http.StatusBadGateway
				code = "social_api_auth_failed"
			case errors.Is(err, socialclient.ErrRateLimit):
				status = http.StatusTooManyRequests
				code = "social_api_rate_limited"
			case errors.Is(err, socialclient.ErrPermanent):
				status = http.StatusUnprocessableEntity
				code = "social_target_catalog_rejected"
			}
			c.AbortWithStatusJSON(status, gin.H{"error": code, "message": err.Error()})
			return
		}

		response := PublishingTargetsResponse{
			WorkspaceID: req.WorkspaceID,
			Platform:    req.Platform,
			Targets:     []PublishingTargetResponse{},
		}
		for _, target := range catalog.ResolvedTargets {
			if req.PlatformAccountID > 0 && target.PlatformAccountID != req.PlatformAccountID {
				continue
			}
			out := PublishingTargetResponse{PublishingTarget: target}
			if target.ExternalDestinationID != "" {
				out.DestinationID = veloxDestinationID(target.ExternalDestinationID)
				configuration, marshalErr := json.Marshal(map[string]any{
					"source":                "instaedit_catalog",
					"workspace_id":          req.WorkspaceID,
					"platform":              target.Platform,
					"platform_account_id":   target.PlatformAccountID,
					"channel_id":            target.ChannelID,
					"external_destination_id": target.ExternalDestinationID,
				})
				if marshalErr != nil {
					c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "destination_configuration_failed"})
					return
				}
				if upsertErr := h.store.UpsertSyncedDeliveryDestination(c.Request.Context(), store.DeliveryDestination{
					DestinationID:         out.DestinationID,
					Provider:              "social_gateway",
					ExternalDestinationID: target.ExternalDestinationID,
					Name:                  target.ChannelName,
					Enabled:               target.CanPost,
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
		}

		c.JSON(http.StatusOK, response)
	}
}

// veloxDestinationID maps an opaque InstaEdit destination to one stable Velox
// registry key. The prefix keeps provider namespaces explicit and the mapping
// is reversible for diagnostics.
func veloxDestinationID(externalDestinationID string) string {
	return "instaedit_" + strings.TrimSpace(externalDestinationID)
}
