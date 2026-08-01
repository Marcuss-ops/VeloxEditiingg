package pipeline

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/apiwire"
	targetpublishing "velox-server/internal/publishing"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// Reuse the canonical wire DTOs so the handler response and generated
// OpenAPI schemas cannot drift apart.
type PublishingCatalogRequest = apiwire.PublishingCatalogRequest
type PublishingCatalogChannel = apiwire.PublishingCatalogChannel
type PublishingCatalogGroup = apiwire.PublishingCatalogGroup
type PublishingCatalogResponse = apiwire.PublishingCatalogResponse

// ListPublishingCatalog implements POST /api/v1/publishing/catalog. It uses
// the same M2M middleware as job submission and /publishing/targets, calls the
// authoritative InstaEdit catalog once, synchronizes concrete channels into
// Velox's destination registry, and returns group summaries without creating
// fake local delivery destinations.
func (h *Handlers) ListPublishingCatalog() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req PublishingCatalogRequest
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
				"message": "workspace_id must be positive",
			})
			return
		}
		req.Platform = strings.ToLower(strings.TrimSpace(req.Platform))
		if req.Platform == "" {
			c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
				"error":   "invalid_payload",
				"message": "platform is required",
			})
			return
		}
		if req.Platform != "youtube" {
			c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
				"error":   "invalid_payload",
				"message": "platform must be youtube",
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
			c.AbortWithStatusJSON(status, gin.H{
				"error":   code,
				"message": err.Error(),
			})
			return
		}
		projected, externalIDs := projectResolvedCatalog(resolved)
		channels := projected.Channels
		groups := projected.Groups

		// Synchronize the complete valid snapshot atomically. This prevents
		// a partial catalog refresh when one destination row fails.
		destinations := make([]store.DeliveryDestination, 0, len(channels))
		for i, channel := range channels {
			configuration, marshalErr := json.Marshal(map[string]any{
				"source":                  "instaedit_catalog",
				"workspace_id":            req.WorkspaceID,
				"platform":                req.Platform,
				"platform_account_id":     channel.PlatformAccountID,
				"channel_id":              channel.ChannelID,
				"external_destination_id": externalIDs[i],
			})
			if marshalErr != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "destination_configuration_failed"})
				return
			}
			destinations = append(destinations, store.DeliveryDestination{
				DestinationID:         channel.DestinationID,
				Provider:              "social_gateway",
				ExternalDestinationID: externalIDs[i],
				Name:                  channel.Name,
				Enabled:               channel.Enabled,
				ConfigurationJSON:     string(configuration),
			})
		}
		if err := h.store.SyncSyncedDeliveryDestinations(c.Request.Context(), destinations); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error":   "destination_sync_failed",
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, PublishingCatalogResponse{
			WorkspaceID: req.WorkspaceID,
			Platform:    req.Platform,
			Channels:    channels,
			Groups:      groups,
		})
	}
}

func publishingCatalogUpstreamError(err error) (int, string) {
	status := http.StatusBadGateway
	code := "social_api_failure"
	switch {
	case errors.Is(err, socialclient.ErrNotConfigured):
		status = http.StatusServiceUnavailable
		code = "social_api_not_configured"
	case errors.Is(err, socialclient.ErrAuth):
		code = "social_api_auth_failed"
	case errors.Is(err, socialclient.ErrRateLimit):
		status = http.StatusTooManyRequests
		code = "social_api_rate_limited"
	case errors.Is(err, socialclient.ErrPermanent):
		status = http.StatusUnprocessableEntity
		code = "social_target_catalog_rejected"
	}
	return status, code
}

func projectResolvedCatalog(catalog *targetpublishing.Catalog) (PublishingCatalogResponse, []string) {
	response := PublishingCatalogResponse{
		WorkspaceID: catalog.WorkspaceID,
		Platform:    catalog.Platform,
		Channels:    make([]PublishingCatalogChannel, 0, len(catalog.Channels)),
		Groups:      make([]PublishingCatalogGroup, 0, len(catalog.Groups)),
	}
	externalIDs := make([]string, 0, len(catalog.Channels))
	for _, channel := range catalog.Channels {
		response.Channels = append(response.Channels, PublishingCatalogChannel{
			Type:              channel.Type,
			DestinationID:     channel.DestinationID,
			PlatformAccountID: channel.PlatformAccountID,
			ChannelID:         channel.ChannelID,
			Name:              channel.Name,
			Status:            channel.Status,
			Enabled:           channel.Eligible,
			CanPost:           channel.CanPost,
			Capabilities: apiwire.PublishingCatalogCapabilities{
				UploadVideo:  channel.Capabilities.UploadVideo,
				SetThumbnail: channel.Capabilities.SetThumbnail,
				Publish:      channel.Capabilities.Publish,
				Schedule:     channel.Capabilities.Schedule,
			},
			BlockReason:     channel.BlockReason,
			TargetErrorCode: channel.TargetErrorCode,
		})
		externalIDs = append(externalIDs, channel.ExternalDestinationID)
	}
	for _, group := range catalog.Groups {
		response.Groups = append(response.Groups, PublishingCatalogGroup{
			Type:                   group.Type,
			GroupID:                group.GroupID,
			Name:                   group.Name,
			ParentGroupID:          group.ParentGroupID,
			MemberCount:            group.MemberCount,
			PublishableMemberCount: group.PublishableMemberCount,
			Status:                 group.Status,
			CanPost:                group.CanPost,
			BlockReason:            group.BlockReason,
			TargetErrorCode:        group.TargetErrorCode,
		})
	}
	return response, externalIDs
}

// These adapters keep the package-local projection tests focused while all
// validation remains owned by internal/publishing.
func projectCatalogChannels(targets []socialclient.PublishingTarget, platform string) ([]PublishingCatalogChannel, []string, error) {
	catalog, err := targetpublishing.NormalizeCatalog(targetpublishing.CatalogRequest{WorkspaceID: 1, Platform: platform}, &socialclient.PublishingTargetCatalogResponse{Valid: true, ResolvedTargets: targets})
	if err != nil {
		return nil, nil, err
	}
	response, externalIDs := projectResolvedCatalog(catalog)
	return response.Channels, externalIDs, nil
}

func projectCatalogGroups(catalog *socialclient.PublishingTargetCatalogResponse) ([]PublishingCatalogGroup, error) {
	if catalog == nil {
		return []PublishingCatalogGroup{}, nil
	}
	resolved, err := targetpublishing.NormalizeCatalog(targetpublishing.CatalogRequest{WorkspaceID: 1, Platform: targetpublishing.PlatformYouTube}, catalog)
	if err != nil {
		return nil, err
	}
	projected, _ := projectResolvedCatalog(resolved)
	return projected.Groups, nil
}
