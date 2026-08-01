package pipeline

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/apiwire"
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
		if h.store == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "destination_store_not_configured"})
			return
		}

		catalog, err := h.socialClient.ListPublishingCatalog(c.Request.Context(), req.WorkspaceID, req.Platform)
		if err != nil {
			status, code := publishingCatalogUpstreamError(err)
			c.AbortWithStatusJSON(status, gin.H{"error": code, "message": err.Error()})
			return
		}

		channels, externalIDs, err := projectCatalogChannels(catalog.ResolvedTargets, req.Platform)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{
				"error":   "social_catalog_invalid",
				"message": err.Error(),
			})
			return
		}
		groups, err := projectCatalogGroups(catalog)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{
				"error":   "social_catalog_invalid",
				"message": err.Error(),
			})
			return
		}

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

func projectCatalogChannels(targets []socialclient.PublishingTarget, platform string) ([]PublishingCatalogChannel, []string, error) {
	channels := make([]PublishingCatalogChannel, 0, len(targets))
	externalIDs := make([]string, 0, len(targets))
	seenExternalIDs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target.Platform != "" && target.Platform != platform {
			return nil, nil, errors.New("catalog target platform does not match request")
		}
		if target.PlatformAccountID <= 0 || strings.TrimSpace(target.ChannelID) == "" || strings.TrimSpace(target.ChannelName) == "" || strings.TrimSpace(target.Status) == "" {
			return nil, nil, errors.New("catalog channel fields are incomplete")
		}
		externalID := strings.TrimSpace(target.ExternalDestinationID)
		if externalID == "" {
			return nil, nil, errors.New("catalog channel is missing external_destination_id")
		}
		if _, duplicate := seenExternalIDs[externalID]; duplicate {
			return nil, nil, errors.New("catalog contains duplicate external_destination_id")
		}
		seenExternalIDs[externalID] = struct{}{}

		destinationID := veloxDestinationID(externalID)
		channels = append(channels, PublishingCatalogChannel{
			Type:              "channel",
			DestinationID:     destinationID,
			PlatformAccountID: target.PlatformAccountID,
			ChannelID:         target.ChannelID,
			Name:              target.ChannelName,
			Status:            target.Status,
			Enabled:           target.Enabled && target.CanPost,
			CanPost:           target.CanPost,
			Capabilities: apiwire.PublishingCatalogCapabilities{
				UploadVideo:  target.Capabilities.UploadVideo,
				SetThumbnail: target.Capabilities.SetThumbnail,
				Publish:      target.Capabilities.Publish,
				Schedule:     target.Capabilities.Schedule,
			},
			BlockReason:     target.BlockReason,
			TargetErrorCode: target.TargetErrorCode,
		})
		externalIDs = append(externalIDs, externalID)
	}
	return channels, externalIDs, nil
}

func projectCatalogGroups(catalog *socialclient.PublishingTargetCatalogResponse) ([]PublishingCatalogGroup, error) {
	if catalog == nil {
		return []PublishingCatalogGroup{}, nil
	}
	all := make([]socialclient.PublishingGroup, 0, len(catalog.ResolvedGroups)+len(catalog.Groups))
	all = append(all, catalog.ResolvedGroups...)
	all = append(all, catalog.Groups...)
	groups := make([]PublishingCatalogGroup, 0, len(all))
	seen := make(map[int64]socialclient.PublishingGroup, len(all))
	for _, group := range all {
		if group.GroupID <= 0 {
			return nil, errors.New("catalog group_id must be positive")
		}
		if previous, duplicate := seen[group.GroupID]; duplicate {
			if previous.Name == group.Name &&
				sameOptionalInt64(previous.ParentGroupID, group.ParentGroupID) &&
				previous.MemberCount == group.MemberCount &&
				previous.PublishableMemberCount == group.PublishableMemberCount &&
				previous.Status == group.Status &&
				previous.CanPost == group.CanPost &&
				previous.BlockReason == group.BlockReason &&
				previous.TargetErrorCode == group.TargetErrorCode {
				continue
			}
			return nil, errors.New("catalog contains conflicting duplicate group_id")
		}
		if strings.TrimSpace(group.Name) == "" {
			return nil, errors.New("catalog group name is required")
		}
		if group.MemberCount < 0 || group.PublishableMemberCount < 0 || group.PublishableMemberCount > group.MemberCount {
			return nil, errors.New("catalog group member counts are invalid")
		}
		seen[group.GroupID] = group
		groups = append(groups, PublishingCatalogGroup{
			Type:                   "group",
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
	return groups, nil
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
