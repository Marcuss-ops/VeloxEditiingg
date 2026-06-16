package youtube

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

// AddChannelHandler adds a channel to a group
func (ym *YouTubeManager) AddChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")

		var req youtube.AddChannelRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid request: " + err.Error(),
			})
			return
		}

		url := strings.TrimSpace(req.URL)
		if url == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "URL cannot be empty",
			})
			return
		}

		if _, ok := ym.storage.GetGroup(groupName); !ok {
			c.JSON(http.StatusNotFound, youtube.APIResponse{
				OK:    false,
				Error: "Group not found",
			})
			return
		}

		if isVideoURL(url) {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid Channel URL. Please provide a channel URL or handle (@name).",
			})
			return
		}

		ctx := c.Request.Context()
		channelInfo, err := ym.apiClient.GetChannelInfo(ctx, url)
		if err != nil {
			channelInfo = &youtube.ChannelInfo{URL: url, Title: req.Title, Thumbnail: req.Thumbnail}
		}

		channelTitle := req.Title
		channelThumbnail := req.Thumbnail
		resolvedURL := url

		if channelInfo != nil && channelInfo.URL != "" {
			resolvedURL = channelInfo.URL
			if channelInfo.Title != "" {
				channelTitle = channelInfo.Title
			}
			if channelInfo.Thumbnail != "" {
				channelThumbnail = channelInfo.Thumbnail
			}
		}

		if isVideoURL(resolvedURL) {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid Channel URL resolved. Please provide a channel URL or handle (@name).",
			})
			return
		}

		var keywords []string
		if channelTitle != "" {
			keywords = extractKeywords(channelTitle)
		}
		if channelInfo != nil && channelInfo.Description != "" {
			keywords = append(keywords, extractKeywords(channelInfo.Description)...)
			seen := make(map[string]bool)
			var unique []string
			for _, k := range keywords {
				if !seen[k] && len(unique) < 10 {
					seen[k] = true
					unique = append(unique, k)
				}
			}
			keywords = unique
		}

		channel := youtube.Channel{
			ID:        strconv.FormatInt(time.Now().UnixMilli(), 10),
			URL:       resolvedURL,
			Title:     channelTitle,
			Thumbnail: channelThumbnail,
			Notes:     req.Notes,
			AddedAt:   time.Now(),
			Keywords:  keywords,
		}

		if err := ym.storage.AddChannel(groupName, channel); err != nil {
			if err == youtube.ErrChannelExists {
				c.JSON(http.StatusConflict, youtube.APIResponse{
					OK:    false,
					Error: "Channel already in group",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Channel added",
			Data:    channel,
		})
	}
}
