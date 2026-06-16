package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

// DeleteChannelHandler removes a channel from a group
func (ym *YouTubeManager) DeleteChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		channelID := c.Param("channel_id")

		if err := ym.storage.RemoveChannel(groupName, channelID); err != nil {
			if err == youtube.ErrGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Group not found",
				})
				return
			}
			if err == youtube.ErrChannelNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Channel not found in group",
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
			Message: "Channel removed",
		})
	}
}

// MoveChannelHandler moves a channel between groups
func (ym *YouTubeManager) MoveChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		sourceGroup := c.Param("group_name")
		channelID := c.Param("channel_id")

		var req youtube.MoveChannelRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid request: " + err.Error(),
			})
			return
		}

		if err := ym.storage.MoveChannel(sourceGroup, channelID, req.TargetGroup); err != nil {
			if err == youtube.ErrGroupNotFound || err == youtube.ErrTargetGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Source or target group not found",
				})
				return
			}
			if err == youtube.ErrChannelNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Channel not found in source group",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Moved to " + req.TargetGroup,
		})
	}
}
