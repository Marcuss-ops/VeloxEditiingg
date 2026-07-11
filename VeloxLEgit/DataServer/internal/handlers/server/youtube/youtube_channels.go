package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/youtube"
)

// AddChannelHandler adds a channel to a group.
func (ym *YouTubeManager) AddChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		var req youtube.AddChannelRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Invalid request: " + err.Error()})
			return
		}

		ctx := c.Request.Context()
		channel, err := ym.svc.AddChannelToGroup(ctx, groupName, req)
		if err != nil {
			if err == youtube.ErrChannelExists {
				c.JSON(http.StatusConflict, youtube.APIResponse{OK: false, Error: "Channel already in group"})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Message: "Channel added", Data: channel})
	}
}

// DeleteChannelHandler removes a channel from a group.
func (ym *YouTubeManager) DeleteChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		channelID := c.Param("channel_id")

		if err := ym.svc.RemoveChannelFromGroup(groupName, channelID); err != nil {
			if err == youtube.ErrGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{OK: false, Error: "Group not found"})
				return
			}
			if err == youtube.ErrChannelNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{OK: false, Error: "Channel not found in group"})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Message: "Channel removed"})
	}
}

// MoveChannelHandler moves a channel between groups.
func (ym *YouTubeManager) MoveChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		sourceGroup := c.Param("group_name")
		channelID := c.Param("channel_id")

		var req youtube.MoveChannelRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Invalid request: " + err.Error()})
			return
		}

		if err := ym.svc.MoveChannel(sourceGroup, channelID, req.TargetGroup); err != nil {
			if err == youtube.ErrGroupNotFound || err == youtube.ErrTargetGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{OK: false, Error: "Source or target group not found"})
				return
			}
			if err == youtube.ErrChannelNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{OK: false, Error: "Channel not found in source group"})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Message: "Moved to " + req.TargetGroup})
	}
}

// RefreshChannelStatsHandler updates stats for a channel.
func (ym *YouTubeManager) RefreshChannelStatsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		channelID := c.Param("channel_id")

		ctx := c.Request.Context()
		channel, err := ym.svc.RefreshChannelStats(ctx, groupName, channelID)
		if err != nil {
			status := http.StatusInternalServerError
			if err.Error() == "group not found" || err.Error() == "channel not found" {
				status = http.StatusNotFound
			}
			c.JSON(status, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Data: channel})
	}
}

// MoveChannelToGroupHandler moves a channel to a target group (drag & drop).
func (ym *YouTubeManager) MoveChannelToGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("channel_id")
		targetGroup := c.Param("target_group")

		ctx := c.Request.Context()
		res, err := ym.svc.MoveChannelToGroup(ctx, channelID, targetGroup)
		if err != nil {
			if err == youtube.ErrChannelExists {
				c.JSON(http.StatusConflict, youtube.APIResponse{OK: false, Error: "Channel already in group"})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: res.Message,
			Data: gin.H{
				"channel_id":   res.ChannelID,
				"source_group": res.SourceGroup,
				"target_group": res.TargetGroup,
			},
		})
	}
}
