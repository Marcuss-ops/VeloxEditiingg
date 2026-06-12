package analytics

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func ChannelsListHandler(c *gin.Context) {
	groups := loadYouTubeGroups()
	channels := flattenChannels(groups)
	c.JSON(http.StatusOK, channels)
}

func ChannelsSimpleHandler(c *gin.Context) {
	groups := loadYouTubeGroups()
	channels := flattenChannels(groups)
	out := make([]gin.H, 0, len(channels))
	for _, ch := range channels {
		out = append(out, gin.H{
			"channelId": toStr(ch["channelId"]),
			"title":     toStr(ch["title"]),
		})
	}
	c.JSON(http.StatusOK, out)
}
