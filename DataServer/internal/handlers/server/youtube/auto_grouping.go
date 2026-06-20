package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"
	yt "velox-server/internal/integrations/youtube"
)

// AutoOrganizeUndefinedChannelsHandler groups unassigned channels into topic-based upload groups.
// POST /api/v1/youtube/manager/groups/auto-organize-undefined
func (ym *YouTubeManager) AutoOrganizeUndefinedChannelsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		res, err := ym.svc.AutoOrganizeUndefinedChannels(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, yt.APIResponse{OK: false, Error: err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":             true,
			"moved":          res.Moved,
			"skipped":        res.Skipped,
			"created_groups": res.CreatedGroups,
			"assignments":    res.Assignments,
			"undefined_left": res.UndefinedLeft,
		})
	}
}
