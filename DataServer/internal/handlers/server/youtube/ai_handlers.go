package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GenerateTitles generates AI titles for a video
// POST /api/v1/youtube/ai/titles
func (h *YouTubeHandlers) GenerateTitles(c *gin.Context) {
	var req struct {
		FileName     string `json:"file_name"`
		Description  string `json:"description"`
		CustomPrompt string `json:"custom_prompt"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	// Generate mock titles
	titles := generateMockTitles(req.FileName, req.CustomPrompt)

	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"titles": titles,
	})
}

// GenerateDescription generates AI description for a video
// POST /api/v1/youtube/ai/description
func (h *YouTubeHandlers) GenerateDescription(c *gin.Context) {
	var req struct {
		Title        string `json:"title" binding:"required"`
		CustomPrompt string `json:"custom_prompt"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	description := generateMockDescription(req.Title, req.CustomPrompt)

	c.JSON(http.StatusOK, gin.H{
		"ok":          true,
		"description": description,
	})
}

// GenerateTags generates AI tags for a video
// POST /api/v1/youtube/ai/tags
func (h *YouTubeHandlers) GenerateTags(c *gin.Context) {
	var req struct {
		Title        string `json:"title" binding:"required"`
		Description  string `json:"description"`
		CustomPrompt string `json:"custom_prompt"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	tags := generateMockTags(req.Title, req.CustomPrompt)

	c.JSON(http.StatusOK, gin.H{
		"ok":   true,
		"tags": tags,
	})
}
