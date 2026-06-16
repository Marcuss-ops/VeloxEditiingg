package creative

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var titleCaser = cases.Title(language.Und)

// GenerateTitles generates AI titles for a video
// POST /api/v1/youtube/ai/titles
func (h *Handler) GenerateTitles(c *gin.Context) {
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
func (h *Handler) GenerateDescription(c *gin.Context) {
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
func (h *Handler) GenerateTags(c *gin.Context) {
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

func generateMockTitles(fileName, customPrompt string) []string {
	baseName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	baseName = strings.ReplaceAll(baseName, "_", " ")
	baseName = strings.ReplaceAll(baseName, "-", " ")

	return []string{
		fmt.Sprintf("%s - Complete Guide 2025", titleCaser.String(baseName)),
		fmt.Sprintf("How to Master %s in 10 Minutes", titleCaser.String(baseName)),
		fmt.Sprintf("%s Explained: Everything You Need to Know", titleCaser.String(baseName)),
		fmt.Sprintf("The Ultimate %s Tutorial", titleCaser.String(baseName)),
		fmt.Sprintf("%s Tips and Tricks You Need to See", titleCaser.String(baseName)),
	}
}

func generateMockDescription(title, customPrompt string) string {
	return fmt.Sprintf(`%s

In this video, we dive deep into the topic and explore all the key aspects you need to know.

📌 Timestamps:
0:00 - Introduction
2:30 - Main Content
8:45 - Key Takeaways
12:00 - Conclusion

🔔 Subscribe for more content!
💬 Leave a comment if you have any questions!

#youtube #tutorial #2025 #guide`, title)
}

func generateMockTags(title, customPrompt string) []string {
	words := strings.Fields(strings.ToLower(title))
	tags := []string{"tutorial", "how to", "guide", "2025", "tips", "tricks"}

	for _, w := range words {
		if len(w) > 3 {
			tags = append(tags, w)
		}
	}

	seen := make(map[string]bool)
	result := []string{}
	for _, t := range tags {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}

	return result[:min(10, len(result))]
}
