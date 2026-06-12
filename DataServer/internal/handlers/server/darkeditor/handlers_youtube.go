package darkeditor

import (
	"io"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
)

// YouTubeGrab extracts thumbnail from YouTube video URL
func (h *Handler) YouTubeGrab(c *gin.Context) {
	var req YouTubeGrabRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?:v=|\/)([0-9A-Za-z_-]{11}).*`),
		regexp.MustCompile(`(?:youtu\.be\/)([0-9A-Za-z_-]{11})`),
	}

	var videoID string
	for _, pattern := range patterns {
		matches := pattern.FindStringSubmatch(req.URL)
		if len(matches) > 1 {
			videoID = matches[1]
			break
		}
	}

	if videoID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Could not parse YouTube Video ID"})
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}

	const maxThumbSize = 5 * 1024 * 1024

	thumbURL := "https://img.youtube.com/vi/" + videoID + "/maxresdefault.jpg"
	resp, err := client.Get(thumbURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		thumbURL = "https://img.youtube.com/vi/" + videoID + "/hqdefault.jpg"
		resp, err = client.Get(thumbURL)
		if err != nil || resp.StatusCode != http.StatusOK {
			c.JSON(http.StatusNotFound, gin.H{"error": "Thumbnail not found"})
			return
		}
	}
	defer resp.Body.Close()

	imgData, err := io.ReadAll(io.LimitReader(resp.Body, maxThumbSize+1))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to download thumbnail"})
		return
	}
	if int64(len(imgData)) > maxThumbSize {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "Thumbnail too large"})
		return
	}

	filename := "yt_" + videoID + ".jpg"
	outputPath := h.getTempPath(filename)

	if err := os.WriteFile(outputPath, imgData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save thumbnail"})
		return
	}

	c.JSON(http.StatusOK, YouTubeGrabResponse{
		Filename: filename,
		VideoID:  videoID,
	})
}
