package drive

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type outroFileItem struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	WebViewLink string `json:"web_view_link"`
	Link        string `json:"link"`
}

func isOutroMasterFolder(row map[string]any) bool {
	if row == nil {
		return false
	}
	meta := strings.ToLower(strings.TrimSpace(fmt.Sprint(row["metadata_json"])))
	if strings.Contains(meta, `"type":"outro"`) {
		return true
	}
	name := strings.ToLower(strings.TrimSpace(fmt.Sprint(row["name"])))
	return strings.Contains(name, "outro")
}

// ListOutroFoldersHandler returns all configured outro folders.
// GET /api/drive/outros
func (h *DriveHandlers) ListOutroFoldersHandler(c *gin.Context) {
	if h == nil || h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "error": "drive store not configured"})
		return
	}

	rows, err := h.store.ListMasterFoldersDetailed()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	outros := make([]map[string]any, 0)
	for _, row := range rows {
		if !isOutroMasterFolder(row) {
			continue
		}
		outros = append(outros, row)
	}

	sort.Slice(outros, func(i, j int) bool {
		return strings.Compare(strings.ToLower(fmt.Sprint(outros[i]["language"])), strings.ToLower(fmt.Sprint(outros[j]["language"]))) < 0
	})

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"count":   len(outros),
		"folders": outros,
	})
}

// GetOutroFolderContentsHandler resolves an outro folder by language and lists its files.
// GET /api/drive/outros/:language
func (h *DriveHandlers) GetOutroFolderContentsHandler(c *gin.Context) {
	if h == nil || h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "error": "drive store not configured"})
		return
	}
	if h.driveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "error": "drive service not configured"})
		return
	}

	language := strings.TrimSpace(c.Param("language"))
	if language == "" {
		language = strings.TrimSpace(c.Query("language"))
	}
	if language == "" {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "language is required"})
		return
	}

	folder, err := h.store.FindMasterFolderByLanguage(language)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}
	if folder == nil {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "outro folder not found"})
		return
	}

	folderID := strings.TrimSpace(fmt.Sprint(folder["id"]))
	if folderID == "" {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "outro folder id missing"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	files, err := h.driveService.ListFiles(ctx, folderID, 100)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"success": false, "error": err.Error(), "folder": folder})
		return
	}

	items := make([]outroFileItem, 0, len(files))
	for _, f := range files {
		link := strings.TrimSpace(f.WebViewLink)
		if link == "" && f.ID != "" {
			link = fmt.Sprintf("https://drive.google.com/file/d/%s/view", f.ID)
		}
		items = append(items, outroFileItem{
			ID:          f.ID,
			Name:        f.Name,
			WebViewLink: f.WebViewLink,
			Link:        link,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"folder":  folder,
		"count":   len(items),
		"files":   items,
	})
}

// GetOutroFolderContentsByIDHandler resolves an outro folder by folder ID or URL and lists its files.
// GET /api/drive/outros-by-id/:folder_id
func (h *DriveHandlers) GetOutroFolderContentsByIDHandler(c *gin.Context) {
	if h == nil || h.driveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "error": "drive service not configured"})
		return
	}

	folderID := strings.TrimSpace(c.Param("folder_id"))
	if folderID == "" {
		folderID = strings.TrimSpace(c.Query("folder_id"))
	}
	if folderID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "folder_id is required"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	files, err := h.driveService.ListFiles(ctx, folderID, 100)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"success": false, "error": err.Error()})
		return
	}

	items := make([]outroFileItem, 0, len(files))
	for _, f := range files {
		link := strings.TrimSpace(f.WebViewLink)
		if link == "" && f.ID != "" {
			link = fmt.Sprintf("https://drive.google.com/file/d/%s/view", f.ID)
		}
		items = append(items, outroFileItem{
			ID:          f.ID,
			Name:        f.Name,
			WebViewLink: f.WebViewLink,
			Link:        link,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"folder": gin.H{
			"id":   folderID,
			"name": folderID,
			"url":  fmt.Sprintf("https://drive.google.com/drive/folders/%s", folderID),
		},
		"count": len(items),
		"files": items,
	})
}


