package darkeditor

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
)

// Folder handlers
func (h *Handler) ListFolders(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Folder storage unavailable"})
		return
	}
	ctx := c.Request.Context()

	folders, err := h.store.ListFolders(ctx)
	if err != nil {
		log.Printf("❌ ListFolders: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list folders"})
		return
	}

	response := make([]Folder, len(folders))
	for i, f := range folders {
		response[i] = mapStoreFolder(f)
	}

	c.JSON(http.StatusOK, response)
}

func (h *Handler) GetFolder(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Folder storage unavailable"})
		return
	}
	ctx := c.Request.Context()
	folderID := c.Param("id")

	folder, err := h.store.GetFolder(ctx, folderID)
	if err != nil {
		log.Printf("❌ GetFolder: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve folder"})
		return
	}
	if folder == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Folder not found"})
		return
	}

	c.JSON(http.StatusOK, mapStoreFolder(folder))
}

func (h *Handler) CreateFolder(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Folder storage unavailable"})
		return
	}
	var req CreateFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	ctx := c.Request.Context()
	folder := &store.Folder{
		Name:     req.Name,
		ParentID: req.ParentID,
	}
	if err := h.store.CreateFolder(ctx, folder); err != nil {
		log.Printf("❌ CreateFolder: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create folder"})
		return
	}

	c.JSON(http.StatusOK, mapStoreFolder(folder))
}

func (h *Handler) UpdateFolder(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Folder storage unavailable"})
		return
	}
	folderID := c.Param("id")
	var req UpdateFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	ctx := c.Request.Context()
	folder, err := h.store.GetFolder(ctx, folderID)
	if err != nil {
		log.Printf("❌ UpdateFolder (fetch): %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load folder"})
		return
	}
	if folder == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Folder not found"})
		return
	}

	if req.Name != nil {
		folder.Name = *req.Name
	}
	if req.ParentID != nil {
		folder.ParentID = req.ParentID
	}

	if err := h.store.UpdateFolder(ctx, folder); err != nil {
		log.Printf("❌ UpdateFolder: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update folder"})
		return
	}

	c.JSON(http.StatusOK, mapStoreFolder(folder))
}

func (h *Handler) DeleteFolder(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Folder storage unavailable"})
		return
	}
	folderID := c.Param("id")
	ctx := c.Request.Context()

	if err := h.store.DeleteFolder(ctx, folderID); err != nil {
		log.Printf("❌ DeleteFolder: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete folder"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) AssignProjectToFolder(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Folder storage unavailable"})
		return
	}
	projectID := c.Param("id")
	var req AssignProjectFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	ctx := c.Request.Context()
	if req.FolderID != nil && *req.FolderID == "" {
		req.FolderID = nil
	}
	if err := h.store.AssignProjectFolder(ctx, projectID, req.FolderID); err != nil {
		log.Printf("❌ AssignProjectToFolder: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to assign folder"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

func mapStoreFolder(src *store.Folder) Folder {
	return Folder{
		ID:        src.ID,
		Name:      src.Name,
		ParentID:  src.ParentID,
		CreatedAt: src.CreatedAt,
		UpdatedAt: src.UpdatedAt,
	}
}

// ============== FILE SERVING ==============

// ServeTempFile serves a file from the temp directory
func (h *Handler) ServeTempFile(c *gin.Context) {
	filename := c.Param("filename")
	filePath := h.getTempPath(filename)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "File not found"})
		return
	}

	c.File(filePath)
}

// ServeProjectFile serves files from the projects directory
func (h *Handler) ServeProjectFile(c *gin.Context) {
	projectID := c.Param("id")
	filename := c.Param("filename")

	filePath := filepath.Join(h.cfg.ProjectsDir, projectID, filename)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "File not found"})
		return
	}

	c.File(filePath)
}
