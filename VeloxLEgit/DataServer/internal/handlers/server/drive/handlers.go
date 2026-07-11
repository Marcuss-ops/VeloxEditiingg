package drive

import (
	"net/http"

	"github.com/gin-gonic/gin"

	driveSvc "velox-server/internal/services/drive"
)

// ListDriveTokensHandler lists all Drive token files
func (h *DriveHandlers) ListDriveTokensHandler(c *gin.Context) {
	files, err := h.svc.ListDriveTokens()
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"files": []driveSvc.TokenFile{}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"files": files})
}

// GetDriveLinksHandler returns all drive links
func (h *DriveHandlers) GetDriveLinksHandler(c *gin.Context) {
	folders := h.svc.GetDriveLinks()
	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: folders,
		Count:   len(folders),
	})
}

// GetDriveLinksByGroupHandler returns drive links for a specific group
func (h *DriveHandlers) GetDriveLinksByGroupHandler(c *gin.Context) {
	groupName := c.Param("group_name")
	folders := h.svc.GetDriveLinksByGroup(groupName)
	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: folders,
		Count:   len(folders),
	})
}

// GetMasterFoldersHandler returns master folders
func (h *DriveHandlers) GetMasterFoldersHandler(c *gin.Context) {
	masters, err := h.svc.GetMasterFolders()
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"masters": gin.H{}}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"masters": masters}})
}

// UpsertMasterFolderHandler creates or updates a master folder entry.
func (h *DriveHandlers) UpsertMasterFolderHandler(c *gin.Context) {
	var req driveSvc.UpsertMasterFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	if req.ID == "" || req.Name == "" || req.URL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "id, name and url are required"})
		return
	}

	if err := h.svc.UpsertMasterFolder(req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"id":      req.ID,
		"name":    req.Name,
		"url":     req.URL,
	})
}

// SaveDriveLinksHandler replaces all drive links (bulk save)
func (h *DriveHandlers) SaveDriveLinksHandler(c *gin.Context) {
	var req driveSvc.SaveDriveLinksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	if err := h.svc.SaveDriveLinks(req.Folders); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"count":   len(req.Folders),
	})
}

// AddDriveFolderHandler adds or updates a single folder
func (h *DriveHandlers) AddDriveFolderHandler(c *gin.Context) {
	var req driveSvc.AddDriveFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	id, err := h.svc.AddDriveFolder(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "folder_id": id})
}

// UpdateDriveFolderHandler updates a folder by ID
func (h *DriveHandlers) UpdateDriveFolderHandler(c *gin.Context) {
	folderID := c.Param("folder_id")

	var req driveSvc.UpdateDriveFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	if err := h.svc.UpdateDriveFolder(folderID, req); err != nil {
		status := http.StatusInternalServerError
		if err.Error() == "folder not found" {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "folder_id": folderID})
}

// DeleteDriveFolderHandler deletes a folder and all its children
func (h *DriveHandlers) DeleteDriveFolderHandler(c *gin.Context) {
	folderID := c.Param("folder_id")

	deletedCount, err := h.svc.DeleteDriveFolder(folderID)
	if err != nil {
		status := http.StatusInternalServerError
		if err.Error() == "folder not found" {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"deleted": deletedCount,
	})
}

// CreateDriveFolderHandler is not implemented in production.
// Use the Google Drive web interface or API directly.
func (h *DriveHandlers) CreateDriveFolderHandler(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"success": false,
		"error":   "CreateDriveFolder is not implemented in production. Use the Google Drive web interface or API directly.",
	})
}

// UploadTextHandler is not implemented in production.
// Use the Google Drive web interface or API directly.
func (h *DriveHandlers) UploadTextHandler(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"success": false,
		"error":   "UploadText is not implemented in production. Use the Google Drive web interface or API directly.",
	})
}
