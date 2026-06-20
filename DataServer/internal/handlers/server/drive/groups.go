package drive

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetDriveGroupsHandler builds group structure (clip/stock/voiceover) grouped by language
func (h *DriveHandlers) GetDriveGroupsHandler(c *gin.Context) {
	groups, err := h.svc.GetDriveGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"groups":  groups,
		"count":   len(groups),
	})
}

// GetDriveFoldersHandler returns master folders OR children of a specific parent
func (h *DriveHandlers) GetDriveFoldersHandler(c *gin.Context) {
	parentID := c.Query("parent_id")
	folders, err := h.svc.GetDriveFolders(parentID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: folders,
		Count:   len(folders),
	})
}

// GroupFoldersHandler returns clip/stock/voiceover folder IDs for a given group name
func (h *DriveHandlers) GroupFoldersHandler(c *gin.Context) {
	groupName := c.Param("group_name")
	result, err := h.svc.GroupFolders(groupName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	resMap := gin.H{
		"success": true,
		"group":   groupName,
	}
	for k, v := range result {
		resMap[k] = v
	}

	c.JSON(http.StatusOK, resMap)
}

// ClipFolderIDHandler returns the clip folder ID for a given folder_name or group
func (h *DriveHandlers) ClipFolderIDHandler(c *gin.Context) {
	folderName := c.Query("folder_name")
	group := c.Query("group")

	res, err := h.svc.ClipFolderID(folderName, group)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	resMap := gin.H{
		"success": true,
	}
	for k, v := range res {
		resMap[k] = v
	}

	c.JSON(http.StatusOK, resMap)
}

// DriveFilesHandler lists subfolder items under a parent_id (folders only)
func (h *DriveHandlers) DriveFilesHandler(c *gin.Context) {
	parentID := c.Query("parent_id")
	children, err := h.svc.DriveFiles(parentID)
	if err != nil {
		status := http.StatusInternalServerError
		if err.Error() == "parent_id required" {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: children,
		Count:   len(children),
	})
}
