package drive

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Group-to-folder mappings
var groupToClipFolder = map[string]string{
	"wwe":    "WWE",
	"hiphop": "Hip Hop",
	"news":   "News",
	"tech":   "Tech",
}

var groupToVoiceoverFolder = map[string]string{
	"wwe":    "WWE Voice",
	"hiphop": "Hip Hop Voice",
	"news":   "News Voice",
	"tech":   "Tech Voice",
}

var stockFolderAliases = map[string]string{
	"wwe":    "WWE Stock",
	"hiphop": "Hip Hop Stock",
	"news":   "News Stock",
	"tech":   "Tech Stock",
}

// findMasterIDByName finds a master folder (ParentID=="") whose name matches any in names[]
func findMasterIDByName(folders []DriveFolder, names []string) string {
	for _, name := range names {
		normName := normalizeName(name)
		for _, f := range folders {
			if f.ParentID == "" && normalizeName(f.Name) == normName {
				return f.ID
			}
		}
	}
	return ""
}

// resolveDriveFolderID finds matching cache folder and returns its ID
func resolveDriveFolderID(folders []DriveFolder, folderID string) string {
	if len(folderID) > 15 {
		for _, f := range folders {
			if f.Link == folderID || f.ID == folderID {
				return f.ID
			}
		}
	}
	return folderID
}

// GetDriveGroupsHandler builds group structure (clip/stock/voiceover) grouped by language
func (h *DriveHandlers) GetDriveGroupsHandler(c *gin.Context) {
	folders := h.getLinks()

	groups := make(map[string]interface{})

	for group, clipName := range groupToClipFolder {
		clipID := findMasterIDByName(folders, []string{clipName, group})
		voiceoverID := findMasterIDByName(folders, []string{groupToVoiceoverFolder[group], group + " Voice"})
		stockID := findMasterIDByName(folders, []string{stockFolderAliases[group], group + " Stock"})

		if clipID != "" || voiceoverID != "" || stockID != "" {
			groups[group] = gin.H{
				"clip":      clipID,
				"voiceover": voiceoverID,
				"stock":     stockID,
			}
		}
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
	folders := h.getLinks()

	if parentID == "" || parentID == "root" {
		var masters []DriveFolder
		for _, f := range folders {
			if f.ParentID == "" || f.IsMaster {
				masters = append(masters, f)
			}
		}
		c.JSON(http.StatusOK, DriveFoldersResponse{
			Success: true,
			Folders: masters,
			Count:   len(masters),
		})
		return
	}

	resolvedID := resolveDriveFolderID(folders, parentID)

	var children []DriveFolder
	for _, f := range folders {
		if f.ParentID == resolvedID {
			children = append(children, f)
		}
	}

	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: children,
		Count:   len(children),
	})
}

// GroupFoldersHandler returns clip/stock/voiceover folder IDs for a given group name
func (h *DriveHandlers) GroupFoldersHandler(c *gin.Context) {
	groupName := c.Param("group_name")
	folders := h.getLinks()

	result := gin.H{
		"success": true,
		"group":   groupName,
	}

	if clipName, ok := groupToClipFolder[groupName]; ok {
		clipID := findMasterIDByName(folders, []string{clipName, groupName})
		result["clip"] = clipID
	}

	if stockName, ok := stockFolderAliases[groupName]; ok {
		stockID := findMasterIDByName(folders, []string{stockName, groupName + " Stock"})
		result["stock"] = stockID
	}

	if voiceoverName, ok := groupToVoiceoverFolder[groupName]; ok {
		voiceoverID := findMasterIDByName(folders, []string{voiceoverName, groupName + " Voice"})
		result["voiceover"] = voiceoverID
	}

	c.JSON(http.StatusOK, result)
}

// ClipFolderIDHandler returns the clip folder ID for a given folder_name or group
func (h *DriveHandlers) ClipFolderIDHandler(c *gin.Context) {
	folderName := c.Query("folder_name")
	group := c.Query("group")

	folders := h.getLinks()

	if folderName != "" {
		for _, f := range folders {
			if normalizeName(f.Name) == normalizeName(folderName) {
				c.JSON(http.StatusOK, gin.H{
					"success": true,
					"id":      f.ID,
					"name":    f.Name,
				})
				return
			}
		}
	}

	if group != "" {
		if clipName, ok := groupToClipFolder[group]; ok {
			clipID := findMasterIDByName(folders, []string{clipName, group})
			if clipID != "" {
				c.JSON(http.StatusOK, gin.H{
					"success": true,
					"id":      clipID,
					"group":   group,
				})
				return
			}
		}
	}

	c.JSON(http.StatusNotFound, gin.H{
		"success": false,
		"error":   "folder not found",
	})
}

// DriveFilesHandler lists subfolder items under a parent_id (folders only)
func (h *DriveHandlers) DriveFilesHandler(c *gin.Context) {
	parentID := c.Query("parent_id")
	if parentID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "parent_id required"})
		return
	}

	folders := h.getLinks()
	resolvedID := resolveDriveFolderID(folders, parentID)

	var children []DriveFolder
	for _, f := range folders {
		if f.ParentID == resolvedID {
			children = append(children, f)
		}
	}

	if len(children) == 0 {
		for _, f := range folders {
			if strings.Contains(normalizeName(f.Name), normalizeName(parentID)) {
				children = append(children, f)
			}
		}
	}

	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: children,
		Count:   len(children),
	})
}
