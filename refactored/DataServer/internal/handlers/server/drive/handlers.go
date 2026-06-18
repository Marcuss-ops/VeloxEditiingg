package drive

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// TokenFile represents a Drive token file entry
type TokenFile struct {
	Name string `json:"name"`
	Path string `json:"path,omitempty"`
}

// ListDriveTokensHandler lists all Drive token files
func ListDriveTokensHandler(c *gin.Context) {
	if driveTokensDir == "" {
		c.JSON(http.StatusOK, gin.H{"files": []TokenFile{}})
		return
	}

	entries, err := os.ReadDir(driveTokensDir)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"files": []TokenFile{}})
		return
	}

	var files []TokenFile
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, TokenFile{
				Name: entry.Name(),
				Path: filepath.Join(driveTokensDir, entry.Name()),
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{"files": files})
}

// Request types
type SaveDriveLinksRequest struct {
	Folders []DriveFolder `json:"folders"`
}

type AddDriveFolderRequest struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Link     string `json:"link"`
	ParentID string `json:"parentId,omitempty"`
	Language string `json:"language,omitempty"`
}

type UpdateDriveFolderRequest struct {
	Name     string `json:"name,omitempty"`
	Link     string `json:"link,omitempty"`
	ParentID string `json:"parentId,omitempty"`
	Language string `json:"language,omitempty"`
}

type CreateDriveFolderRequest struct {
	Name     string `json:"name"`
	ParentID string `json:"parentId,omitempty"`
	Language string `json:"language,omitempty"`
}

type UploadTextRequest struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
	ParentID string `json:"parentId,omitempty"`
}

type UpsertMasterFolderRequest struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	URL             string `json:"url"`
	Language        string `json:"language,omitempty"`
	SubfoldersCount int    `json:"subfolders_count,omitempty"`
	MetadataJSON    string `json:"metadata_json,omitempty"`
}

// GetDriveLinksHandler returns all drive links
func GetDriveLinksHandler(c *gin.Context) {
	folders := getDriveLinksFromCache()
	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: folders,
		Count:   len(folders),
	})
}

// GetDriveLinksByGroupHandler returns drive links for a specific group
func GetDriveLinksByGroupHandler(c *gin.Context) {
	groupName := c.Param("group_name")
	folders := getDriveLinksFromCache()

	var filtered []DriveFolder
	groupLower := strings.ToLower(groupName)
	for _, f := range folders {
		nameLower := strings.ToLower(f.Name)
		langLower := strings.ToLower(f.Language)
		if strings.HasPrefix(nameLower, groupLower) || langLower == groupLower {
			filtered = append(filtered, f)
		}
	}

	c.JSON(http.StatusOK, DriveFoldersResponse{
		Success: true,
		Folders: filtered,
		Count:   len(filtered),
	})
}

// GetMasterFoldersHandler returns master folders from SQLite (source of truth)
func GetMasterFoldersHandler(c *gin.Context) {
	masters := make(gin.H)

	// SQLite is the source of truth
	if driveLinksStore != nil {
		dbMasters, err := driveLinksStore.ListMasterFolders()
		if err == nil && len(dbMasters) > 0 {
			for _, m := range dbMasters {
				language, _ := m["language"].(string)
				if language == "" {
					continue
				}
				masters[language] = m
			}
			c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"masters": masters}})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"masters": gin.H{}}})
}

// UpsertMasterFolderHandler creates or updates a master folder entry.
func UpsertMasterFolderHandler(c *gin.Context) {
	if driveLinksStore == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "drive store not initialized"})
		return
	}

	var req UpsertMasterFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	if req.ID == "" || req.Name == "" || req.URL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "id, name and url are required"})
		return
	}

	if err := driveLinksStore.UpsertMasterFolder(req.ID, req.Name, req.URL, req.Language, req.SubfoldersCount); err != nil {
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
func SaveDriveLinksHandler(c *gin.Context) {
	var req SaveDriveLinksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	if err := saveDriveLinksToDisk(req.Folders); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	// Update cache
	driveLinksCache.mu.Lock()
	driveLinksCache.folders = req.Folders
	driveLinksCache.lastLoad = time.Now()
	driveLinksCache.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"count":   len(req.Folders),
	})
}

// AddDriveFolderHandler adds or updates a single folder
func AddDriveFolderHandler(c *gin.Context) {
	var req AddDriveFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	folders := getDriveLinksFromCache()

	// Auto-generate ID from URL if missing
	if req.ID == "" && req.Link != "" {
		parts := strings.Split(req.Link, "/")
		if len(parts) > 0 {
			req.ID = parts[len(parts)-1]
		}
	}

	// Check if folder exists (by ID or Link)
	found := false
	for i, f := range folders {
		if f.ID == req.ID || f.Link == req.Link {
			folders[i].Name = req.Name
			folders[i].Link = req.Link
			folders[i].ParentID = req.ParentID
			folders[i].Language = req.Language
			folders[i].UpdatedAt = time.Now().UnixMilli()
			found = true
			break
		}
	}

	if !found {
		newFolder := DriveFolder{
			ID:        req.ID,
			Name:      req.Name,
			Link:      req.Link,
			ParentID:  req.ParentID,
			Language:  req.Language,
			CreatedAt: time.Now().UnixMilli(),
			UpdatedAt: time.Now().UnixMilli(),
		}
		folders = append(folders, newFolder)
	}

	if err := saveDriveLinksToDisk(folders); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	// Update cache
	driveLinksCache.mu.Lock()
	driveLinksCache.folders = folders
	driveLinksCache.lastLoad = time.Now()
	driveLinksCache.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"success": true, "folder_id": req.ID})
}

// UpdateDriveFolderHandler updates a folder by ID
func UpdateDriveFolderHandler(c *gin.Context) {
	folderID := c.Param("folder_id")

	var req UpdateDriveFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	folders := getDriveLinksFromCache()
	for i, f := range folders {
		if f.ID == folderID {
			if req.Name != "" {
				folders[i].Name = req.Name
			}
			if req.Link != "" {
				folders[i].Link = req.Link
			}
			if req.ParentID != "" {
				folders[i].ParentID = req.ParentID
			}
			if req.Language != "" {
				folders[i].Language = req.Language
			}
			folders[i].UpdatedAt = time.Now().UnixMilli()

			if err := saveDriveLinksToDisk(folders); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
				return
			}

			// Update cache
			driveLinksCache.mu.Lock()
			driveLinksCache.folders = folders
			driveLinksCache.lastLoad = time.Now()
			driveLinksCache.mu.Unlock()

			c.JSON(http.StatusOK, gin.H{"success": true, "folder_id": folderID})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "folder not found"})
}

// DeleteDriveFolderHandler deletes a folder and all its children
func DeleteDriveFolderHandler(c *gin.Context) {
	folderID := c.Param("folder_id")

	folders := getDriveLinksFromCache()

	// Find folder and delete it
	var remaining []DriveFolder
	for _, f := range folders {
		if f.ID != folderID && f.ParentID != folderID {
			remaining = append(remaining, f)
		}
	}

	if len(remaining) == len(folders) {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "folder not found"})
		return
	}

	deletedCount := len(folders) - len(remaining)

	if err := saveDriveLinksToDisk(remaining); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	// Update cache
	driveLinksCache.mu.Lock()
	driveLinksCache.folders = remaining
	driveLinksCache.lastLoad = time.Now()
	driveLinksCache.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"deleted": deletedCount,
	})
}

// CreateDriveFolderHandler creates a new folder entry (metadata only)
func CreateDriveFolderHandler(c *gin.Context) {
	var req CreateDriveFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	folders := getDriveLinksFromCache()

	// Generate synthetic ID
	newID := fmt.Sprintf("folder_%d", time.Now().UnixNano())
	newFolder := DriveFolder{
		ID:        newID,
		Name:      req.Name,
		Link:      fmt.Sprintf("https://drive.google.com/drive/folders/%s", newID),
		ParentID:  req.ParentID,
		Language:  req.Language,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		IsMaster:  false,
	}

	folders = append(folders, newFolder)

	if err := saveDriveLinksToDisk(folders); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	// Update cache
	driveLinksCache.mu.Lock()
	driveLinksCache.folders = folders
	driveLinksCache.lastLoad = time.Now()
	driveLinksCache.mu.Unlock()

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"id":      newID,
	})
}

// UploadTextHandler simulates text upload to Drive (returns mock URL)
func UploadTextHandler(c *gin.Context) {
	var req UploadTextRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	// Generate mock upload URL
	mockURL := fmt.Sprintf("https://drive.google.com/file/d/text_%d/view", time.Now().UnixNano())

	c.JSON(http.StatusOK, gin.H{
		"success":  true,
		"url":      mockURL,
		"filename": req.Filename,
	})
}
