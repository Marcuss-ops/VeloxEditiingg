package darkeditor

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/drive"
)

// DriveIntegrationConfig holds configuration for Drive integration
type DriveIntegrationConfig struct {
	// DriveService is the existing Drive service
	DriveService *drive.Service
	// TempDir for temporary files
	TempDir string
	// DefaultFolderID is the default folder for uploads
	DefaultFolderID string
}

// DriveIntegrationHandler handles Drive-related operations for Dark Editor
type DriveIntegrationHandler struct {
	cfg *DriveIntegrationConfig
}

// UploadToDrive uploads a file from temp to Google Drive
func (h *DriveIntegrationHandler) UploadToDrive(c *gin.Context) {
	var req UploadToDriveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Check if Drive service is available
	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	// Get the file path
	filePath := filepath.Join(h.cfg.TempDir, req.Filename)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "File not found"})
		return
	}

	// Determine folder ID
	folderID := req.FolderID
	if folderID == "" && req.FolderName != "" {
		// Create or get folder by name
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()

		folder, err := h.cfg.DriveService.GetOrCreateFolder(ctx, req.FolderName, h.cfg.DefaultFolderID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to create folder: %v", err),
			})
			return
		}
		folderID = folder.ID
	}

	if folderID == "" {
		folderID = h.cfg.DefaultFolderID
	}

	// Upload the file
	ctx, cancel := context.WithTimeout(c.Request.Context(), 120*time.Second)
	defer cancel()

	result, err := h.cfg.DriveService.UploadFile(ctx, filePath, folderID)
	if err != nil {
		log.Printf("❌ Failed to upload to Drive: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to upload: %v", err),
		})
		return
	}

	// Share if requested
	if req.ShareWithEmail != "" && result.FileID != "" {
		if err := h.cfg.DriveService.ShareFile(ctx, result.FileID, req.ShareWithEmail, "reader"); err != nil {
			log.Printf("⚠️ Failed to share file: %v", err)
		}
	}

	log.Printf("✅ Uploaded '%s' to Drive", req.Filename)

	c.JSON(http.StatusOK, UploadToDriveResponse{
		Success:     result.Success,
		FileID:      result.FileID,
		WebViewLink: result.WebViewLink,
		FolderLink:  result.FolderLink,
		Message:     "File uploaded successfully",
	})
}

// UploadDirect handles direct file upload to Drive (file + metadata in same request)
func (h *DriveIntegrationHandler) UploadDirect(c *gin.Context) {
	// Get form values
	folderID := c.PostForm("folder_id")
	folderName := c.PostForm("folder_name")
	shareWith := c.PostForm("share_with")

	// Get the uploaded file
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	// Check if Drive service is available
	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	// Save to temp file first
	tempFilename := fmt.Sprintf("drive_upload_%d_%s", time.Now().Unix(), header.Filename)
	tempPath := filepath.Join(h.cfg.TempDir, tempFilename)

	// Create temp file
	outFile, err := os.Create(tempPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temp file"})
		return
	}
	defer outFile.Close()

	// Copy file content
	if _, err := outFile.ReadFrom(file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}

	// Determine folder ID
	if folderID == "" && folderName != "" {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()

		folder, err := h.cfg.DriveService.GetOrCreateFolder(ctx, folderName, h.cfg.DefaultFolderID)
		if err != nil {
			os.Remove(tempPath)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to create folder: %v", err),
			})
			return
		}
		folderID = folder.ID
	}

	if folderID == "" {
		folderID = h.cfg.DefaultFolderID
	}

	// Upload to Drive
	ctx, cancel := context.WithTimeout(c.Request.Context(), 120*time.Second)
	defer cancel()

	result, err := h.cfg.DriveService.UploadFile(ctx, tempPath, folderID)

	// Clean up temp file
	os.Remove(tempPath)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to upload: %v", err),
		})
		return
	}

	// Share if requested
	if shareWith != "" && result.FileID != "" {
		if err := h.cfg.DriveService.ShareFile(ctx, result.FileID, shareWith, "reader"); err != nil {
			log.Printf("⚠️ Failed to share file: %v", err)
		}
	}

	log.Printf("✅ Direct upload to Drive completed: %s", header.Filename)

	c.JSON(http.StatusOK, gin.H{
		"success":       true,
		"file_id":       result.FileID,
		"web_view_link": result.WebViewLink,
		"folder_link":   result.FolderLink,
		"message":       "File uploaded successfully",
	})
}

// CreateFolder creates a new folder in Drive
func (h *DriveIntegrationHandler) CreateFolder(c *gin.Context) {
	var req DriveCreateFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	parentID := req.ParentID
	if parentID == "" {
		parentID = h.cfg.DefaultFolderID
	}

	folder, err := h.cfg.DriveService.CreateFolder(ctx, req.Name, parentID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to create folder: %v", err),
		})
		return
	}

	log.Printf("📁 Created Drive folder: %s", req.Name)

	c.JSON(http.StatusOK, DriveCreateFolderResponse{
		Success: true,
		ID:      folder.ID,
		Name:    folder.Name,
		Message: "Folder created successfully",
	})
}

// ListFiles lists files in a Drive folder
func (h *DriveIntegrationHandler) ListFiles(c *gin.Context) {
	folderID := c.Query("folder_id")
	if folderID == "" {
		folderID = h.cfg.DefaultFolderID
	}

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	files, err := h.cfg.DriveService.ListFiles(ctx, folderID, 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to list files: %v", err),
		})
		return
	}

	// Convert to response format
	fileInfos := make([]FileInfo, 0, len(files))
	for _, f := range files {
		fileInfos = append(fileInfos, FileInfo{
			ID:           f.ID,
			Name:         f.Name,
			MimeType:     f.MimeType,
			WebViewLink:  f.WebViewLink,
			Size:         f.Size,
			CreatedTime:  f.CreatedTime,
			ModifiedTime: f.ModifiedTime,
		})
	}

	c.JSON(http.StatusOK, ListFilesResponse{
		Files: fileInfos,
	})
}

// GetFileInfo gets information about a specific file
func (h *DriveIntegrationHandler) GetFileInfo(c *gin.Context) {
	fileID := c.Param("file_id")

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	link, err := h.cfg.DriveService.GetFileLink(ctx, fileID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to get file info: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":            fileID,
		"web_view_link": link,
	})
}

// DownloadFile downloads a file from Drive
func (h *DriveIntegrationHandler) DownloadFile(c *gin.Context) {
	fileID := c.Param("file_id")

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	// Create temp file for download
	destFilename := fmt.Sprintf("drive_download_%d_%s", time.Now().Unix(), fileID)
	destPath := filepath.Join(h.cfg.TempDir, destFilename)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 120*time.Second)
	defer cancel()

	if err := h.cfg.DriveService.DownloadFile(ctx, fileID, destPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to download file: %v", err),
		})
		return
	}

	log.Printf("📥 Downloaded file from Drive: %s", fileID)

	c.JSON(http.StatusOK, gin.H{
		"success":  true,
		"filename": destFilename,
		"url":      fmt.Sprintf("temp/%s", destFilename),
	})
}

// ShareFile shares a file with an email
func (h *DriveIntegrationHandler) ShareFile(c *gin.Context) {
	fileID := c.Param("file_id")

	var req struct {
		Email string `json:"email" binding:"required"`
		Role  string `json:"role"` // "reader", "writer", "owner"
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.Role == "" {
		req.Role = "reader"
	}

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	if err := h.cfg.DriveService.ShareFile(ctx, fileID, req.Email, req.Role); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to share file: %v", err),
		})
		return
	}

	log.Printf("🔗 Shared file %s with %s", fileID, req.Email)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("File shared with %s", req.Email),
	})
}

// DeleteFile moves a file to trash
func (h *DriveIntegrationHandler) DeleteFile(c *gin.Context) {
	fileID := c.Param("file_id")

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	if err := h.cfg.DriveService.DeleteFile(ctx, fileID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to delete file: %v", err),
		})
		return
	}

	log.Printf("🗑️ Deleted file from Drive: %s", fileID)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "File moved to trash",
	})
}

// GetStorageInfo returns storage quota information
func (h *DriveIntegrationHandler) GetStorageInfo(c *gin.Context) {
	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	about, err := h.cfg.DriveService.GetAbout(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to get storage info: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, about)
}

// HealthCheck returns the health status of Drive integration
func (h *DriveIntegrationHandler) HealthCheck(c *gin.Context) {
	if h.cfg.DriveService == nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  "unavailable",
			"message": "Drive service not configured",
		})
		return
	}

	// Try to get about info to verify connection
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	_, err := h.cfg.DriveService.GetAbout(ctx)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Drive API error: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"message": "Drive service connected",
	})
}

// SyncProject syncs a project folder with Drive
func (h *DriveIntegrationHandler) SyncProject(c *gin.Context) {
	var req struct {
		ProjectName string `json:"project_name" binding:"required"`
		FolderID    string `json:"folder_id"`
		Direction   string `json:"direction"` // "upload" or "download"
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if h.cfg.DriveService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Drive service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 300*time.Second)
	defer cancel()

	// Get or create project folder
	folderID := req.FolderID
	if folderID == "" {
		folder, err := h.cfg.DriveService.GetOrCreateFolder(ctx, req.ProjectName, h.cfg.DefaultFolderID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to get/create folder: %v", err),
			})
			return
		}
		folderID = folder.ID
	}

	switch req.Direction {
	case "download", "":
		// Download files from Drive
		projectDir := filepath.Join(h.cfg.TempDir, "projects", req.ProjectName)
		files, err := h.cfg.DriveService.DownloadFilesFromFolder(ctx, folderID, projectDir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to download: %v", err),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success":     true,
			"direction":   "download",
			"folder_id":   folderID,
			"files_count": len(files),
			"local_path":  projectDir,
			"message":     fmt.Sprintf("Downloaded %d files", len(files)),
		})

	case "upload":
		// Upload files from local project to Drive
		projectDir := filepath.Join(h.cfg.TempDir, "projects", req.ProjectName)
		entries, err := os.ReadDir(projectDir)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Project directory not found",
			})
			return
		}

		var uploadedFiles []string
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			filePath := filepath.Join(projectDir, entry.Name())
			result, err := h.cfg.DriveService.UploadFile(ctx, filePath, folderID)
			if err != nil {
				log.Printf("⚠️ Failed to upload %s: %v", entry.Name(), err)
				continue
			}
			uploadedFiles = append(uploadedFiles, result.FileID)
		}

		c.JSON(http.StatusOK, gin.H{
			"success":     true,
			"direction":   "upload",
			"folder_id":   folderID,
			"files_count": len(uploadedFiles),
			"message":     fmt.Sprintf("Uploaded %d files", len(uploadedFiles)),
		})

	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid direction. Use 'upload' or 'download'",
		})
	}
}
