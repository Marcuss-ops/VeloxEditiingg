package darkeditor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/handlers/server/darkeditor/processors"
	"velox-server/internal/store"
)

// ============== CORE IMAGE HANDLERS ==============

// UploadImage handles image upload
func (h *Handler) UploadImage(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	// Check content type
	contentType := header.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "image/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File must be an image"})
		return
	}

	// Get file extension
	ext := strings.TrimPrefix(filepath.Ext(header.Filename), ".")
	if ext == "" {
		ext = "png"
	}

	// Generate unique filename
	filename := h.getUniqueFilename(ext)

	// Ensure temp dir exists
	if err := h.ensureDir(h.cfg.TempDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temp directory"})
		return
	}

	// Create destination file
	dstPath := h.getTempPath(filename)
	dst, err := os.Create(dstPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create file"})
		return
	}
	defer dst.Close()

	// Copy file content
	if _, err := io.Copy(dst, file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}

	c.JSON(http.StatusOK, UploadResponse{
		Filename: filename,
		URL:      fmt.Sprintf("temp/%s", filename),
	})
}

// ApplyFilter applies a filter to an image
func (h *Handler) ApplyFilter(c *gin.Context) {
	var req FilterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	// Load the image
	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	// Apply the filter
	filterOpts := processors.FilterOptions{
		Type:  processors.FilterType(strings.ToLower(req.FilterType)),
		Value: req.Value,
	}

	// For blur, use Value as radius
	if strings.ToLower(req.FilterType) == "blur" {
		filterOpts.Radius = req.Value
	}

	processedImg := processors.ApplyFilter(img, filterOpts)

	// Determine output format
	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	// Save the processed image
	if err := processors.SaveImage(processedImg, outputPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to save processed image: %v", err)})
		return
	}

	c.JSON(http.StatusOK, FilterResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
	})
}

// TransformImage handles crop and resize operations
func (h *Handler) TransformImage(c *gin.Context) {
	var req TransformRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	// Load the image
	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	// Apply transformations
	transformOpts := processors.TransformOptions{
		CropBox:             req.CropBox,
		ResizeDims:          req.ResizeDims,
		MaintainAspectRatio: true, // Default to maintaining aspect ratio
	}

	processedImg := processors.Transform(img, transformOpts)

	// Determine output format
	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	// Save the processed image
	if err := processors.SaveImage(processedImg, outputPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to save processed image: %v", err)})
		return
	}

	c.JSON(http.StatusOK, FilterResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
	})
}

// ExportImage exports an image in the specified format
func (h *Handler) ExportImage(c *gin.Context) {
	var req ExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	// Load the image
	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	// Determine output format
	format := processors.ParseFormat(req.Format)
	ext := processors.GetFileExtension(format)

	// Set quality default
	quality := req.Quality
	if quality <= 0 || quality > 100 {
		quality = 90
	}

	// Create export options
	exportOpts := processors.ExportOptions{
		Format:  format,
		Quality: quality,
	}

	// Generate output filename
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	// Export the image
	if err := processors.ExportToFile(img, outputPath, exportOpts); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to export image: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"url":      fmt.Sprintf("temp/%s", newFilename),
		"filename": newFilename,
	})
}

// GenerateImage generates an image using NVIDIA FLUX API
func (h *Handler) GenerateImage(c *gin.Context) {
	var req GenerateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if h.cfg.NVIDIAAPIKey == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "NVIDIA API Key not configured"})
		return
	}

	// Set defaults
	if req.Width == 0 {
		req.Width = 1024
	}
	if req.Height == 0 {
		req.Height = 1024
	}
	if req.Steps == 0 {
		req.Steps = 4
	}

	// Prepare request to NVIDIA API
	payload := map[string]interface{}{
		"prompt": req.Prompt,
		"width":  req.Width,
		"height": req.Height,
		"seed":   req.Seed,
		"steps":  req.Steps,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to prepare request"})
		return
	}

	// Call NVIDIA API
	reqHTTP, err := http.NewRequest("POST", "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell", bytes.NewBuffer(jsonPayload))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}

	reqHTTP.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.cfg.NVIDIAAPIKey))
	reqHTTP.Header.Set("Content-Type", "application/json")
	reqHTTP.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(reqHTTP)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("NVIDIA API error: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("NVIDIA API returned %d: %s", resp.StatusCode, string(body))})
		return
	}

	// Parse response
	var result struct {
		Artifacts []struct {
			Base64 string `json:"base64"`
		} `json:"artifacts"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse NVIDIA response"})
		return
	}

	if len(result.Artifacts) == 0 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "No image generated"})
		return
	}

	// Decode base64 image
	imgData, err := base64.StdEncoding.DecodeString(result.Artifacts[0].Base64)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode image"})
		return
	}

	// Save image
	filename := h.getUniqueFilename("png")
	outputPath := h.getTempPath(filename)

	if err := os.WriteFile(outputPath, imgData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save image"})
		return
	}

	c.JSON(http.StatusOK, GenerateResponse{
		Filename: filename,
		URL:      fmt.Sprintf("temp/%s", filename),
		Prompt:   req.Prompt,
	})
}

// UpscaleImage upscales an image using Real-ESRGAN or fallback method
func (h *Handler) UpscaleImage(c *gin.Context) {
	var req UpscaleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		// Try as absolute path
		if filepath.IsAbs(req.Filename) {
			inputPath = req.Filename
			if _, err := os.Stat(inputPath); os.IsNotExist(err) {
				c.JSON(http.StatusNotFound, gin.H{"error": "Input file not found"})
				return
			}
		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "Input file not found"})
			return
		}
	}

	// Set default scale
	if req.Scale == 0 {
		req.Scale = 2
	}

	// Load the image
	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	// Determine output format
	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	// Upscale options
	upscaleOpts := processors.UpscaleOptions{
		Scale:       req.Scale,
		SaveInPlace: req.SaveInPlace,
	}

	// Perform upscaling (will use Real-ESRGAN if available, otherwise fallback)
	result, err := processors.Upscale(img, upscaleOpts, outputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to upscale image: %v", err)})
		return
	}

	// Log if using fallback
	if !processors.IsRealESRGANAvailable() {
		h.logger.Log(LogLevelWarn, "Real-ESRGAN not available, used Lanczos upscaling fallback", nil, "server")
	}

	c.JSON(http.StatusOK, UpscaleResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
		SavedAt:  result.OutputPath,
	})
}

// YouTubeGrab extracts thumbnail from YouTube video URL
func (h *Handler) YouTubeGrab(c *gin.Context) {
	var req YouTubeGrabRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Extract video ID from URL
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

	// HTTP client with timeout
	client := &http.Client{Timeout: 10 * time.Second}
	
	// Max thumbnail size: 5MB
	const maxThumbSize = 5 * 1024 * 1024

	// Try to get maxresdefault first
	thumbURL := fmt.Sprintf("https://img.youtube.com/vi/%s/maxresdefault.jpg", videoID)
	resp, err := client.Get(thumbURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		// Fallback to hqdefault
		thumbURL = fmt.Sprintf("https://img.youtube.com/vi/%s/hqdefault.jpg", videoID)
		resp, err = client.Get(thumbURL)
		if err != nil || resp.StatusCode != http.StatusOK {
			c.JSON(http.StatusNotFound, gin.H{"error": "Thumbnail not found"})
			return
		}
	}
	defer resp.Body.Close()

	// Read image data with size limit
	imgData, err := io.ReadAll(io.LimitReader(resp.Body, maxThumbSize+1))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to download thumbnail"})
		return
	}
	if int64(len(imgData)) > maxThumbSize {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "Thumbnail too large"})
		return
	}

	// Save image
	filename := fmt.Sprintf("yt_%s.jpg", videoID)
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

// ============== PROJECT HANDLERS ==============

// ListProjects lists all projects
func (h *Handler) ListProjects(c *gin.Context) {
	projectType := c.Query("type")
	ctx := c.Request.Context()

	// Use PostgreSQL store if available
	if h.store != nil {
		opts := store.ProjectListOptions{
			Type:     projectType,
			Limit:    100,
			OrderBy:  "updated_at",
			OrderDir: "desc",
		}

		projects, err := h.store.ListProjects(ctx, opts)
		if err != nil {
			log.Printf("❌ ListProjects (DB): %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list projects"})
			return
		}

		// Convert to response format
		result := make([]Project, len(projects))
		for i, p := range projects {
			result[i] = Project{
				ID:         p.ID,
				Name:       p.Name,
				Type:       p.Type,
				CanvasJSON: p.CanvasJSON,
				PreviewURL: p.PreviewURL,
				CreatedAt:  p.CreatedAt,
				UpdatedAt:  p.UpdatedAt,
				FolderID:   p.FolderID,
			}
		}

		c.JSON(http.StatusOK, result)
		return
	}

	// Fallback to file-based storage
	if err := h.ensureDir(h.cfg.ProjectsDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to access projects directory"})
		return
	}

	entries, err := os.ReadDir(h.cfg.ProjectsDir)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read projects directory"})
		return
	}

	projects := []Project{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		metaPath := filepath.Join(h.cfg.ProjectsDir, entry.Name(), "meta.json")
		metaData, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}

		var project Project
		if err := json.Unmarshal(metaData, &project); err != nil {
			continue
		}

		if projectType != "" && project.Type != projectType {
			continue
		}

		projects = append(projects, project)
	}

	c.JSON(http.StatusOK, projects)
}

// SaveProject saves a project
func (h *Handler) SaveProject(c *gin.Context) {
	var req SaveProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Generate ID if not provided
	if req.ID == "" {
		req.ID = fmt.Sprintf("%s", uuid.New().String())
	}

	// Set default type
	if req.Type == "" {
		req.Type = "project"
	}

	ctx := c.Request.Context()

	// Use PostgreSQL store if available
	if h.store != nil {
		// Ensure project dir exists for preview storage
		projectDir := filepath.Join(h.cfg.ProjectsDir, req.ID)
		_ = h.ensureDir(projectDir)

		// Check if project exists (for update vs create)
		existing, _ := h.store.GetProject(ctx, req.ID)

		project := &store.Project{
			ID:         req.ID,
			Name:       req.Name,
			Type:       req.Type,
			CanvasJSON: req.CanvasJSON,
			PreviewURL: fmt.Sprintf("/dark_editor_v2/projects/%s/preview.png", req.ID),
		}

		if existing != nil {
			project.FolderID = existing.FolderID
			// Update existing project
			project.CreatedAt = existing.CreatedAt
			if err := h.store.UpdateProject(ctx, project); err != nil {
				log.Printf("❌ SaveProject (DB update): %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update project"})
				return
			}
		} else {
			// Create new project
			if err := h.store.CreateProject(ctx, project); err != nil {
				log.Printf("❌ SaveProject (DB create): %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create project"})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"id":      project.ID,
			"message": "Project saved successfully",
		})

		// Best-effort: persist preview.png if provided
		if req.PreviewFilename != "" {
			srcPath := h.getTempPath(req.PreviewFilename)
			dstPath := filepath.Join(projectDir, "preview.png")
			if data, err := os.ReadFile(srcPath); err == nil {
				_ = os.WriteFile(dstPath, data, 0644)
			} else {
				log.Printf("⚠️ SaveProject preview copy failed: %v", err)
			}
		}
		return
	}

	// Fallback to file-based storage
	projectDir := filepath.Join(h.cfg.ProjectsDir, req.ID)
	if err := h.ensureDir(projectDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create project directory"})
		return
	}

	project := Project{
		ID:         req.ID,
		Name:       req.Name,
		Type:       req.Type,
		CanvasJSON: req.CanvasJSON,
		PreviewURL: fmt.Sprintf("/dark_editor_v2/projects/%s/preview.png", req.ID),
		UpdatedAt:  time.Now(),
	}

	existingMetaPath := filepath.Join(projectDir, "meta.json")
	if existingData, err := os.ReadFile(existingMetaPath); err == nil {
		var existing Project
		if json.Unmarshal(existingData, &existing) == nil {
			project.CreatedAt = existing.CreatedAt
		}
	}
	if project.CreatedAt.IsZero() {
		project.CreatedAt = time.Now()
	}

	canvasPath := filepath.Join(projectDir, "canvas.json")
	canvasData, err := json.MarshalIndent(req.CanvasJSON, "", "  ")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize canvas"})
		return
	}
	if err := os.WriteFile(canvasPath, canvasData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save canvas"})
		return
	}

	metaData, err := json.MarshalIndent(project, "", "  ")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize metadata"})
		return
	}
	if err := os.WriteFile(existingMetaPath, metaData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save metadata"})
		return
	}

	// Best-effort: persist preview.png if provided
	if req.PreviewFilename != "" {
		srcPath := h.getTempPath(req.PreviewFilename)
		dstPath := filepath.Join(projectDir, "preview.png")
		if data, err := os.ReadFile(srcPath); err == nil {
			_ = os.WriteFile(dstPath, data, 0644)
		} else {
			log.Printf("⚠️ SaveProject preview copy failed: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"id":      project.ID,
		"message": "Project saved successfully",
	})
}

// LoadProject loads a project by ID
func (h *Handler) LoadProject(c *gin.Context) {
	projectID := c.Param("id")
	ctx := c.Request.Context()

	// Use PostgreSQL store if available
	if h.store != nil {
		project, err := h.store.GetProject(ctx, projectID)
		if err != nil {
			log.Printf("❌ LoadProject (DB): %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load project"})
			return
		}
		if project == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"id":          project.ID,
			"canvas_json": project.CanvasJSON,
			"name":        project.Name,
			"type":        project.Type,
			"preview_url": project.PreviewURL,
			"folder_id":   project.FolderID,
			"created_at":  project.CreatedAt,
			"updated_at":  project.UpdatedAt,
		})
		return
	}

	// Fallback to file-based storage
	projectDir := filepath.Join(h.cfg.ProjectsDir, projectID)
	canvasPath := filepath.Join(projectDir, "canvas.json")

	canvasData, err := os.ReadFile(canvasPath)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
		return
	}

	var canvasJSON map[string]interface{}
	if err := json.Unmarshal(canvasData, &canvasJSON); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse project data"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":          projectID,
		"canvas_json": canvasJSON,
	})
}

// DeleteProject deletes a project
func (h *Handler) DeleteProject(c *gin.Context) {
	projectID := c.Param("id")
	ctx := c.Request.Context()

	// Use PostgreSQL store if available
	if h.store != nil {
		if err := h.store.DeleteProject(ctx, projectID); err != nil {
			log.Printf("❌ DeleteProject (DB): %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete project"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true})
		return
	}

	// Fallback to file-based storage
	projectDir := filepath.Join(h.cfg.ProjectsDir, projectID)
	if err := os.RemoveAll(projectDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete project"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

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

// ============== LOGS ==============

// GetLogs returns recent log entries
func (h *Handler) GetLogs(c *gin.Context) {
	// Check if logger is available
	if h.logger == nil {
		c.JSON(http.StatusOK, gin.H{
			"logs":  []LogEntry{},
			"count": 0,
			"error": "Logger not initialized",
		})
		return
	}

	// Parse query parameters
	level := LogLevel(c.Query("level"))
	limit := 100
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	// Get entries from logger
	entries := h.logger.GetEntries(limit, level)
	c.JSON(http.StatusOK, gin.H{
		"logs":  entries,
		"count": len(entries),
	})
}

// ClientLog receives client-side log messages
func (h *Handler) ClientLog(c *gin.Context) {
	var req ClientLogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Log to stdout for immediate visibility
	log.Printf("[CLIENT-%s] %s", strings.ToUpper(req.Level), req.Message)

	// Persist to logger if available
	if h.logger != nil {
		h.logger.Log(LogLevel(req.Level), req.Message, req.Metadata, "client")
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
