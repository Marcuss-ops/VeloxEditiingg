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

	contentType := header.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "image/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File must be an image"})
		return
	}

	ext := strings.TrimPrefix(filepath.Ext(header.Filename), ".")
	if ext == "" {
		ext = "png"
	}

	filename := h.getUniqueFilename(ext)

	if err := h.ensureDir(h.cfg.TempDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temp directory"})
		return
	}

	dstPath := h.getTempPath(filename)
	dst, err := os.Create(dstPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create file"})
		return
	}
	defer dst.Close()

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

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	filterOpts := processors.FilterOptions{
		Type:  processors.FilterType(strings.ToLower(req.FilterType)),
		Value: req.Value,
	}

	if strings.ToLower(req.FilterType) == "blur" {
		filterOpts.Radius = req.Value
	}

	processedImg := processors.ApplyFilter(img, filterOpts)

	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

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

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	transformOpts := processors.TransformOptions{
		CropBox:             req.CropBox,
		ResizeDims:          req.ResizeDims,
		MaintainAspectRatio: true,
	}

	processedImg := processors.Transform(img, transformOpts)

	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

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

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	format := processors.ParseFormat(req.Format)
	ext := processors.GetFileExtension(format)

	quality := req.Quality
	if quality <= 0 || quality > 100 {
		quality = 90
	}

	exportOpts := processors.ExportOptions{
		Format:  format,
		Quality: quality,
	}

	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

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

	if req.Width == 0 {
		req.Width = 1024
	}
	if req.Height == 0 {
		req.Height = 1024
	}
	if req.Steps == 0 {
		req.Steps = 4
	}

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

	imgData, err := base64.StdEncoding.DecodeString(result.Artifacts[0].Base64)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode image"})
		return
	}

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

	if req.Scale == 0 {
		req.Scale = 2
	}

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	upscaleOpts := processors.UpscaleOptions{
		Scale:       req.Scale,
		SaveInPlace: req.SaveInPlace,
	}

	result, err := processors.Upscale(img, upscaleOpts, outputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to upscale image: %v", err)})
		return
	}

	if !processors.IsRealESRGANAvailable() {
		h.logger.Log(LogLevelWarn, "Real-ESRGAN not available, used Lanczos upscaling fallback", nil, "server")
	}

	c.JSON(http.StatusOK, UpscaleResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
		SavedAt:  result.OutputPath,
	})
}

// ============== PROJECT HANDLERS ==============

// ListProjects lists all projects
func (h *Handler) ListProjects(c *gin.Context) {
	projectType := c.Query("type")
	ctx := c.Request.Context()

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

	if req.ID == "" {
		req.ID = fmt.Sprintf("%s", uuid.New().String())
	}

	if req.Type == "" {
		req.Type = "project"
	}

	ctx := c.Request.Context()

	if h.store != nil {
		projectDir := filepath.Join(h.cfg.ProjectsDir, req.ID)
		_ = h.ensureDir(projectDir)

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
			project.CreatedAt = existing.CreatedAt
			if err := h.store.UpdateProject(ctx, project); err != nil {
				log.Printf("❌ SaveProject (DB update): %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update project"})
				return
			}
		} else {
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

	if h.store != nil {
		if err := h.store.DeleteProject(ctx, projectID); err != nil {
			log.Printf("❌ DeleteProject (DB): %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete project"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true})
		return
	}

	projectDir := filepath.Join(h.cfg.ProjectsDir, projectID)
	if err := os.RemoveAll(projectDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete project"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ============== LOGS ==============

// GetLogs returns recent log entries
func (h *Handler) GetLogs(c *gin.Context) {
	if h.logger == nil {
		c.JSON(http.StatusOK, gin.H{
			"logs":  []LogEntry{},
			"count": 0,
			"error": "Logger not initialized",
		})
		return
	}

	level := LogLevel(c.Query("level"))
	limit := 100
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

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

	log.Printf("[CLIENT-%s] %s", strings.ToUpper(req.Level), req.Message)

	if h.logger != nil {
		h.logger.Log(LogLevel(req.Level), req.Message, req.Metadata, "client")
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
