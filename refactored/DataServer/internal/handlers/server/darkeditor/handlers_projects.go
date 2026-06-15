package darkeditor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/store"
)

// ListProjects lists all projects
func (h *Handler) ListProjects(c *gin.Context) {
	projectType := c.Query("type")

	if h.dbStore == nil {
		// Fallback to file-based listing
		h.listProjectsFromFile(c, projectType)
		return
	}

	ctx := context.Background()
	opts := store.ProjectListOptions{
		Type:  projectType,
		Limit: 200,
	}
	dbProjects, err := h.dbStore.ListProjects(ctx, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list projects"})
		return
	}

	projects := []Project{}
	for _, p := range dbProjects {
		proj := Project{
			ID:         p.ID,
			Name:       p.Name,
			Type:       p.Type,
			CanvasJSON: p.CanvasJSON,
			PreviewURL: p.PreviewURL,
			CreatedAt:  p.CreatedAt,
			UpdatedAt:  p.UpdatedAt,
			FolderID:   p.FolderID,
		}
		projects = append(projects, proj)
	}

	c.JSON(http.StatusOK, projects)
}

// listProjectsFromFile is the legacy file-based fallback for listing projects.
func (h *Handler) listProjectsFromFile(c *gin.Context, projectType string) {
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

	if h.dbStore != nil {
		h.saveProjectToDB(c, req)
		return
	}

	// Fallback to file-based persistence
	h.saveProjectToFile(c, req)
}

func (h *Handler) saveProjectToDB(c *gin.Context, req SaveProjectRequest) {
	ctx := context.Background()

	now := time.Now()
	project := &store.Project{
		ID:         req.ID,
		Name:       req.Name,
		Type:       req.Type,
		CanvasJSON: req.CanvasJSON,
		PreviewURL: fmt.Sprintf("/dark_editor_v2/projects/%s/preview.png", req.ID),
		UpdatedAt:  now,
	}

	// Check if project already exists
	existing, err := h.dbStore.GetProject(ctx, req.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check existing project"})
		return
	}

	if existing != nil {
		// Preserve original created_at
		project.CreatedAt = existing.CreatedAt
		project.FolderID = existing.FolderID
		if err := h.dbStore.UpdateProject(ctx, project); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update project"})
			return
		}
	} else {
		project.CreatedAt = now
		if err := h.dbStore.CreateProject(ctx, project); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save project"})
			return
		}
	}

	// Handle preview file if provided
	if req.PreviewFilename != "" {
		srcPath := h.getTempPath(req.PreviewFilename)
		projectDir := filepath.Join(h.cfg.ProjectsDir, req.ID)
		if err := h.ensureDir(projectDir); err == nil {
			dstPath := filepath.Join(projectDir, "preview.png")
			if data, err := os.ReadFile(srcPath); err == nil {
				_ = os.WriteFile(dstPath, data, 0644)
			} else {
				log.Printf("[WARN] SaveProject preview copy failed: %v", err)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"id":      project.ID,
		"message": "Project saved successfully",
	})
}

func (h *Handler) saveProjectToFile(c *gin.Context, req SaveProjectRequest) {
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
			log.Printf("[WARN] SaveProject preview copy failed: %v", err)
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

	if h.dbStore != nil {
		ctx := context.Background()
		project, err := h.dbStore.GetProject(ctx, projectID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load project"})
			return
		}
		if project == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"id":          projectID,
			"canvas_json": project.CanvasJSON,
		})
		return
	}

	// Fallback to file-based loading
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

	if h.dbStore != nil {
		ctx := context.Background()
		if err := h.dbStore.DeleteProject(ctx, projectID); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true})
		return
	}

	// Fallback to file-based deletion
	projectDir := filepath.Join(h.cfg.ProjectsDir, projectID)
	if err := os.RemoveAll(projectDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete project"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}
