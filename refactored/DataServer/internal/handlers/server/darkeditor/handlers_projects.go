package darkeditor

import (
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
