package darkeditor

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// getUniqueFilename generates a unique filename with the given extension
func (h *Handler) getUniqueFilename(ext string) string {
	id := uuid.New().String()
	timestamp := time.Now().Unix()
	return fmt.Sprintf("%d_%s.%s", timestamp, id[:8], ext)
}

// ensureDir ensures a directory exists
func (h *Handler) ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// getTempPath returns the full path for a temp file
func (h *Handler) getTempPath(filename string) string {
	return filepath.Join(h.cfg.TempDir, filename)
}

// getProjectsPath returns the full path for a project directory
func (h *Handler) getProjectsPath(projectID string) string {
	return filepath.Join(h.cfg.ProjectsDir, projectID)
}
