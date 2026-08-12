// Package drive / folders.go — folders sub-domain of the Drive service.
// Extracted from service.go: folder caching, persistence, CRUD and listing.
package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"velox-server/internal/integrations/drive"
)

type linksCache struct {
	folders  []DriveFolder
	lastLoad time.Time
	mu       sync.RWMutex
}

// getLinks returns folders from cache with 30s TTL.
func (s *Service) getLinks() ([]DriveFolder, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("drive: store not configured")
	}
	s.cache.mu.RLock()
	if !s.cache.lastLoad.IsZero() && time.Since(s.cache.lastLoad) < 30*time.Second {
		folders := make([]DriveFolder, len(s.cache.folders))
		copy(folders, s.cache.folders)
		s.cache.mu.RUnlock()
		return folders, nil
	}
	s.cache.mu.RUnlock()

	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()

	if err := s.loadFromDisk(); err != nil {
		return nil, err
	}
	return append([]DriveFolder(nil), s.cache.folders...), nil
}

// loadFromDisk loads folders from SQLite.
func (s *Service) loadFromDisk() error {
	if s == nil || s.store == nil {
		return fmt.Errorf("drive: store not configured")
	}
	dbFolders, err := s.store.ListDriveLinks()
	if err != nil {
		return fmt.Errorf("drive: list links: %w", err)
	}
	folders := make([]DriveFolder, len(dbFolders))
	for i, f := range dbFolders {
		folders[i] = DriveFolder{
			ID:              getStringField(f, "id"),
			Name:            getStringField(f, "name"),
			Link:            getStringField(f, "link"),
			ParentID:        getStringField(f, "parent_id"),
			Language:        getStringField(f, "language"),
			CreatedAt:       getInt64Field(f, "created_at"),
			UpdatedAt:       getInt64Field(f, "updated_at"),
			IsMaster:        getBoolField(f, "is_master"),
			SubfoldersCount: getIntIntField(f, "subfolders_count"),
		}
	}
	s.cache.folders = folders
	s.cache.lastLoad = time.Now()
	return nil
}

// saveToDisk persists folders to SQLite.
func (s *Service) saveToDisk(folders []DriveFolder) error {
	if s.store != nil {
		rawList, _ := json.Marshal(folders)
		if err := s.store.ReplaceDriveLinks(rawList); err != nil {
			log.Printf("[WARN] Drive links SQLite save failed: %v", err)
			return err
		}
	}
	return nil
}

func (s *Service) updateCache(folders []DriveFolder) {
	s.cache.mu.Lock()
	s.cache.folders = folders
	s.cache.lastLoad = time.Now()
	s.cache.mu.Unlock()
}

// GetDriveLinks returns all links
func (s *Service) GetDriveLinks() ([]DriveFolder, error) {
	return s.getLinks()
}

// GetDriveLinksByGroup returns links filtered by group name
func (s *Service) GetDriveLinksByGroup(groupName string) ([]DriveFolder, error) {
	folders, err := s.getLinks()
	if err != nil {
		return nil, err
	}
	var filtered []DriveFolder
	groupLower := strings.ToLower(groupName)
	for _, f := range folders {
		nameLower := strings.ToLower(f.Name)
		langLower := strings.ToLower(f.Language)
		if strings.HasPrefix(nameLower, groupLower) || langLower == groupLower {
			filtered = append(filtered, f)
		}
	}
	return filtered, nil
}

// SaveDriveLinks replaces all drive links
func (s *Service) SaveDriveLinks(folders []DriveFolder) error {
	if err := s.saveToDisk(folders); err != nil {
		return err
	}
	s.updateCache(folders)
	return nil
}

// AddDriveFolder adds or updates a folder
func (s *Service) AddDriveFolder(req AddDriveFolderRequest) (string, error) {
	folders, err := s.getLinks()
	if err != nil {
		return "", err
	}

	if req.ID == "" && req.Link != "" {
		parts := strings.Split(req.Link, "/")
		if len(parts) > 0 {
			req.ID = parts[len(parts)-1]
		}
	}

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

	if err := s.saveToDisk(folders); err != nil {
		return "", err
	}
	s.updateCache(folders)
	return req.ID, nil
}

// UpdateDriveFolder updates a single folder
func (s *Service) UpdateDriveFolder(folderID string, req UpdateDriveFolderRequest) error {
	folders, err := s.getLinks()
	if err != nil {
		return err
	}
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

			if err := s.saveToDisk(folders); err != nil {
				return err
			}
			s.updateCache(folders)
			return nil
		}
	}
	return fmt.Errorf("%w", ErrFolderNotFound)
}

// DeleteDriveFolder deletes a folder and its children
func (s *Service) DeleteDriveFolder(folderID string) (int, error) {
	folders, err := s.getLinks()
	if err != nil {
		return 0, err
	}
	var remaining []DriveFolder
	for _, f := range folders {
		if f.ID != folderID && f.ParentID != folderID {
			remaining = append(remaining, f)
		}
	}

	if len(remaining) == len(folders) {
		return 0, fmt.Errorf("folder not found")
	}

	deletedCount := len(folders) - len(remaining)
	if err := s.saveToDisk(remaining); err != nil {
		return 0, err
	}
	s.updateCache(remaining)
	return deletedCount, nil
}

// GetDriveFolders lists child folders
func (s *Service) GetDriveFolders(parentID string) ([]DriveFolder, error) {
	folders, err := s.getLinks()
	if err != nil {
		return nil, err
	}
	if parentID == "" || parentID == "root" {
		var masters []DriveFolder
		for _, f := range folders {
			if f.ParentID == "" || f.IsMaster {
				masters = append(masters, f)
			}
		}
		return masters, nil
	}

	resolvedID := resolveDriveFolderID(folders, parentID)
	var children []DriveFolder
	for _, f := range folders {
		if f.ParentID == resolvedID {
			children = append(children, f)
		}
	}
	return children, nil
}

// DriveFiles returns folder contents matching parent_id
func (s *Service) DriveFiles(parentID string) ([]DriveFolder, error) {
	if parentID == "" {
		return nil, fmt.Errorf("%w", ErrParentIDRequired)
	}

	folders, err := s.getLinks()
	if err != nil {
		return nil, err
	}
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

	return children, nil
}

// ListFiles lists files in a Google Drive folder using the integration service.
func (s *Service) ListFiles(ctx context.Context, folderID string, pageSize int) ([]drive.File, error) {
	if s.driveService == nil {
		return nil, fmt.Errorf("drive service not configured")
	}
	return s.driveService.ListFiles(ctx, folderID, pageSize)
}

// Helpers
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

func normalizeName(s string) string {
	s = strings.ToLower(s)
	var result strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func getStringField(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt64Field(m map[string]any, key string) int64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int64:
			return val
		case float64:
			return int64(val)
		case int:
			return int64(val)
		}
	}
	return 0
}

func getIntIntField(m map[string]any, key string) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case int64:
			return int(val)
		case float64:
			return int(val)
		}
	}
	return 0
}

func getBoolField(m map[string]any, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
