package drive

import (
	"encoding/json"
	"log"
	"strings"
	"time"
)

// getDriveLinksFromCache returns folders from cache with 30s TTL
func getDriveLinksFromCache() []DriveFolder {
	driveLinksCache.mu.RLock()
	if time.Since(driveLinksCache.lastLoad) < 30*time.Second && len(driveLinksCache.folders) > 0 {
		folders := make([]DriveFolder, len(driveLinksCache.folders))
		copy(folders, driveLinksCache.folders)
		driveLinksCache.mu.RUnlock()
		return folders
	}
	driveLinksCache.mu.RUnlock()

	driveLinksCache.mu.Lock()
	defer driveLinksCache.mu.Unlock()

	loadDriveLinksFromDisk()
	return driveLinksCache.folders
}

// loadDriveLinksFromDisk loads folders from SQLite (source of truth).
func loadDriveLinksFromDisk() {
	if driveLinksStore != nil {
		dbFolders, err := driveLinksStore.ListDriveLinks()
		if err == nil && len(dbFolders) > 0 {
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
			driveLinksCache.folders = folders
			driveLinksCache.lastLoad = time.Now()
			return
		}
	}
	driveLinksCache.folders = nil
	driveLinksCache.lastLoad = time.Now()
}

// saveDriveLinksToDisk persists folders to SQLite only.
func saveDriveLinksToDisk(folders []DriveFolder) error {
	if driveLinksStore != nil {
		rawList, _ := json.Marshal(folders)
		if err := driveLinksStore.ReplaceDriveLinks(rawList); err != nil {
			log.Printf("[WARN] Drive links SQLite save failed: %v", err)
			return err
		}
	}
	return nil
}

// normalizeName normalizes a folder name for matching
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

// Helper functions for map[string]any field extraction
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
