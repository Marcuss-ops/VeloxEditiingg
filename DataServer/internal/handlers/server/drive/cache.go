package drive

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"
)

// driveLinksCache holds cached data — embedded in DriveHandlers.
type driveLinksCache struct {
	folders  []DriveFolder
	lastLoad time.Time
	mu       sync.RWMutex
}

// getLinks returns folders from cache with 30s TTL.
func (h *DriveHandlers) getLinks() []DriveFolder {
	h.cache.mu.RLock()
	if time.Since(h.cache.lastLoad) < 30*time.Second && len(h.cache.folders) > 0 {
		folders := make([]DriveFolder, len(h.cache.folders))
		copy(folders, h.cache.folders)
		h.cache.mu.RUnlock()
		return folders
	}
	h.cache.mu.RUnlock()

	h.cache.mu.Lock()
	defer h.cache.mu.Unlock()

	_ = h.loadFromDisk()
	return h.cache.folders
}

// loadFromDisk loads folders from SQLite (source of truth).
func (h *DriveHandlers) loadFromDisk() error {
	if h.store != nil {
		dbFolders, err := h.store.ListDriveLinks()
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
			h.cache.folders = folders
			h.cache.lastLoad = time.Now()
			return nil
		}

		h.cache.folders = nil
		h.cache.lastLoad = time.Now()
		return nil
	}

	return nil
}

// saveToDisk persists folders to SQLite.
func (h *DriveHandlers) saveToDisk(folders []DriveFolder) error {
	if h.store != nil {
		rawList, _ := json.Marshal(folders)
		if err := h.store.ReplaceDriveLinks(rawList); err != nil {
			log.Printf("[WARN] Drive links SQLite save failed: %v", err)
			return err
		}
	}
	return nil
}

// updateCache sets the cache folders and timestamp.
func (h *DriveHandlers) updateCache(folders []DriveFolder) {
	h.cache.mu.Lock()
	h.cache.folders = folders
	h.cache.lastLoad = time.Now()
	h.cache.mu.Unlock()
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
