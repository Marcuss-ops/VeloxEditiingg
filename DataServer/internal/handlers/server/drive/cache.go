package drive

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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

	if err := loadDriveLinksFromDisk(); err != nil {
		log.Printf("[WARN] Drive cache reload failed: %v", err)
	}
	return driveLinksCache.folders
}

// loadDriveLinksFromDisk loads folders from DB, JSON fallback, or master folders
func loadDriveLinksFromDisk() error {
	var folders []DriveFolder
	var data []byte
	var err error

	// Try SQLite store first
	if driveLinksStore != nil {
		dbFolders, err := driveLinksStore.ListDriveLinks()
		if err == nil && len(dbFolders) > 0 {
			folders = make([]DriveFolder, len(dbFolders))
			for i, f := range dbFolders {
				folders[i] = DriveFolder{
					ID:        getStringField(f, "id"),
					Name:      getStringField(f, "name"),
					Link:      getStringField(f, "link"),
					ParentID:  getStringField(f, "parent_id"),
					Language:  getStringField(f, "language"),
					CreatedAt: getInt64Field(f, "created_at"),
					UpdatedAt: getInt64Field(f, "updated_at"),
					IsMaster:  getBoolField(f, "is_master"),
				}
			}
			driveLinksCache.folders = folders
			driveLinksCache.lastLoad = time.Now()
			return nil
		}
	}

	// Fallback to YAML/JSON file
	for _, path := range []string{
		filepath.Join(driveLinksDataDir, "drive", "drive_links.yaml"),
		filepath.Join(driveLinksDataDir, "drive", "drive_links.yml"),
		filepath.Join(driveLinksDataDir, "drive", "drive_links.json"),
	} {
		data, err = os.ReadFile(path)
		if err != nil || len(data) == 0 {
			continue
		}
		if strings.HasSuffix(strings.ToLower(path), ".json") {
			if err := json.Unmarshal(data, &folders); err == nil && len(folders) > 0 {
				driveLinksCache.folders = folders
				driveLinksCache.lastLoad = time.Now()
				return nil
			}
			continue
		}
		if err := yaml.Unmarshal(data, &folders); err == nil && len(folders) > 0 {
			driveLinksCache.folders = folders
			driveLinksCache.lastLoad = time.Now()
			return nil
		}
	}

	// Final fallback: master folders list
	masterPath := filepath.Join(driveLinksDataDir, "drive", "drive_master_folders_list.json")
	data, err = os.ReadFile(masterPath)
	if err == nil {
		var masterData MasterFoldersData
		if err := json.Unmarshal(data, &masterData); err == nil {
			folders = make([]DriveFolder, 0, len(masterData.Masters))
			for name, info := range masterData.Masters {
				folders = append(folders, DriveFolder{
					ID:              info.ID,
					Name:            info.Name,
					Link:            info.URL,
					IsMaster:        true,
					SubfoldersCount: info.SubfoldersCount,
					Language:        name,
				})
			}
		}
	}

	driveLinksCache.folders = folders
	driveLinksCache.lastLoad = time.Now()
	return nil
}

// saveDriveLinksToDisk persists folders to JSON file and SQLite store
func saveDriveLinksToDisk(folders []DriveFolder) error {
	jsonPath := filepath.Join(driveLinksDataDir, "drive", "drive_links.json")
	if err := os.MkdirAll(filepath.Dir(jsonPath), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(folders, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(jsonPath, data, 0644); err != nil {
		return err
	}

	// Best-effort sync to SQLite (note: SQLiteStore doesn't have SyncDriveLinks)
	// The JSON file is the source of truth for drive links
	_ = driveLinksStore // keep reference for potential future use

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

func getBoolField(m map[string]any, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
