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

// loadDriveLinksFromDisk loads folders from SQLite (source of truth) with JSON legacy fallback.
func loadDriveLinksFromDisk() error {
	// SQLite is the source of truth
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

		// SQLite empty — try legacy JSON import
		if imported := importLegacyDriveLinks(); imported > 0 {
			return loadDriveLinksFromDisk()
		}
	}

	// No store available — load from legacy JSON (first run before SQLite init)
	var folders []DriveFolder
	for _, path := range []string{
		filepath.Join(driveLinksDataDir, "drive", "drive_links.yaml"),
		filepath.Join(driveLinksDataDir, "drive", "drive_links.yml"),
		filepath.Join(driveLinksDataDir, "drive", "drive_links.json"),
	} {
		data, err := os.ReadFile(path)
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
	data, err := os.ReadFile(masterPath)
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
}

// importLegacyDriveLinks imports JSON/YAML drive links into SQLite on first run.
func importLegacyDriveLinks() int {
	if driveLinksStore == nil {
		return 0
	}

	// Import drive_links.json / .yaml / .yml
	for _, path := range []string{
		filepath.Join(driveLinksDataDir, "drive", "drive_links.json"),
		filepath.Join(driveLinksDataDir, "drive", "drive_links.yaml"),
		filepath.Join(driveLinksDataDir, "drive", "drive_links.yml"),
	} {
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			continue
		}

		var folders []map[string]any
		if strings.HasSuffix(strings.ToLower(path), ".json") {
			if err := json.Unmarshal(data, &folders); err != nil {
				continue
			}
		} else {
			if err := yaml.Unmarshal(data, &folders); err != nil {
				continue
			}
		}

		if count, err := driveLinksStore.MigrateDriveLinksFromJSON(folders); err == nil && count > 0 {
			log.Printf("[MIGRATE] Imported %d drive links from %s", count, path)
			return count
		}
	}

	// Import drive_master_folders_list.json
	masterPath := filepath.Join(driveLinksDataDir, "drive", "drive_master_folders_list.json")
	data, err := os.ReadFile(masterPath)
	if err == nil {
		var masterData MasterFoldersData
		if err := json.Unmarshal(data, &masterData); err == nil {
			masters := make(map[string]any, len(masterData.Masters))
			for k, v := range masterData.Masters {
				masters[k] = v
			}
			if count, err := driveLinksStore.MigrateMasterFoldersFromJSON(masters); err == nil && count > 0 {
				log.Printf("[MIGRATE] Imported %d master folders from %s", count, masterPath)
				return count
			}
		}
	}

	return 0
}

// saveDriveLinksToDisk persists folders to SQLite (primary) and JSON (backup).
func saveDriveLinksToDisk(folders []DriveFolder) error {
	// Primary: SQLite
	if driveLinksStore != nil {
		rawList, _ := json.Marshal(folders)
		if err := driveLinksStore.ReplaceDriveLinks(rawList); err != nil {
			log.Printf("[WARN] Drive links SQLite save failed: %v", err)
		}
	}

	// Backup: JSON file (legacy compatibility)
	jsonPath := filepath.Join(driveLinksDataDir, "drive", "drive_links.json")
	if err := os.MkdirAll(filepath.Dir(jsonPath), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(folders, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(jsonPath, data, 0644)
}

// saveMasterFoldersToDisk persists master folders to SQLite (primary) and JSON (backup).
func saveMasterFoldersToDisk(masters map[string]MasterFolderInfo) error {
	// Primary: SQLite
	if driveLinksStore != nil {
		for language, info := range masters {
			if err := driveLinksStore.UpsertMasterFolder(info.ID, info.Name, info.URL, language, info.SubfoldersCount); err != nil {
				log.Printf("[WARN] Master folder SQLite save failed for %s: %v", language, err)
			}
		}
	}

	// Backup: JSON file
	if driveLinksDataDir == "" {
		return nil
	}
	masterPath := filepath.Join(driveLinksDataDir, "drive", "drive_master_folders_list.json")
	if err := os.MkdirAll(filepath.Dir(masterPath), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(MasterFoldersData{Masters: masters}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(masterPath, data, 0644)
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
