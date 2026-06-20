package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveFolder represents a Drive folder entry
type DriveFolder struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Link            string `json:"link"`
	ParentID        string `json:"parentId,omitempty"`
	Language        string `json:"language,omitempty"`
	CreatedAt       int64  `json:"createdAt,omitempty"`
	UpdatedAt       int64  `json:"updatedAt,omitempty"`
	IsMaster        bool   `json:"isMaster,omitempty"`
	SubfoldersCount int    `json:"subfoldersCount,omitempty"`
}

// MasterFolderInfo represents a master folder entry
type MasterFolderInfo struct {
	ID              string        `json:"id"`
	Name            string        `json:"name"`
	URL             string        `json:"url"`
	SubfoldersCount int           `json:"subfolders_count"`
	Subfolders      []interface{} `json:"subfolders"`
	MetadataJSON    string        `json:"metadata_json,omitempty"`
}

// TokenFile represents a Drive token file entry
type TokenFile struct {
	Name string `json:"name"`
	Path string `json:"path,omitempty"`
}

// Request types
type SaveDriveLinksRequest struct {
	Folders []DriveFolder `json:"folders"`
}

type AddDriveFolderRequest struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Link     string `json:"link"`
	ParentID string `json:"parentId,omitempty"`
	Language string `json:"language,omitempty"`
}

type UpdateDriveFolderRequest struct {
	Name     string `json:"name,omitempty"`
	Link     string `json:"link,omitempty"`
	ParentID string `json:"parentId,omitempty"`
	Language string `json:"language,omitempty"`
}

type CreateDriveFolderRequest struct {
	Name     string `json:"name"`
	ParentID string `json:"parentId,omitempty"`
	Language string `json:"language,omitempty"`
}

type UploadTextRequest struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
	ParentID string `json:"parentId,omitempty"`
}

type UpsertMasterFolderRequest struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	URL             string `json:"url"`
	Language        string `json:"language,omitempty"`
	SubfoldersCount int    `json:"subfolders_count,omitempty"`
	MetadataJSON    string `json:"metadata_json,omitempty"`
}

// Group mappings
var groupToClipFolder = map[string]string{
	"wwe":    "WWE",
	"hiphop": "Hip Hop",
	"news":   "News",
	"tech":   "Tech",
}

var groupToVoiceoverFolder = map[string]string{
	"wwe":    "WWE Voice",
	"hiphop": "Hip Hop Voice",
	"news":   "News Voice",
	"tech":   "Tech Voice",
}

var stockFolderAliases = map[string]string{
	"wwe":    "WWE Stock",
	"hiphop": "Hip Hop Stock",
	"news":   "News Stock",
	"tech":   "Tech Stock",
}

type linksCache struct {
	folders  []DriveFolder
	lastLoad time.Time
	mu       sync.RWMutex
}

// Service holds Drive business operations
type Service struct {
	store        *store.SQLiteStore
	driveService *drive.Service
	tokensDir    string
	dataDir      string
	cache        linksCache
}

// New creates a new Drive service
func New(tokensDir, dataDir string, driveService *drive.Service, sqliteStore *store.SQLiteStore) *Service {
	s := &Service{
		store:        sqliteStore,
		driveService: driveService,
		tokensDir:    tokensDir,
		dataDir:      dataDir,
	}
	_ = s.loadFromDisk()
	return s
}

func (s *Service) DriveService() *drive.Service {
	return s.driveService
}

func (s *Service) SetStore(st *store.SQLiteStore) {
	s.store = st
	_ = s.loadFromDisk()
}

func (s *Service) Store() *store.SQLiteStore {
	return s.store
}

func (s *Service) TokensDir() string {
	return s.tokensDir
}

// getLinks returns folders from cache with 30s TTL.
func (s *Service) getLinks() []DriveFolder {
	s.cache.mu.RLock()
	if time.Since(s.cache.lastLoad) < 30*time.Second && len(s.cache.folders) > 0 {
		folders := make([]DriveFolder, len(s.cache.folders))
		copy(folders, s.cache.folders)
		s.cache.mu.RUnlock()
		return folders
	}
	s.cache.mu.RUnlock()

	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()

	_ = s.loadFromDisk()
	return s.cache.folders
}

// loadFromDisk loads folders from SQLite.
func (s *Service) loadFromDisk() error {
	if s.store != nil {
		dbFolders, err := s.store.ListDriveLinks()
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
			s.cache.folders = folders
			s.cache.lastLoad = time.Now()
			return nil
		}
		s.cache.folders = nil
		s.cache.lastLoad = time.Now()
	}
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
func (s *Service) GetDriveLinks() []DriveFolder {
	return s.getLinks()
}

// GetDriveLinksByGroup returns links filtered by group name
func (s *Service) GetDriveLinksByGroup(groupName string) []DriveFolder {
	folders := s.getLinks()
	var filtered []DriveFolder
	groupLower := strings.ToLower(groupName)
	for _, f := range folders {
		nameLower := strings.ToLower(f.Name)
		langLower := strings.ToLower(f.Language)
		if strings.HasPrefix(nameLower, groupLower) || langLower == groupLower {
			filtered = append(filtered, f)
		}
	}
	return filtered
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
	folders := s.getLinks()

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
	folders := s.getLinks()
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
	return fmt.Errorf("folder not found")
}

// DeleteDriveFolder deletes a folder and its children
func (s *Service) DeleteDriveFolder(folderID string) (int, error) {
	folders := s.getLinks()
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

// CreateDriveFolder creates a synthetic folder entry
func (s *Service) CreateDriveFolder(req CreateDriveFolderRequest) (string, error) {
	folders := s.getLinks()
	newID := fmt.Sprintf("folder_%d", time.Now().UnixNano())
	newFolder := DriveFolder{
		ID:        newID,
		Name:      req.Name,
		Link:      fmt.Sprintf("https://drive.google.com/drive/folders/%s", newID),
		ParentID:  req.ParentID,
		Language:  req.Language,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		IsMaster:  false,
	}
	folders = append(folders, newFolder)

	if err := s.saveToDisk(folders); err != nil {
		return "", err
	}
	s.updateCache(folders)
	return newID, nil
}

// UploadText simulates a file upload
func (s *Service) UploadText(req UploadTextRequest) (string, error) {
	return fmt.Sprintf("https://drive.google.com/file/d/text_%d/view", time.Now().UnixNano()), nil
}

// GetMasterFolders returns master folders
func (s *Service) GetMasterFolders() (map[string]interface{}, error) {
	masters := make(map[string]interface{})
	if s.store != nil {
		dbMasters, err := s.store.ListMasterFolders()
		if err == nil && len(dbMasters) > 0 {
			for _, m := range dbMasters {
				language, _ := m["language"].(string)
				if language == "" {
					continue
				}
				masters[language] = m
			}
		}
	}
	return masters, nil
}

// UpsertMasterFolder upserts a master folder
func (s *Service) UpsertMasterFolder(req UpsertMasterFolderRequest) error {
	if s.store == nil {
		return fmt.Errorf("drive store not initialized")
	}
	return s.store.UpsertMasterFolder(req.ID, req.Name, req.URL, req.Language, req.SubfoldersCount)
}

// GetDriveGroups builds group structures
func (s *Service) GetDriveGroups() (map[string]interface{}, error) {
	folders := s.getLinks()
	groups := make(map[string]interface{})

	for group, clipName := range groupToClipFolder {
		clipID := findMasterIDByName(folders, []string{clipName, group})
		voiceoverID := findMasterIDByName(folders, []string{groupToVoiceoverFolder[group], group + " Voice"})
		stockID := findMasterIDByName(folders, []string{stockFolderAliases[group], group + " Stock"})

		if clipID != "" || voiceoverID != "" || stockID != "" {
			groups[group] = map[string]interface{}{
				"clip":      clipID,
				"voiceover": voiceoverID,
				"stock":     stockID,
			}
		}
	}
	return groups, nil
}

// GetDriveFolders lists child folders
func (s *Service) GetDriveFolders(parentID string) ([]DriveFolder, error) {
	folders := s.getLinks()
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

// GroupFolders resolves group folder mappings
func (s *Service) GroupFolders(groupName string) (map[string]interface{}, error) {
	folders := s.getLinks()
	result := make(map[string]interface{})

	if clipName, ok := groupToClipFolder[groupName]; ok {
		result["clip"] = findMasterIDByName(folders, []string{clipName, groupName})
	}
	if stockName, ok := stockFolderAliases[groupName]; ok {
		result["stock"] = findMasterIDByName(folders, []string{stockName, groupName + " Stock"})
	}
	if voiceoverName, ok := groupToVoiceoverFolder[groupName]; ok {
		result["voiceover"] = findMasterIDByName(folders, []string{voiceoverName, groupName + " Voice"})
	}
	return result, nil
}

// ClipFolderID finds folder ID by name or group
func (s *Service) ClipFolderID(folderName, group string) (map[string]interface{}, error) {
	folders := s.getLinks()

	if folderName != "" {
		for _, f := range folders {
			if normalizeName(f.Name) == normalizeName(folderName) {
				return map[string]interface{}{
					"id":   f.ID,
					"name": f.Name,
				}, nil
			}
		}
	}

	if group != "" {
		if clipName, ok := groupToClipFolder[group]; ok {
			clipID := findMasterIDByName(folders, []string{clipName, group})
			if clipID != "" {
				return map[string]interface{}{
					"id":    clipID,
					"group": group,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("folder not found")
}

// DriveFiles returns folder contents matching parent_id
func (s *Service) DriveFiles(parentID string) ([]DriveFolder, error) {
	if parentID == "" {
		return nil, fmt.Errorf("parent_id required")
	}

	folders := s.getLinks()
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

// ListDriveTokens lists token files
func (s *Service) ListDriveTokens() ([]TokenFile, error) {
	if s.tokensDir == "" {
		return []TokenFile{}, nil
	}

	entries, err := os.ReadDir(s.tokensDir)
	if err != nil {
		return []TokenFile{}, nil
	}

	var files []TokenFile
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, TokenFile{
				Name: entry.Name(),
				Path: filepath.Join(s.tokensDir, entry.Name()),
			})
		}
	}
	return files, nil
}

// ListOutroFolders retrieves all detailed master folder entries from the database.
func (s *Service) ListOutroFolders() ([]map[string]any, error) {
	if s.store == nil {
		return nil, fmt.Errorf("drive store not configured")
	}
	return s.store.ListMasterFoldersDetailed()
}

// FindMasterFolderByLanguage retrieves a master folder by its language tag.
func (s *Service) FindMasterFolderByLanguage(language string) (map[string]any, error) {
	if s.store == nil {
		return nil, fmt.Errorf("drive store not configured")
	}
	return s.store.FindMasterFolderByLanguage(language)
}

// ListFiles lists files in a Google Drive folder using the integration service.
func (s *Service) ListFiles(ctx context.Context, folderID string, pageSize int) ([]drive.File, error) {
	if s.driveService == nil {
		return nil, fmt.Errorf("drive service not configured")
	}
	return s.driveService.ListFiles(ctx, folderID, pageSize)
}

// Helpers
func findMasterIDByName(folders []DriveFolder, names []string) string {
	for _, name := range names {
		normName := normalizeName(name)
		for _, f := range folders {
			if f.ParentID == "" && normalizeName(f.Name) == normName {
				return f.ID
			}
		}
	}
	return ""
}

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
