package drive

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
