// Package drive / service.go — core Drive service: types, constructor and
// accessors. Sub-domains live in folders.go, groups.go, tokens.go, master.go.
package drive

import (
	"errors"

	"velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveFolder represents a Drive folder entry
var (
	ErrFolderNotFound   = errors.New("drive: folder not found")
	ErrParentIDRequired = errors.New("drive: parent_id required")
)

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
