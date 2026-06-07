package drive

import (
	"sync"
	"time"

	"velox-server/internal/store"
)

// DriveFolder represents a Drive folder entry
type DriveFolder struct {
	ID              string `json:"id" yaml:"id"`
	Name            string `json:"name" yaml:"name"`
	Link            string `json:"link" yaml:"link"`
	ParentID        string `json:"parentId,omitempty" yaml:"parentId,omitempty"`
	Language        string `json:"language,omitempty" yaml:"language,omitempty"`
	CreatedAt       int64  `json:"createdAt,omitempty" yaml:"createdAt,omitempty"`
	UpdatedAt       int64  `json:"updatedAt,omitempty" yaml:"updatedAt,omitempty"`
	IsMaster        bool   `json:"isMaster,omitempty" yaml:"isMaster,omitempty"`
	SubfoldersCount int    `json:"subfoldersCount,omitempty" yaml:"subfoldersCount,omitempty"`
}

// DriveFoldersResponse is the API response
type DriveFoldersResponse struct {
	Success bool          `json:"success"`
	Folders []DriveFolder `json:"folders"`
	Count   int           `json:"count"`
}

// MasterFoldersData represents the master folders file structure
type MasterFoldersData struct {
	Masters map[string]MasterFolderInfo `json:"masters"`
}

// MasterFolderInfo represents a master folder entry
type MasterFolderInfo struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	URL             string            `json:"url"`
	SubfoldersCount int               `json:"subfolders_count"`
	Subfolders      []MasterSubfolder `json:"subfolders"`
}

// MasterSubfolder represents a subfolder in master
type MasterSubfolder struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// driveLinksCache holds cached data
type driveLinksCacheType struct {
	folders  []DriveFolder
	lastLoad time.Time
	mu       sync.RWMutex
}

var driveLinksCache driveLinksCacheType
var driveLinksDataDir string
var driveTokensDir string
var driveLinksStore *store.SQLiteStore

// InitDriveLinksCache initializes the cache with data directory and store
func InitDriveLinksCache(dataDirectory string, store *store.SQLiteStore) {
	driveLinksDataDir = dataDirectory
	driveLinksStore = store
	// Trigger initial load
	_ = loadDriveLinksFromDisk()
}
