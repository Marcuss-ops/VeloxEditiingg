package drive

import (
	"log"
	"path/filepath"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveHandlers holds Drive links dependencies
type DriveHandlers struct {
	dataDir      string
	store        *store.SQLiteStore
	driveService *drive.Service
	tokensDir    string
	cache        driveLinksCache
}

// NewDriveHandlers creates Drive handlers.
// driveSvc may be nil if Drive integration is not configured.
// sqliteStore must be the primary store (opened once at bootstrap).
func NewDriveHandlers(cfg *drive.ServiceConfig, driveSvc *drive.Service, sqliteStore *store.SQLiteStore) (*DriveHandlers, error) {
	dataDir := resolveDriveDataDir(cfg.TokensDir)

	h := &DriveHandlers{
		dataDir:      dataDir,
		store:        sqliteStore,
		driveService: driveSvc,
		tokensDir:    cfg.TokensDir,
	}

	// Initialize cache from SQLite
	_ = h.loadFromDisk()

	return h, nil
}

// SetSQLiteStore wires (or rewires) the SQLite store post-construction.
func (h *DriveHandlers) SetSQLiteStore(s *store.SQLiteStore) {
	if h == nil {
		return
	}
	h.store = s
	if s != nil {
		_ = h.loadFromDisk()
	}
}

func resolveDriveDataDir(tokensDir string) string {
	if dir := config.GetDataDir(); dir != "" {
		return filepath.Clean(dir)
	}

	cleaned := filepath.Clean(tokensDir)
	if cleaned == "." || cleaned == string(filepath.Separator) {
		return cleaned
	}

	if filepath.Base(cleaned) == "tokens" {
		parent := filepath.Dir(cleaned)
		if filepath.Base(parent) == "drive" {
			grandParent := filepath.Dir(parent)
			if filepath.Base(grandParent) == "secrets" {
				return filepath.Dir(grandParent)
			}
			return grandParent
		}
		return filepath.Dir(parent)
	}

	return filepath.Dir(cleaned)
}

// RegisterDriveRoutes registers Drive Links routes
func RegisterDriveRoutes(r *gin.Engine, h *DriveHandlers) {
	// Drive Links CRUD routes
	r.GET("/api/drive/links", h.GetDriveLinksHandler)
	r.GET("/api/drive/links/:group_name", h.GetDriveLinksByGroupHandler)
	r.POST("/api/drive/links/save", h.SaveDriveLinksHandler)
	r.POST("/api/drive/links/add", h.AddDriveFolderHandler)
	r.PUT("/api/drive/links/:folder_id", h.UpdateDriveFolderHandler)
	r.DELETE("/api/drive/links/:folder_id", h.DeleteDriveFolderHandler)
	r.GET("/api/drive/links/master", h.GetMasterFoldersHandler)
	r.POST("/api/drive/links/master/upsert", h.UpsertMasterFolderHandler)
	r.GET("/api/drive/oauth/start", h.DriveOAuthStartHandler)
	r.GET("/api/drive/oauth/callback", h.DriveOAuthCallbackHandler)

	// Drive Groups & Folders routes
	r.GET("/api/drive/groups", h.GetDriveGroupsHandler)
	r.GET("/api/drive/folders/list", h.GetDriveFoldersHandler)
	r.POST("/api/drive/folders/create", h.CreateDriveFolderHandler)
	r.GET("/api/drive/folders/group/:group_name", h.GroupFoldersHandler)
	r.GET("/api/drive/folders/clip", h.ClipFolderIDHandler)
	r.GET("/api/drive/files/list", h.DriveFilesHandler)
	r.POST("/api/drive/upload/text", h.UploadTextHandler)
	r.GET("/api/drive/outros", h.ListOutroFoldersHandler)
	r.GET("/api/drive/outros/:language", h.GetOutroFolderContentsHandler)
	r.GET("/api/drive/outros-by-id/:folder_id", h.GetOutroFolderContentsByIDHandler)

	// Drive Tokens
	r.GET("/api/drive/tokens/list", h.ListDriveTokensHandler)

	// Drive Health Check
	r.GET("/api/drive/health", h.DriveHealthCheckHandler)

	log.Printf("[OK] Drive API routes registered at /api/drive/*")
}
