package drive

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveHandlers holds Drive links dependencies (legacy compatibility layer)
type DriveHandlers struct {
	dataDir      string
	store        *store.SQLiteStore
	driveService *drive.Service
}

// NewDriveHandlers creates Drive handlers (legacy compatibility).
// driveSvc may be nil if Drive integration is not configured.
func NewDriveHandlers(cfg *drive.ServiceConfig, driveSvc *drive.Service) (*DriveHandlers, error) {
	dataDir := resolveDriveDataDir(cfg.TokensDir)
	storePath := filepath.Join(dataDir, "velox.db")
	sqliteStore, err := store.NewSQLiteStore(storePath)
	if err != nil {
		log.Printf("[WARN] Drive SQLite store init failed: %v", err)
		sqliteStore = nil
	}

	InitDriveLinksCache(dataDir, sqliteStore)
	driveTokensDir = cfg.TokensDir

	return &DriveHandlers{
		dataDir:      dataDir,
		store:        sqliteStore,
		driveService: driveSvc,
	}, nil
}

func resolveDriveDataDir(tokensDir string) string {
	if env := os.Getenv("VELOX_DATA_DIR"); strings.TrimSpace(env) != "" {
		return filepath.Clean(env)
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
	r.GET("/api/drive/links", GetDriveLinksHandler)
	r.GET("/api/drive/links/:group_name", GetDriveLinksByGroupHandler)
	r.POST("/api/drive/links/save", SaveDriveLinksHandler)
	r.POST("/api/drive/links/add", AddDriveFolderHandler)
	r.PUT("/api/drive/links/:folder_id", UpdateDriveFolderHandler)
	r.DELETE("/api/drive/links/:folder_id", DeleteDriveFolderHandler)
	r.GET("/api/drive/links/master", GetMasterFoldersHandler)

	// Drive Groups & Folders routes
	r.GET("/api/drive/groups", GetDriveGroupsHandler)
	r.GET("/api/drive/folders/list", GetDriveFoldersHandler)
	r.POST("/api/drive/folders/create", CreateDriveFolderHandler)
	r.GET("/api/drive/folders/group/:group_name", GroupFoldersHandler)
	r.GET("/api/drive/folders/clip", ClipFolderIDHandler)
	r.GET("/api/drive/files/list", DriveFilesHandler)
	r.POST("/api/drive/upload/text", UploadTextHandler)

	// Drive Tokens
	r.GET("/api/drive/tokens/list", ListDriveTokensHandler)

	// Drive Health Check
	r.GET("/api/drive/health", h.DriveHealthCheckHandler)

	log.Printf("[OK] Drive API routes registered at /api/drive/*")
}
