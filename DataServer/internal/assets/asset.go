package assets

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// Asset statuses.
const (
	AssetStatusStaging      = "STAGING"
	AssetStatusReady        = "READY"
	AssetStatusQuarantined  = "QUARANTINED"
	AssetStatusDeleted      = "DELETED"
)

// AssetRoles are the canonical roles a job can bind to an asset.
const (
	RoleVoiceover    = "voiceover"
	RoleSceneImage   = "scene_image"
	RoleStockClip    = "stock_clip"
	RoleMusic        = "music"
	RoleSubtitle     = "subtitle"
	RoleFont         = "font"
	RoleThumbnail    = "thumbnail"
	RoleProjectFile  = "project_file"
	RoleOverlay      = "overlay"
	RoleLogo         = "logo"
)

// Asset is the canonical domain record for a content-addressed asset.
type Asset struct {
	AssetID         string `json:"asset_id"`
	Kind            string `json:"kind"`
	Status          string `json:"status"`
	SHA256          string `json:"sha256"`
	MimeType        string `json:"mime_type,omitempty"`
	SizeBytes       int64  `json:"size_bytes"`
	StorageProvider string `json:"storage_provider"`
	StorageKey      string `json:"storage_key"`
	MetadataJSON    string `json:"metadata_json,omitempty"`
	CreatedAt       string `json:"created_at"`
	VerifiedAt      string `json:"verified_at,omitempty"`
	DeletedAt       string `json:"deleted_at,omitempty"`
}

// AssetSource tracks where an asset was resolved from.
type AssetSource struct {
	SourceID        string `json:"source_id"`
	AssetID         string `json:"asset_id"`
	SourceType      string `json:"source_type"`
	SourceReference string `json:"source_reference"`
	SourceAccountID string `json:"source_account_id,omitempty"`
	MetadataJSON    string `json:"metadata_json,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// JobAsset binds an asset to a job with a role and ordinal.
type JobAsset struct {
	JobID     string `json:"job_id"`
	AssetID   string `json:"asset_id"`
	Role      string `json:"role"`
	Ordinal   int    `json:"ordinal"`
	Required  bool   `json:"required"`
	CreatedAt string `json:"created_at"`
}

// ResolveAssetCommand carries the inputs for ResolveAndRegister.
type ResolveAssetCommand struct {
	Kind         string // voiceover, scene_image, music, etc.
	Reference    string // original source reference (URL, path, velox-asset://)
	SourceType   string // override source type classification
	MetadataJSON string // optional extra metadata
}

// Reference returns the canonical velox-asset:// URI for this asset.
func (a *Asset) Reference() string {
	if a == nil || strings.TrimSpace(a.AssetID) == "" {
		return ""
	}
	return VeloxAssetScheme + "://" + a.AssetID
}

// IsTerminal reports whether this asset has finished its lifecycle.
func (a *Asset) IsTerminal() bool {
	switch a.Status {
	case AssetStatusQuarantined, AssetStatusDeleted:
		return true
	}
	return false
}

// ComputeSHA256 reads r fully and returns the hex-encoded SHA-256 hash.
func ComputeSHA256(r io.Reader) (string, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", fmt.Errorf("compute sha256: %w", err)
	}
	if n == 0 {
		return "", fmt.Errorf("compute sha256: empty input")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// AssetRecord is the storage projection of an assets row.
type AssetRecord struct {
	AssetID         string
	Kind            string
	Status          string
	SHA256          string
	MimeType        string
	SizeBytes       int64
	StorageProvider string
	StorageKey      string
	MetadataJSON    string
	CreatedAt       string
	VerifiedAt      string
	DeletedAt       string
}

// AssetSourceRecord is the storage projection of an asset_sources row.
type AssetSourceRecord struct {
	SourceID        string
	AssetID         string
	SourceType      string
	SourceReference string
	SourceAccountID string
	MetadataJSON    string
	CreatedAt       string
}
