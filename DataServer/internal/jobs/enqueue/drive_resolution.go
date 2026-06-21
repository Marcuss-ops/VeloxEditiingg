package enqueue

import (
	"context"
	"path/filepath"
	"strings"

	"velox-server/internal/store"
)

// ResolveDriveOutputFolderReference normalizes a user-provided Drive target.
// It accepts:
//   - direct folder URLs
//   - raw folder IDs
//   - local aliases like "rap" stored in drive_master_folders metadata_json
//   - exact folder names stored in drive_master_folders
//
// The resolver parameter may be nil; when nil only URL/ID extraction and
// filepath fallback are performed (no DB lookup). This keeps tests and
// offline callers simple while preserving the legacy dataDir default.
func ResolveDriveOutputFolderReference(ctx context.Context, dataDir string, resolver store.DriveFolderResolver, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return filepath.Join(dataDir, "output")
	}

	if folderID := extractDriveFolderID(ref); folderID != "" {
		return folderID
	}

	if resolver != nil {
		folders, err := resolver.ListMasterFolders(ctx)
		if err == nil {
			normRef := normalizeDriveAlias(ref)
			for _, f := range folders {
				if driveFolderMatches(ref, normRef, f.ID, f.Name, f.URL, f.Language, f.Metadata) {
					return f.ID
				}
			}
		}
	}

	if filepath.IsAbs(ref) {
		return ref
	}
	return filepath.Join(dataDir, ref)
}

func extractDriveFolderID(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if strings.Contains(ref, "drive.google.com/drive/folders/") {
		parts := strings.Split(strings.TrimRight(ref, "/"), "/")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	if !strings.Contains(ref, "://") && len(ref) > 15 {
		return ref
	}
	return ""
}

func normalizeDriveAlias(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func driveFolderMatches(rawRef, normRef, id, name, url, language, meta string) bool {
	if normRef == "" {
		return false
	}
	if normalizeDriveAlias(id) == normRef {
		return true
	}
	if normalizeDriveAlias(name) == normRef {
		return true
	}
	if normalizeDriveAlias(language) == normRef {
		return true
	}
	if normalizeDriveAlias(url) == normRef {
		return true
	}
	metaLower := strings.ToLower(meta)
	if strings.Contains(metaLower, normRef) {
		return true
	}
	if strings.Contains(strings.ToLower(rawRef), normRef) {
		return true
	}
	return false
}
