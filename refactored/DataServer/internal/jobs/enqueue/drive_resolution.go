package enqueue

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// ResolveDriveOutputFolderReference normalizes a user-provided Drive target.
// It accepts:
// - direct folder URLs
// - raw folder IDs
// - local aliases like "rap" stored in drive_master_folders metadata_json
// - exact folder names stored in drive_master_folders
func ResolveDriveOutputFolderReference(dataDir, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}

	if folderID := extractDriveFolderID(ref); folderID != "" {
		return folderID
	}

	dbPath := filepath.Join(strings.TrimSpace(dataDir), "velox.db")
	if _, err := os.Stat(dbPath); err != nil {
		return ref
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return ref
	}
	defer db.Close()

	normRef := normalizeDriveAlias(ref)
	rows, err := db.Query(`SELECT id, name, url, language, metadata_json FROM drive_master_folders`)
	if err != nil {
		return ref
	}
	defer rows.Close()

	for rows.Next() {
		var id, name, url, language, meta string
		if err := rows.Scan(&id, &name, &url, &language, &meta); err != nil {
			continue
		}
		if driveFolderMatches(ref, normRef, id, name, url, language, meta) {
			return id
		}
	}

	return ref
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
