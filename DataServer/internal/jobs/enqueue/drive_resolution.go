// sql-allowlist: jobs/enqueue Drive resolution helper — read-only lookup of drive_master_folders aliases inside the enqueue path. Resolves a user-provided Drive reference (URL / folder ID / alias / name) to a canonical folder_id. Future refactor candidate for move into store as a typed DriveFolderRepository.

package enqueue

import (
	"context"
	"database/sql"
	"strings"
)

// ResolveDriveOutputFolderReference normalizes a user-provided Drive target.
// It accepts:
// - direct folder URLs
// - raw folder IDs
// - local aliases like "rap" stored in drive_master_folders metadata_json
// - exact folder names stored in drive_master_folders
//
// ResolveDriveOutputFolderReference resolves a Drive folder reference using a
// borrowed, process-owned SQLite handle when one is supplied. The dataDir
// argument remains for source compatibility with older callers, but is no
// longer used to discover or open a database. Without a DB, database-backed
// aliases fail closed by returning the original reference unchanged.
func ResolveDriveOutputFolderReference(dataDir, ref string, dbs ...*sql.DB) string {
	_ = dataDir
	var db *sql.DB
	if len(dbs) > 0 {
		db = dbs[0]
	}
	return resolveDriveOutputFolderReference(context.Background(), db, ref)
}

// resolveDriveOutputFolderReference performs the lookup with explicit
// ownership and context. db is borrowed and must never be closed here.
func resolveDriveOutputFolderReference(ctx context.Context, db *sql.DB, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}

	if folderID := extractDriveFolderID(ref); folderID != "" {
		return folderID
	}
	if db == nil {
		return ref
	}

	normRef := normalizeDriveAlias(ref)
	rows, err := db.QueryContext(ctx, `SELECT id, name, url, language, metadata_json FROM drive_master_folders`)
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
	if err := rows.Err(); err != nil {
		return ref
	}

	return ref
}

func extractDriveFolderID(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if strings.Contains(ref, "drive.google.com") && strings.Contains(ref, "/folders/") {
		parts := strings.Split(strings.TrimRight(ref, "/"), "/folders/")
		if len(parts) == 2 {
			id := strings.TrimSpace(strings.SplitN(parts[1], "?", 2)[0])
			if id != "" {
				return id
			}
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
