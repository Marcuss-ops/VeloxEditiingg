package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (s *SQLiteStore) ReplaceDriveLinks(rawList []byte) error {
	var list []map[string]any
	if err := json.Unmarshal(rawList, &list); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("DELETE FROM drive_links"); err != nil {
		return err
	}
	now := nowRFC3339()
	for _, f := range list {
		id := asString(f["id"])
		if id == "" {
			continue
		}
		created := toISO(f["createdAt"])
		updated := toISO(f["updatedAt"])
		raw, _ := json.Marshal(f)
		if _, err := tx.Exec(
			`INSERT INTO drive_links (id, parent_id, name, link, language, created_at, updated_at, raw_json, migrated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, asString(f["parentId"]), asString(f["name"]), asString(f["link"]), asString(f["language"]),
			created, updated, string(raw), now,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteStore) ListDriveLinks() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT raw_json FROM drive_links ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("list drive links scan: %w", err)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			return nil, fmt.Errorf("list drive links decode: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list drive links iterate: %w", err)
	}
	return out, nil
}

// MasterFolders: structured master folder CRUD

// ResolveDriveFolderReference resolves a user-provided Drive target against
// the canonical master-folder table. The store owns the SQL lookup so callers
// do not need a borrowed *sql.DB. Direct folder IDs and URLs are handled
// without a query; aliases and names are matched against the persisted row.
func (s *SQLiteStore) ResolveDriveFolderReference(ctx context.Context, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", nil
	}
	if folderID := extractDriveFolderID(ref); folderID != "" {
		return folderID, nil
	}
	if s == nil || s.db == nil {
		return "", fmt.Errorf("store: resolve drive folder: store is not configured")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	normRef := normalizeDriveAlias(ref)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, url, language, metadata_json FROM drive_master_folders`)
	if err != nil {
		return "", fmt.Errorf("store: resolve drive folder query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, name, url, language, metadataJSON string
		if err := rows.Scan(&id, &name, &url, &language, &metadataJSON); err != nil {
			return "", fmt.Errorf("store: resolve drive folder scan: %w", err)
		}
		if driveFolderMatches(ref, normRef, id, name, url, language, metadataJSON) {
			return id, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("store: resolve drive folder rows: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return ref, nil
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

func normalizeDriveAlias(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func driveFolderMatches(rawRef, normRef, id, name, url, language, metadataJSON string) bool {
	if normRef == "" {
		return false
	}
	if normalizeDriveAlias(id) == normRef ||
		normalizeDriveAlias(name) == normRef ||
		normalizeDriveAlias(language) == normRef ||
		normalizeDriveAlias(url) == normRef {
		return true
	}
	metadataLower := strings.ToLower(metadataJSON)
	return strings.Contains(metadataLower, normRef) ||
		strings.Contains(strings.ToLower(rawRef), normRef)
}

// UpsertMasterFolder creates or updates a master folder.
func (s *SQLiteStore) UpsertMasterFolder(id, name, url, language string, subfoldersCount int, metadataJSON ...string) error {
	now := nowRFC3339()
	meta := ""
	if len(metadataJSON) > 0 {
		meta = metadataJSON[0]
	}
	_, err := s.db.Exec(
		`INSERT INTO drive_master_folders (id, name, url, subfolders_count, language, metadata_json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   name=excluded.name, url=excluded.url, subfolders_count=excluded.subfolders_count,
		   language=excluded.language, metadata_json=excluded.metadata_json, updated_at=excluded.updated_at`,
		id, name, url, subfoldersCount, language, meta, now, now,
	)
	return err
}

// ListMasterFolders returns all master folders.
func (s *SQLiteStore) ListMasterFolders() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT id, name, url, subfolders_count, language, created_at, updated_at, metadata_json FROM drive_master_folders ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var id, name, url, language, createdAt, updatedAt, metadataJSON string
		var subfoldersCount int
		if err := rows.Scan(&id, &name, &url, &subfoldersCount, &language, &createdAt, &updatedAt, &metadataJSON); err != nil {
			return nil, fmt.Errorf("list master folders scan: %w", err)
		}
		result = append(result, map[string]any{
			"id": id, "name": name, "url": url,
			"subfolders_count": subfoldersCount, "language": language,
			"created_at": createdAt, "updated_at": updatedAt,
		})
	}
	return result, rows.Err()
}

// ListMasterFoldersDetailed returns all master folders with full metadata.
func (s *SQLiteStore) ListMasterFoldersDetailed() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT id, name, url, subfolders_count, language, metadata_json, created_at, updated_at FROM drive_master_folders ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var id, name, url, language, metadataJSON, createdAt, updatedAt string
		var subfoldersCount int
		if err := rows.Scan(&id, &name, &url, &subfoldersCount, &language, &metadataJSON, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("list master folders detailed scan: %w", err)
		}
		m := map[string]any{
			"id": id, "name": name, "url": url,
			"subfolders_count": subfoldersCount, "language": language,
			"created_at": createdAt, "updated_at": updatedAt,
		}
		if metadataJSON != "" {
			var meta map[string]any
			if err := json.Unmarshal([]byte(metadataJSON), &meta); err != nil {
				return nil, fmt.Errorf("list master folders detailed metadata decode: %w", err)
			}
			m["metadata"] = meta
			if t, ok := meta["type"].(string); ok {
				m["type"] = t
			}
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// FindMasterFolderByLanguage returns the master folder for a given language.
func (s *SQLiteStore) FindMasterFolderByLanguage(language string) (map[string]any, error) {
	if language == "" {
		return nil, nil
	}
	row := s.db.QueryRow(`SELECT id, name, url, subfolders_count, language, metadata_json, created_at, updated_at FROM drive_master_folders WHERE LOWER(language) = LOWER(?) LIMIT 1`, language)
	var id, name, url, lang, metadataJSON, createdAt, updatedAt string
	var subfoldersCount int
	if err := row.Scan(&id, &name, &url, &subfoldersCount, &lang, &metadataJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("find master folder by language: %w", err)
	}
	m := map[string]any{
		"id": id, "name": name, "url": url,
		"subfolders_count": subfoldersCount, "language": lang,
		"created_at": createdAt, "updated_at": updatedAt,
	}
	if metadataJSON != "" {
		var meta map[string]any
		if err := json.Unmarshal([]byte(metadataJSON), &meta); err != nil {
			return nil, fmt.Errorf("find master folder metadata decode: %w", err)
		}
		m["metadata"] = meta
		if t, ok := meta["type"].(string); ok {
			m["type"] = t
		}
	}
	return m, nil
}
