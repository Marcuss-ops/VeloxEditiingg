package store

import (
	"encoding/json"
	"strings"
	"time"

	"velox-server/internal/logging"
)

// Package-level structured logger for drive link migration warnings.
var storeLog = logging.NewLogger("store.drive_links")

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
	now := time.Now().UTC().Format(time.RFC3339)
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
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

// UpsertDriveLink creates or updates a single drive link.
func (s *SQLiteStore) UpsertDriveLink(id, parentID, name, link, language string, isMaster bool, subfoldersCount int, createdAt int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	created := time.UnixMilli(createdAt).UTC().Format(time.RFC3339)
	masterInt := 0
	if isMaster {
		masterInt = 1
	}
	raw, _ := json.Marshal(map[string]any{
		"id": id, "parentId": parentID, "name": name, "link": link,
		"language": language, "createdAt": createdAt, "isMaster": isMaster,
	})
	_, err := s.db.Exec(
		`INSERT INTO drive_links (id, parent_id, name, link, language, is_master, subfolders_count, created_at, updated_at, raw_json, migrated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   parent_id=excluded.parent_id, name=excluded.name, link=excluded.link,
		   language=excluded.language, is_master=excluded.is_master, subfolders_count=excluded.subfolders_count,
		   updated_at=excluded.updated_at, raw_json=excluded.raw_json, migrated_at=excluded.migrated_at`,
		id, parentID, name, link, language, masterInt, subfoldersCount, created, now, string(raw), now,
	)
	return err
}

// DeleteDriveLink removes a single drive link by ID.
func (s *SQLiteStore) DeleteDriveLink(id string) error {
	_, err := s.db.Exec(`DELETE FROM drive_links WHERE id = ?`, id)
	return err
}

// DeleteDriveLinksByParent removes all children of a parent folder.
func (s *SQLiteStore) DeleteDriveLinksByParent(parentID string) (int64, error) {
	result, err := s.db.Exec(`DELETE FROM drive_links WHERE parent_id = ?`, parentID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// GetDriveLink returns a single drive link by ID.
func (s *SQLiteStore) GetDriveLink(id string) (map[string]any, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM drive_links WHERE id = ?`, id).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// MasterFolders: structured master folder CRUD

// UpsertMasterFolder creates or updates a master folder.
func (s *SQLiteStore) UpsertMasterFolder(id, name, url, language string, subfoldersCount int, metadataJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(metadataJSON) == "" {
		metadataJSON = "{}"
	}
	_, err := s.db.Exec(
		`INSERT INTO drive_master_folders (id, name, url, subfolders_count, language, created_at, updated_at, metadata_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   name=excluded.name, url=excluded.url, subfolders_count=excluded.subfolders_count,
		   language=excluded.language, updated_at=excluded.updated_at, metadata_json=excluded.metadata_json`,
		id, name, url, subfoldersCount, language, now, now, metadataJSON,
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
			continue
		}
		result = append(result, map[string]any{
			"id": id, "name": name, "url": url,
			"subfolders_count": subfoldersCount, "language": language,
			"created_at": createdAt, "updated_at": updatedAt,
		})
	}
	return result, rows.Err()
}

// ListMasterFoldersDetailed returns all master folders including metadata_json.
func (s *SQLiteStore) ListMasterFoldersDetailed() ([]map[string]any, error) {
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
			continue
		}
		result = append(result, map[string]any{
			"id":               id,
			"name":             name,
			"url":              url,
			"subfolders_count": subfoldersCount,
			"language":         language,
			"created_at":       createdAt,
			"updated_at":       updatedAt,
			"metadata_json":    metadataJSON,
		})
	}
	return result, rows.Err()
}

// FindMasterFolderByLanguage returns the first master folder matching the given language.
func (s *SQLiteStore) FindMasterFolderByLanguage(language string) (map[string]any, error) {
	language = strings.TrimSpace(strings.ToLower(language))
	if language == "" {
		return nil, nil
	}

	rows, err := s.ListMasterFoldersDetailed()
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		rowLanguage := strings.TrimSpace(strings.ToLower(asString(row["language"])))
		if rowLanguage == language {
			return row, nil
		}

		meta := strings.ToLower(asString(row["metadata_json"]))
		if strings.Contains(meta, `"type":"outro"`) && strings.Contains(meta, `"language":"`+language+`"`) {
			return row, nil
		}
	}

	return nil, nil
}

// DeleteMasterFolder removes a master folder by ID.
func (s *SQLiteStore) DeleteMasterFolder(id string) error {
	_, err := s.db.Exec(`DELETE FROM drive_master_folders WHERE id = ?`, id)
	return err
}

// MigrateDriveLinksFromJSON imports drive_links.json data into SQLite.
// It's idempotent: re-importing the same data won't create duplicates.
func (s *SQLiteStore) MigrateDriveLinksFromJSON(folders []map[string]any) (int, error) {
	if len(folders) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	stmt, err := tx.Prepare(
		`INSERT INTO drive_links (id, parent_id, name, link, language, is_master, subfolders_count, created_at, updated_at, raw_json, migrated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   parent_id=excluded.parent_id, name=excluded.name, link=excluded.link,
		   language=excluded.language, is_master=excluded.is_master, subfolders_count=excluded.subfolders_count,
		   updated_at=excluded.updated_at, raw_json=excluded.raw_json, migrated_at=excluded.migrated_at`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for _, f := range folders {
		id := asString(f["id"])
		if id == "" {
			continue
		}
		parentID := asString(f["parentId"])
		if v, ok := f["parent_id"].(string); ok && parentID == "" {
			parentID = v
		}
		name := asString(f["name"])
		link := asString(f["link"])
		language := asString(f["language"])

		var isMaster bool
		if v, ok := f["isMaster"].(bool); ok {
			isMaster = v
		}
		masterInt := 0
		if isMaster {
			masterInt = 1
		}

		var subfoldersCount int
		if v, ok := f["subfoldersCount"].(float64); ok {
			subfoldersCount = int(v)
		}

		createdAt := ""
		if v, ok := f["createdAt"].(float64); ok {
			createdAt = time.UnixMilli(int64(v)).UTC().Format(time.RFC3339)
		}
		updatedAt := now

		raw, _ := json.Marshal(f)
		if _, err := stmt.Exec(id, parentID, name, link, language, masterInt, subfoldersCount, createdAt, updatedAt, string(raw), now); err != nil {
			storeLog.WarnWithMsg("drive_link_migrate_skip",
				"Skipping drive link during migration",
				map[string]interface{}{
					"id":  id,
					"err": err.Error(),
				})
			continue
		}
		count++
	}
	return count, tx.Commit()
}

// MigrateMasterFoldersFromJSON imports drive_master_folders_list.json data into SQLite.
func (s *SQLiteStore) MigrateMasterFoldersFromJSON(masters map[string]any) (int, error) {
	if len(masters) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	stmt, err := tx.Prepare(
		`INSERT INTO drive_master_folders (id, name, url, subfolders_count, language, created_at, updated_at, metadata_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   name=excluded.name, url=excluded.url, subfolders_count=excluded.subfolders_count,
		   language=excluded.language, updated_at=excluded.updated_at, metadata_json=excluded.metadata_json`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for language, info := range masters {
		infoMap, _ := info.(map[string]any)
		if infoMap == nil {
			continue
		}
		id := asString(infoMap["id"])
		if id == "" {
			continue
		}
		name := asString(infoMap["name"])
		url := asString(infoMap["url"])
		var subfoldersCount int
		if v, ok := infoMap["subfolders_count"].(float64); ok {
			subfoldersCount = int(v)
		}
		raw, _ := json.Marshal(infoMap)
		if _, err := stmt.Exec(id, name, url, subfoldersCount, language, now, now, string(raw)); err != nil {
			storeLog.WarnWithMsg("master_folder_migrate_skip",
				"Skipping master folder during migration",
				map[string]interface{}{
					"id":  id,
					"err": err.Error(),
				})
			continue
		}
		count++
	}
	return count, tx.Commit()
}
