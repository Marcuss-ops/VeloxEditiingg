package store

import (
	"encoding/json"
	"time"
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
