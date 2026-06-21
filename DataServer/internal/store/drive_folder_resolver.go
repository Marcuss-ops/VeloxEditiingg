package store

import (
	"context"
	"fmt"
)

// DriveMasterFolder is one row from the drive_master_folders table.
type DriveMasterFolder struct {
	ID       string
	Name     string
	URL      string
	Language string
	Metadata string
}

// DriveFolderResolver abstracts lookup of Drive master folders.
type DriveFolderResolver interface {
	ListMasterFolders(ctx context.Context) ([]DriveMasterFolder, error)
}

// SQLiteDriveFolderResolver implements DriveFolderResolver against SQLiteStore.
type SQLiteDriveFolderResolver struct {
	store *SQLiteStore
}

// NewSQLiteDriveFolderResolver creates a resolver backed by store.
func NewSQLiteDriveFolderResolver(store *SQLiteStore) *SQLiteDriveFolderResolver {
	return &SQLiteDriveFolderResolver{store: store}
}

// Compile-time check.
var _ DriveFolderResolver = (*SQLiteDriveFolderResolver)(nil)

// ListMasterFolders selects all rows from drive_master_folders.
func (r *SQLiteDriveFolderResolver) ListMasterFolders(ctx context.Context) ([]DriveMasterFolder, error) {
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT id, name, url, language, metadata_json FROM drive_master_folders`)
	if err != nil {
		return nil, fmt.Errorf("drive folder resolver: ListMasterFolders: %w", err)
	}
	defer rows.Close()

	var out []DriveMasterFolder
	for rows.Next() {
		var f DriveMasterFolder
		if err := rows.Scan(&f.ID, &f.Name, &f.URL, &f.Language, &f.Metadata); err != nil {
			return nil, fmt.Errorf("drive folder resolver: scan: %w", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("drive folder resolver: rows: %w", err)
	}
	return out, nil
}
