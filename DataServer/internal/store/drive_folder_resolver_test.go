package store

import (
	"context"
	"testing"
)

func TestDriveFolderResolver_ListMasterFolders(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		seed     []DriveMasterFolder
		wantLen  int
		wantFirst DriveMasterFolder
	}{
		{
			name: "returns all master folders ordered by id",
			seed: []DriveMasterFolder{
				{ID: "folder-1", Name: "English", URL: "https://drive/f1", Language: "en", Metadata: `{"key":"val1"}`},
				{ID: "folder-2", Name: "Italian", URL: "https://drive/f2", Language: "it", Metadata: `{"key":"val2"}`},
				{ID: "folder-3", Name: "Spanish", URL: "https://drive/f3", Language: "es", Metadata: `{"key":"val3"}`},
			},
			wantLen: 3,
			wantFirst: DriveMasterFolder{ID: "folder-1", Name: "English", URL: "https://drive/f1", Language: "en", Metadata: `{"key":"val1"}`},
		},
		{
			name:     "empty table returns empty slice",
			seed:     nil,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteDriveFolderResolver(s)

			for _, f := range tt.seed {
				_, err := s.db.ExecContext(ctx,
					`INSERT INTO drive_master_folders (id, name, url, language, metadata_json) VALUES (?, ?, ?, ?, ?)`,
					f.ID, f.Name, f.URL, f.Language, f.Metadata)
				if err != nil {
					t.Fatalf("seed folder %s: %v", f.ID, err)
				}
			}

			got, err := repo.ListMasterFolders(ctx)
			if err != nil {
				t.Fatalf("ListMasterFolders error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("len = %v, want %v", len(got), tt.wantLen)
			}
			if tt.wantLen > 0 {
				first := got[0]
				if first.ID != tt.wantFirst.ID {
					t.Errorf("first.ID = %q, want %q", first.ID, tt.wantFirst.ID)
				}
				if first.Name != tt.wantFirst.Name {
					t.Errorf("first.Name = %q, want %q", first.Name, tt.wantFirst.Name)
				}
				if first.Language != tt.wantFirst.Language {
					t.Errorf("first.Language = %q, want %q", first.Language, tt.wantFirst.Language)
				}
			}
		})
	}
}
