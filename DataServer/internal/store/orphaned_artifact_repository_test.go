package store

import (
	"context"
	"testing"
	"time"
)

func seedArtifact(t *testing.T, s *SQLiteStore, id, status, storageKey, createdAt, verifiedAt string) {
	t.Helper()
	_, err := s.db.Exec(
		`INSERT INTO artifacts (id, job_id, type, storage_provider, storage_key, status, created_at, verified_at)
		 VALUES (?, ?, 'video', 'local', ?, ?, ?, ?)`,
		id, "job-"+id, storageKey, status, createdAt, verifiedAt)
	if err != nil {
		t.Fatalf("seed artifact %s: %v", id, err)
	}
}

func TestOrphanedArtifactRepository_ListReadyLocal(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		seed    func(*SQLiteStore)
		wantLen int
		wantIDs []string
	}{
		{
			name: "returns only READY with local storage key and verified_at",
			seed: func(s *SQLiteStore) {
				now := time.Now().UTC().Format(time.RFC3339)
				seedArtifact(t, s, "art-ready-1", "READY", "path/1.mp4", now, "2024-01-01T00:00:00Z")
				seedArtifact(t, s, "art-ready-2", "READY", "path/2.mp4", now, "2024-01-02T00:00:00Z")
				seedArtifact(t, s, "art-staging", "STAGING", "path/3.mp4", now, "2024-01-03T00:00:00Z")
				seedArtifact(t, s, "art-ready-no-key", "READY", "", now, "2024-01-04T00:00:00Z")
				seedArtifact(t, s, "art-ready-no-verify", "READY", "path/5.mp4", now, "")
			},
			wantLen: 2,
			wantIDs: []string{"art-ready-1", "art-ready-2"},
		},
		{
			name:    "empty table returns empty slice",
			seed:    func(s *SQLiteStore) {},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteOrphanedArtifactRepository(s)
			tt.seed(s)

			got, err := repo.ListReadyLocal(ctx)
			if err != nil {
				t.Fatalf("ListReadyLocal error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("len = %v, want %v", len(got), tt.wantLen)
			}
			if tt.wantLen > 0 {
				for i, id := range tt.wantIDs {
					if got[i].ID != id {
						t.Errorf("got[%d].ID = %q, want %q", i, got[i].ID, id)
					}
				}
			}
		})
	}
}

func TestOrphanedArtifactRepository_QuarantineArtifact(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		setup       func(*SQLiteStore, *SQLiteOrphanedArtifactRepository)
		artifactID  string
		reason      string
		wantErr     bool
		wantErrType error
		wantStatus  string
	}{
		{
			name: "quarantine ready artifact",
			setup: func(s *SQLiteStore, _ *SQLiteOrphanedArtifactRepository) {
				seedArtifact(t, s, "art-q-1", "READY", "path/q1.mp4", time.Now().UTC().Format(time.RFC3339), "2024-01-01T00:00:00Z")
			},
			artifactID: "art-q-1",
			reason:     "blob_missing",
			wantStatus: "QUARANTINED",
		},
		{
			name: "quarantine already quarantined returns ErrArtifactAlreadyQuarantined",
			setup: func(s *SQLiteStore, r *SQLiteOrphanedArtifactRepository) {
				seedArtifact(t, s, "art-q-2", "READY", "path/q2.mp4", time.Now().UTC().Format(time.RFC3339), "2024-01-01T00:00:00Z")
				if err := r.QuarantineArtifact(ctx, "art-q-2", "first"); err != nil {
					t.Fatalf("setup quarantine: %v", err)
				}
			},
			artifactID:  "art-q-2",
			reason:      "second",
			wantErr:     true,
			wantErrType: ErrArtifactAlreadyQuarantined,
		},
		{
			name:        "quarantine empty id returns error",
			setup:       func(s *SQLiteStore, _ *SQLiteOrphanedArtifactRepository) {},
			artifactID:  "",
			reason:      "test",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteOrphanedArtifactRepository(s)
			tt.setup(s, repo)

			err := repo.QuarantineArtifact(ctx, tt.artifactID, tt.reason)
			if (err != nil) != tt.wantErr {
				t.Fatalf("QuarantineArtifact error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrType != nil && err != tt.wantErrType {
				t.Errorf("error type = %v, want %v", err, tt.wantErrType)
			}
			if tt.wantStatus != "" {
				var status string
				row := s.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, tt.artifactID)
				if err := row.Scan(&status); err != nil {
					t.Fatalf("read back: %v", err)
				}
				if status != tt.wantStatus {
					t.Errorf("status = %q, want %q", status, tt.wantStatus)
				}
			}
		})
	}
}

func TestOrphanedArtifactRepository_ListStuckStaging(t *testing.T) {
	ctx := context.Background()
	old := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	recent := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)

	tests := []struct {
		name    string
		setup   func(*SQLiteStore)
		cutoff  time.Time
		limit   int
		wantLen int
	}{
		{
			name: "returns stuck staging artifacts older than cutoff",
			setup: func(s *SQLiteStore) {
				seedArtifact(t, s, "art-stuck-1", "STAGING", "path/s1.mp4", old, "")
				seedArtifact(t, s, "art-stuck-2", "STAGING", "path/s2.mp4", old, "")
				seedArtifact(t, s, "art-recent", "STAGING", "path/s3.mp4", recent, "")
				seedArtifact(t, s, "art-ready", "READY", "path/s4.mp4", old, "")
			},
			cutoff:  time.Now().UTC().Add(-24 * time.Hour),
			limit:   10,
			wantLen: 2,
		},
		{
			name: "limit bounds results",
			setup: func(s *SQLiteStore) {
				seedArtifact(t, s, "art-stuck-3", "STAGING", "path/s5.mp4", old, "")
				seedArtifact(t, s, "art-stuck-4", "STAGING", "path/s6.mp4", old, "")
			},
			cutoff:  time.Now().UTC().Add(-24 * time.Hour),
			limit:   1,
			wantLen: 1,
		},
		{
			name:    "default limit when zero",
			setup:   func(s *SQLiteStore) {},
			cutoff:  time.Now().UTC().Add(-24 * time.Hour),
			limit:   0,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteOrphanedArtifactRepository(s)
			tt.setup(s)

			got, err := repo.ListStuckStaging(ctx, tt.cutoff, tt.limit)
			if err != nil {
				t.Fatalf("ListStuckStaging error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("len = %v, want %v", len(got), tt.wantLen)
			}
		})
	}
}

func TestOrphanedArtifactRepository_MarkStuckArtifactFailed(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		setup      func(*SQLiteStore, *SQLiteOrphanedArtifactRepository)
		id         string
		wantOK     bool
		wantErr    bool
		wantStatus string
	}{
		{
			name: "mark stuck artifact as failed",
			setup: func(s *SQLiteStore, _ *SQLiteOrphanedArtifactRepository) {
				seedArtifact(t, s, "art-fail-1", "STAGING", "path/f1.mp4", time.Now().UTC().Format(time.RFC3339), "2024-01-01T00:00:00Z")
			},
			id:         "art-fail-1",
			wantOK:     true,
			wantStatus: "FAILED",
		},
		{
			name: "mark already non-staging returns false",
			setup: func(s *SQLiteStore, r *SQLiteOrphanedArtifactRepository) {
				seedArtifact(t, s, "art-fail-2", "STAGING", "path/f2.mp4", time.Now().UTC().Format(time.RFC3339), "2024-01-01T00:00:00Z")
				_, err := r.MarkStuckArtifactFailed(ctx, "art-fail-2")
				if err != nil {
					t.Fatalf("setup: %v", err)
				}
			},
			id:     "art-fail-2",
			wantOK: false,
		},
		{
			name:    "empty id returns error",
			setup:   func(s *SQLiteStore, _ *SQLiteOrphanedArtifactRepository) {},
			id:      "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteOrphanedArtifactRepository(s)
			tt.setup(s, repo)

			ok, err := repo.MarkStuckArtifactFailed(ctx, tt.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MarkStuckArtifactFailed error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
			if tt.wantStatus != "" {
				var status string
				row := s.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, tt.id)
				if err := row.Scan(&status); err != nil {
					t.Fatalf("read back: %v", err)
				}
				if status != tt.wantStatus {
					t.Errorf("status = %q, want %q", status, tt.wantStatus)
				}
			}
		})
	}
}
