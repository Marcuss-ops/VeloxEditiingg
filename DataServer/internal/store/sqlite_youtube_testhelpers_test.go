package store

import (
	"testing"
)

// newYouTubeTestStore opens an isolated SQLite test database for a single
// YouTube-domain test and enables foreign key enforcement. The store is
// closed via t.Cleanup so callers do not need to remember `defer s.Close()`.
//
// Foreign keys are turned on here because openTestDB (defined in
// sqlite_ansible_test.go) constructs the store from NewSQLiteStore which
// does not always enable FK on the connection that the test ends up using.
// The youtube_oauth_tokens FK to youtube_channels, the cascade inside
// DeleteChannelAtomic, the FK contract pinned by
// TestYouTubeOAuthTokenChannelFKDeleteCascade, and the transactional
// upsert inside ConnectChannelAtomic all depend on FK enforcement being
// active at test time.
//
// Use this helper in every sqlite_youtube_*_test.go file in place of:
//
//	s := openTestDB(t)
//	defer s.Close()
//	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")
func newYouTubeTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s := openTestDB(t)
	if _, err := s.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK pragma: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
