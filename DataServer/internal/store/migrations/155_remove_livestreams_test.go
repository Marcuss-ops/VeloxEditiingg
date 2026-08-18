package migrations

import "testing"

// TestMigration155_RemovesLivestreams verifies that databases which were
// touched by the retired runtime DDL are cleaned up during the normal upgrade
// path. The table is intentionally not represented by a replacement model.
func TestMigration155_RemovesLivestreams(t *testing.T) {
	db := openTestDB(t)
	applyProductionMigrationsUpTo(t, db, 154)

	if _, err := db.Exec(`CREATE TABLE livestreams (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy livestreams table: %v", err)
	}

	applyProductionMigrationsUpTo(t, db, 155)
	if tableExists(t, db, "livestreams") {
		t.Fatal("migration 155 left retired livestreams table behind")
	}
}
