package workers

import (
	"testing"

	"velox-server/internal/store"
)

func TestGenerateTokenFailsClosedWhenStoreCannotPersist(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/token-persistence.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	tm := NewTokenManager(db)
	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}
	if token := tm.GenerateToken("token-persistence-worker"); token != "" {
		t.Fatalf("GenerateToken returned a token after persistence failure: %q", token)
	}
}
