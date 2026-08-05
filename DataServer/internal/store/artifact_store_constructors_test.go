package store

import (
	"path/filepath"
	"testing"
)

func TestArtifactRepositoriesFromStoreUseCanonicalSQLiteStore(t *testing.T) {
	t.Parallel()

	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got := NewSQLiteUploadRepositoryFromStore(s); got == nil || got.db != s.db {
		t.Fatal("upload repository must use the canonical SQLiteStore database")
	}
	if got := NewSQLiteUploadSessionWriterFromStore(s); got == nil || got.db != s.db {
		t.Fatal("upload session writer must use the canonical SQLiteStore database")
	}
	if got := NewSQLiteArtifactReaderFromStore(s); got == nil || got.db != s.db {
		t.Fatal("artifact reader must use the canonical SQLiteStore database")
	}
	if got := NewSQLiteAuthReaderFromStore(s); got == nil || got.db != s.db {
		t.Fatal("auth reader must use the canonical SQLiteStore database")
	}
	if got := NewSQLiteJobDeliveryCounterFromStore(s); got == nil || got.db != s.db {
		t.Fatal("delivery counter must use the canonical SQLiteStore database")
	}
	if got := NewSQLiteArtifactFinalizerFromStore(s, nil); got == nil || got.db != s.db {
		t.Fatal("artifact finalizer must use the canonical SQLiteStore database")
	}
	if got := NewArtifactReconcilerRepositoryFromStore(s); got == nil || got.db != s.db {
		t.Fatal("artifact reconciler repository must use the canonical SQLiteStore database")
	}
	if got := NewArtifactGCStoreFromStore(s); got == nil || got.db != s.db {
		t.Fatal("artifact GC store must use the canonical SQLiteStore database")
	}
}
