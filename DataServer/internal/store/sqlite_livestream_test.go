package store

import "testing"

func TestLivestreamStoreFailsClosedWhenUnconfigured(t *testing.T) {
	var s *SQLiteStore

	if _, err := s.ListLivestreams(); err == nil {
		t.Fatal("ListLivestreams should fail when the store is nil")
	}
	if _, err := s.GetLivestream("stream-1"); err == nil {
		t.Fatal("GetLivestream should fail when the store is nil")
	}
	if err := s.UpsertLivestream(&LivestreamRow{ID: "stream-1"}); err == nil {
		t.Fatal("UpsertLivestream should fail when the store is nil")
	}
	if err := s.DeleteLivestream("stream-1"); err == nil {
		t.Fatal("DeleteLivestream should fail when the store is nil")
	}
}

func TestLivestreamStoreRejectsNilRow(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/livestream.db")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.UpsertLivestream(nil); err == nil {
		t.Fatal("UpsertLivestream should reject a nil row")
	}
}
