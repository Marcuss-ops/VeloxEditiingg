package store

import (
	"context"
	"errors"
	"testing"
)

func TestSQLiteStoreResolveDriveFolderReference(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/drive-folder-resolver.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	if err := s.UpsertMasterFolder("folder-rap", "Rap Videos", "https://drive.google.com/drive/folders/folder-rap", "it", 0, `{"alias":"rap"}`); err != nil {
		t.Fatalf("UpsertMasterFolder: %v", err)
	}

	got, err := s.ResolveDriveFolderReference(context.Background(), " rap ")
	if err != nil {
		t.Fatalf("resolve alias: %v", err)
	}
	if got != "folder-rap" {
		t.Fatalf("resolved alias = %q, want folder-rap", got)
	}

	got, err = s.ResolveDriveFolderReference(context.Background(), "https://drive.google.com/drive/u/2/folders/folder-direct?usp=sharing")
	if err != nil {
		t.Fatalf("resolve direct URL: %v", err)
	}
	if got != "folder-direct" {
		t.Fatalf("resolved direct URL = %q, want folder-direct", got)
	}
}

func TestSQLiteStoreResolveDriveFolderReferencePropagatesContextError(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/drive-folder-resolver-cancelled.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.ResolveDriveFolderReference(ctx, "unknown-alias")
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
