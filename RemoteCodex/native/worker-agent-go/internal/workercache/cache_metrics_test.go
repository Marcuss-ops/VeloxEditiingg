package workercache

import (
	"context"
	"testing"
)

func TestCacheSizeReportsEntriesAndRecordedBytes(t *testing.T) {
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	if err := cache.Store(ctx, Entry{DriveFileID: "asset-a", LocalPath: "/cache/a", SizeBytes: 128, DownloadComplete: true}); err != nil {
		t.Fatalf("Store a: %v", err)
	}
	if err := cache.Store(ctx, Entry{DriveFileID: "asset-b", LocalPath: "/cache/b", SizeBytes: 256, DownloadComplete: true}); err != nil {
		t.Fatalf("Store b: %v", err)
	}

	entries, bytes, err := cache.Size(ctx)
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if entries != 2 || bytes != 384 {
		t.Fatalf("Size = entries:%d bytes:%d, want 2/384", entries, bytes)
	}
}
