// Tests for BlobArtifacts. Cover hash-mismatch rejection, not-found
// sentinel, roundtrip, and Close() lifecycle.
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
)

func testHash(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func makeBlobs(t *testing.T) *BlobArtifacts {
	t.Helper()
	b, err := NewBlobArtifacts(BlobOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("NewBlobArtifacts: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestBlob_PutGetRoundtrip(t *testing.T) {
	b := makeBlobs(t)
	ctx := context.Background()
	data := []byte("blob-payload")
	hash := testHash(t, data)

	if err := b.Put(ctx, hash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := b.Get(ctx, hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("Get data mismatch: %q != %q", got, data)
	}
	s := b.Stats()
	if s.Publish != 1 || s.Fetch != 1 || s.Bytes != int64(len(data)) || s.Entries != 1 {
		t.Fatalf("stats after roundtrip: %+v want publish=1 fetch=1 bytes=%d entries=1", s, len(data))
	}
}

func TestBlob_PutIsIdempotentForExistingContent(t *testing.T) {
	b := makeBlobs(t)
	ctx := context.Background()
	data := []byte("same-content")
	hash := testHash(t, data)
	if err := b.Put(ctx, hash, data); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := b.Put(ctx, hash, data); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if got := b.Stats(); got.Entries != 1 || got.Bytes != int64(len(data)) {
		t.Fatalf("duplicate Put inflated unique storage stats: %+v", got)
	}
}

func TestBlob_PutRejectsHashMismatch(t *testing.T) {
	b := makeBlobs(t)
	ctx := context.Background()

	data := []byte("real")
	wrongHash := testHash(t, []byte("different"))
	err := b.Put(ctx, wrongHash, data)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("want ErrHashMismatch, got %v", err)
	}
	// File must NOT be persisted under the wrong hash.
	if _, err := b.Get(ctx, wrongHash); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("want ErrBlobNotFound after rejected Put, got %v", err)
	}
	if s := b.Stats(); s.PublishFailed < 1 {
		t.Fatalf("PublishFailed should be >=1, got %+v", s)
	}
}

func TestBlob_GetReturnsNotFound(t *testing.T) {
	b := makeBlobs(t)
	ctx := context.Background()
	missing := testHash(t, []byte("never-put"))
	_, err := b.Get(ctx, missing)
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("want ErrBlobNotFound, got %v", err)
	}
	if s := b.Stats(); s.FetchMiss != 1 {
		t.Fatalf("FetchMiss should be 1, got %+v", s)
	}
}

func TestBlob_CloseStopsProcessorAndRejectsPuts(t *testing.T) {
	b, err := NewBlobArtifacts(BlobOptions{
		Root: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewBlobArtifacts: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := b.Put(context.Background(), testHash(t, []byte("x")), []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close: want ErrClosed, got %v", err)
	}
}

func TestBlob_PutHasNoDetachedPublicationQueue(t *testing.T) {
	// BlobArtifacts is local persistence only. A successful Put must not
	// create a second, unowned publication path that can silently drop work.
	b := makeBlobs(t)
	ctx := context.Background()
	const flood = 5000
	for i := 0; i < flood; i++ {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(i))
		hash := testHash(t, data)
		if err := b.Put(ctx, hash, data); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}
	s := b.Stats()
	if s.Publish != int64(flood) {
		t.Fatalf("Publish = %d, want %d successful persisted blobs", s.Publish, flood)
	}
	if s.PublishFailed != 0 {
		t.Fatalf("local blob writes should not report detached publication failures: %+v", s)
	}
	if s.Entries != flood {
		t.Fatalf("Entries = %d, want %d persisted blobs", s.Entries, flood)
	}
}
