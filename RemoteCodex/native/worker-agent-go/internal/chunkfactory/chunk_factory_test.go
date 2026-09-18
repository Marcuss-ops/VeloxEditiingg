package chunkfactory

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func keyframesFor(us ...int64) KeyframeIndex { return KeyframeIndex(us) }

func TestChunkIdentity_DeterministicAndDistinct(t *testing.T) {
	id1, err := ChunkIdentity("clips/base.mp4", "profile", 0, 1_000_000, 0)
	if err != nil {
		t.Fatalf("ChunkIdentity: %v", err)
	}
	if len(id1) != 64 || id1 != lowerHex(id1) {
		t.Fatalf("chunk id must be lowercase hex sha256, got %q", id1)
	}
	id2, err := ChunkIdentity("clips/base.mp4", "profile", 0, 1_000_000, 0)
	if err != nil {
		t.Fatalf("ChunkIdentity: %v", err)
	}
	if id1 != id2 {
		t.Fatal("same identity inputs must produce the same chunk id")
	}
	id3, err := ChunkIdentity("clips/base.mp4", "profile", 1_000_000, 2_000_000, 1)
	if err != nil {
		t.Fatalf("ChunkIdentity: %v", err)
	}
	if id1 == id3 {
		t.Fatal("different windows must produce different chunk ids")
	}
}

func TestChunkIdentity_RejectsDegenerateWindows(t *testing.T) {
	if _, err := ChunkIdentity("", "p", 0, 1_000_000, 0); err == nil {
		t.Fatal("empty asset key must be rejected")
	}
	if _, err := ChunkIdentity("a", "p", 1_000_000, 1_000_000, 0); err == nil {
		t.Fatal("empty window must be rejected")
	}
	if _, err := ChunkIdentity("a", "p", 0, 50_000, 0); err == nil {
		t.Fatal("sub-minimum window must be rejected")
	}
}

func TestSnapToKeyframe(t *testing.T) {
	kf := keyframesFor(0, 2_000_000, 4_000_000)
	if snap, ok := kf.SnapToKeyframe(2_000_000); !ok || snap != 2_000_000 {
		t.Fatalf("exact keyframe must snap to itself, got %d ok=%v", snap, ok)
	}
	if snap, ok := kf.SnapToKeyframe(2_999_999); !ok || snap != 2_000_000 {
		t.Fatalf("between keyframes must snap backwards, got %d ok=%v", snap, ok)
	}
	if _, ok := kf.SnapToKeyframe(-1); ok {
		t.Fatal("before first keyframe must fail")
	}
	if _, ok := (KeyframeIndex{}).SnapToKeyframe(0); ok {
		t.Fatal("empty index must fail")
	}
}

func TestPlanChunks_KeyframeAlignedAndComplete(t *testing.T) {
	// Window 0..10s, target 1s chunks, keyframes every 2s → boundaries snap
	// to 0, 2M, 4M, 6M, 8M and the final chunk runs 8M..10M.
	kf := keyframesFor(0, 2_000_000, 4_000_000, 6_000_000, 8_000_000, 10_000_000)
	chunks, err := PlanChunks("clips/base.mp4", "profile", 0, 10_000_000, 1_000_000, kf)
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	if len(chunks) != 5 {
		t.Fatalf("chunk count = %d, want 5 (boundaries snap to 2s keyframes)", len(chunks))
	}
	var got []int64
	for _, c := range chunks {
		got = append(got, c.SourceInUS)
	}
	want := []int64{0, 2_000_000, 4_000_000, 6_000_000, 8_000_000}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d start = %d, want %d (keyframe-aligned)", i, got[i], want[i])
		}
	}
	last := chunks[len(chunks)-1]
	if last.SourceOutUS != 10_000_000 {
		t.Fatalf("final chunk must extend to window end, got %d", last.SourceOutUS)
	}
	// Coverage: chunks must tile the window with no gaps.
	for i := 1; i < len(chunks); i++ {
		if chunks[i].SourceInUS != chunks[i-1].SourceOutUS {
			t.Fatalf("gap between chunk %d and %d", i-1, i)
		}
	}
}

func TestPlanChunks_RejectsInvalidWindow(t *testing.T) {
	if _, err := PlanChunks("a", "p", 5_000_000, 1_000_000, 1_000_000, keyframesFor(0)); err == nil {
		t.Fatal("inverted window must be rejected")
	}
	if _, err := PlanChunks("a", "p", 0, 10_000_000, 50_000, keyframesFor(0)); err == nil {
		t.Fatal("sub-minimum target duration must be rejected")
	}
}

func TestStore_PutGetHasRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	chunkID, err := ChunkIdentity("clips/base.mp4", "profile", 0, 2_000_000, 0)
	if err != nil {
		t.Fatalf("ChunkIdentity: %v", err)
	}

	// Absent before put.
	if got, err := store.Has(chunkID); err != nil || got {
		t.Fatalf("Has before put = %v err %v, want false nil", got, err)
	}
	if got, err := store.GetChunk(chunkID); err != nil || got != nil {
		t.Fatalf("GetChunk before put = %v err %v, want nil nil", got, err)
	}

	// Produce a fake payload and install it.
	payload := filepath.Join(dir, "src.m4s")
	body := []byte("chunk-payload-bytes")
	if err := os.WriteFile(payload, body, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	chunk := ChunkDescriptor{
		ChunkID: chunkID, AssetKey: "clips/base.mp4", ProfileID: "profile",
		SourceInUS: 0, SourceOutUS: 2_000_000, ChunkIndex: 0, ChunkDurationUS: 2_000_000,
		PayloadSHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body)),
	}
	if err := store.PutChunk(chunk, payload); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	if got, err := store.Has(chunkID); err != nil || !got {
		t.Fatalf("Has after put = %v err %v, want true nil", got, err)
	}
	got, err := store.GetChunk(chunkID)
	if err != nil || got == nil {
		t.Fatalf("GetChunk after put: %v %v", got, err)
	}
	if got.PayloadSHA256 != chunk.PayloadSHA256 || got.SizeBytes != chunk.SizeBytes {
		t.Fatalf("descriptor mismatch: %+v", got)
	}
	if !got.FirstFrameKeyframe {
		t.Fatal("chunks must always be first-frame-keyframe")
	}
	onDisk, err := os.ReadFile(store.PayloadPath(chunkID))
	if err != nil || string(onDisk) != string(body) {
		t.Fatalf("payload round-trip mismatch: %q err %v", onDisk, err)
	}

	// Idempotent duplicate put with the same size: no error, no rewrite.
	if err := store.PutChunk(chunk, payload); err != nil {
		t.Fatalf("duplicate PutChunk must be a no-op, got %v", err)
	}
}

func TestStore_PutChunkRejectsIncompleteIdentity(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutChunk(ChunkDescriptor{ChunkID: "abc"}, "/nonexistent"); err == nil {
		t.Fatal("payload identity incomplete must be rejected before any I/O")
	}
}

func TestStore_DescriptorCorruptionSurfaces(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	chunkID, _ := ChunkIdentity("a", "p", 0, 1_000_000, 0)
	// Write a corrupt descriptor directly.
	corrupt := filepath.Join(dir, "chunks", chunkID[:2], chunkID+".json")
	if err := os.MkdirAll(filepath.Dir(corrupt), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetChunk(chunkID); err == nil {
		t.Fatal("corrupt descriptor must surface, not silently miss")
	}
}

func lowerHex(s string) string {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "\x00invalid"
		}
	}
	return s
}

// Compile-time guard: the descriptor must stay valid JSON round-trippable.
func TestChunkDescriptor_JSONRoundTrip(t *testing.T) {
	chunk := ChunkDescriptor{ChunkID: "abc", AssetKey: "k", ChunkDurationUS: 1_000_000}
	data, err := json.Marshal(&chunk)
	if err != nil {
		t.Fatal(err)
	}
	var back ChunkDescriptor
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back != chunk {
		t.Fatalf("round trip mismatch: %+v vs %+v", back, chunk)
	}
}
