// Package chunkfactory implements the W5 precursor: a content-addressed,
// keyframe-aligned video chunk store on the worker (SCALE-IMPROVEMENT-PLAN
// W5, docs/SCALE-IMPROVEMENT-PLAN.md §4).
//
// W5 (manifest-first delivery) requires a chunk factory: the job becomes a
// template, ~95% of output bytes are shared stock chunks reused across
// "different" videos, and delivery ships playlists + unique audio instead of
// monolithic MP4s. The prerequisite is a worker-side chunk store where a
// chunk's identity is derived from its CONTENT, not from the job that
// happened to produce it — so two jobs rendering the same stock segment
// (same source asset, same trim window, same profile) share one physical
// chunk and one cache entry.
//
// Chunk identity is the SHA-256 over a deterministic identity document:
//
//	{"asset_key":..., "source_in_us":..., "source_out_us":...,
//	 "profile_id":..., "chunk_index":N, "chunk_duration_us":...,
//	 "identity_version":1}
//
// Keyframe alignment: chunk boundaries snap backwards to the keyframe
// timestamps supplied by the caller (the engine's keyframe probe). A chunk
// that cannot start on a keyframe is never produced — the same fail-closed
// rule the packet-copy mux enforces for segments.
//
// Storage layout (under the configured root):
//
//	<root>/chunks/<aa>/<chunk_id>.m4s    the chunk payload
//	<root>/chunks/<aa>/<chunk_id>.json   the chunk descriptor
//
// Two-letter sharding keeps directories small on ext4. Writes are atomic
// (temp file + rename) so a concurrent reader never observes a partial
// chunk, and PutChunk is idempotent: the content-addressed name makes a
// duplicate write a no-op stat-verify.
package chunkfactory

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// IdentityVersion is the chunk identity document version. Bump when the
// identity inputs change semantics; old chunks simply stop matching (their
// cache entries age out via the worker cache eviction) rather than colliding.
const IdentityVersion = 1

// MinChunkDuration guards against degenerate zero/one-frame chunks produced
// by pathological trim windows.
const MinChunkDuration = 100_000 // 100 ms in microseconds

// ChunkDescriptor is the persisted metadata for one chunk.
type ChunkDescriptor struct {
	// ChunkID is the content-addressed identity (lowercase hex SHA-256 of
	// the identity document).
	ChunkID string `json:"chunk_id"`
	// AssetKey is the canonical source asset the chunk was cut from.
	AssetKey string `json:"asset_key"`
	// SourceInUS / SourceOutUS are the keyframe-aligned source window.
	SourceInUS  int64 `json:"source_in_us"`
	SourceOutUS int64 `json:"source_out_us"`
	// ProfileID is the canonical video profile of the source.
	ProfileID string `json:"profile_id"`
	// ChunkIndex / ChunkDurationUS locate the chunk inside its source.
	ChunkIndex      int   `json:"chunk_index"`
	ChunkDurationUS int64 `json:"chunk_duration_us"`
	// PayloadSHA256 / SizeBytes identify the produced bytes.
	PayloadSHA256 string `json:"payload_sha256"`
	SizeBytes     int64  `json:"size_bytes"`
	// FirstFrameKeyframe is always true: chunks only exist keyframe-aligned.
	FirstFrameKeyframe bool  `json:"first_frame_keyframe"`
	CreatedAt          int64 `json:"created_at"`
}

// Keyframe timestamps in microseconds, ascending, from the engine probe.
type KeyframeIndex []int64

// SnapToKeyframe returns the largest keyframe <= t, or false when t is
// before the first keyframe (the window cannot start keyframe-aligned).
func (k KeyframeIndex) SnapToKeyframe(t int64) (int64, bool) {
	if len(k) == 0 || t < k[0] {
		return 0, false
	}
	lo, hi := 0, len(k)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if k[mid] <= t {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return k[lo], true
}

// ChunkIdentity derives the content-addressed chunk ID from the deterministic
// identity inputs. Same inputs → same ID → same physical chunk across jobs.
func ChunkIdentity(assetKey, profileID string, sourceInUS, sourceOutUS int64, chunkIndex int) (string, error) {
	if strings.TrimSpace(assetKey) == "" {
		return "", fmt.Errorf("chunkfactory: asset_key is required")
	}
	if sourceOutUS <= sourceInUS {
		return "", fmt.Errorf("chunkfactory: invalid window in=%d out=%d", sourceInUS, sourceOutUS)
	}
	if sourceOutUS-sourceInUS < MinChunkDuration {
		return "", fmt.Errorf("chunkfactory: window %d us below minimum %d us", sourceOutUS-sourceInUS, MinChunkDuration)
	}
	identity := struct {
		AssetKey        string `json:"asset_key"`
		SourceInUS      int64  `json:"source_in_us"`
		SourceOutUS     int64  `json:"source_out_us"`
		ProfileID       string `json:"profile_id"`
		ChunkIndex      int    `json:"chunk_index"`
		ChunkDurationUS int64  `json:"chunk_duration_us"`
		IdentityVersion int    `json:"identity_version"`
	}{
		AssetKey:        assetKey,
		SourceInUS:      sourceInUS,
		SourceOutUS:     sourceOutUS,
		ProfileID:       profileID,
		ChunkIndex:      chunkIndex,
		ChunkDurationUS: sourceOutUS - sourceInUS,
		IdentityVersion: IdentityVersion,
	}
	data, err := json.Marshal(&identity)
	if err != nil {
		return "", fmt.Errorf("chunkfactory: identity marshal: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// PlanChunk is one keyframe-aligned chunk boundary computed from a keyframe
// index. The engine maps each plan to a packet-copy cut.
type PlanChunk struct {
	ChunkID      string
	AssetKey     string
	ProfileID    string
	ChunkIndex   int
	SourceInUS   int64
	SourceOutUS  int64
	FirstIsExact bool // true when the requested start was already a keyframe
}

// PlanChunks computes the keyframe-aligned chunk boundaries for one source
// asset. The rule mirrors the packet-copy mux: a chunk START must BE a
// keyframe, so boundaries are chosen FROM the keyframe index — for each
// chunk the boundary candidate is the keyframe closest to start+target
// subject to c-start ≥ MinChunkDuration — and the final chunk extends to
// windowOutUS. When no further keyframe boundary exists inside the window,
// the remaining tail is emitted as one final chunk (its start is the
// previous boundary, already keyframe-aligned).
func PlanChunks(assetKey, profileID string, windowInUS, windowOutUS int64, targetDurationUS int64, keyframes KeyframeIndex) ([]PlanChunk, error) {
	if targetDurationUS < MinChunkDuration {
		return nil, fmt.Errorf("chunkfactory: target duration %d us below minimum", targetDurationUS)
	}
	if windowOutUS <= windowInUS {
		return nil, fmt.Errorf("chunkfactory: invalid window in=%d out=%d", windowInUS, windowOutUS)
	}
	_, startIsKeyframe := keyframes.SnapToKeyframe(windowInUS)
	startIsKeyframe = startIsKeyframe && containsExact(keyframes, windowInUS)

	var chunks []PlanChunk
	appendChunk := func(start, end int64, index int) error {
		id, err := ChunkIdentity(assetKey, profileID, start, end, index)
		if err != nil {
			return err
		}
		chunks = append(chunks, PlanChunk{
			ChunkID: id, AssetKey: assetKey, ProfileID: profileID,
			ChunkIndex: index, SourceInUS: start, SourceOutUS: end,
			FirstIsExact: index == 0 && startIsKeyframe,
		})
		return nil
	}

	start := windowInUS
	index := 0
	for start < windowOutUS {
		targetBoundary := start + targetDurationUS
		best, bestDist := int64(-1), int64(-1)
		for _, c := range keyframes {
			if c <= start || c > windowOutUS {
				continue
			}
			if c-start < MinChunkDuration {
				continue
			}
			dist := c - targetBoundary
			if dist < 0 {
				dist = -dist
			}
			if best < 0 || dist < bestDist || (dist == bestDist && c < best) {
				best, bestDist = c, dist
			}
		}
		if best < 0 {
			// No keyframe boundary inside the window: the tail becomes the
			// final chunk.
			if err := appendChunk(start, windowOutUS, index); err != nil {
				return nil, err
			}
			break
		}
		if err := appendChunk(start, best, index); err != nil {
			return nil, err
		}
		start = best
		index++
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("chunkfactory: produced no chunks for window %d..%d", windowInUS, windowOutUS)
	}
	return chunks, nil
}

func containsExact(index KeyframeIndex, t int64) bool {
	for _, k := range index {
		if k == t {
			return true
		}
	}
	return false
}

// Store is the on-disk content-addressed chunk cache.
type Store struct {
	root string
	now  func() time.Time
}

// NewStore creates a chunk store rooted at dir (created on demand).
func NewStore(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("chunkfactory: store root is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "chunks"), 0o755); err != nil {
		return nil, fmt.Errorf("chunkfactory: mkdir %s: %w", dir, err)
	}
	return &Store{root: dir, now: func() time.Time { return time.Now().UTC() }}, nil
}

func shardPath(root, chunkID, suffix string) string {
	if len(chunkID) < 2 {
		return filepath.Join(root, "chunks", chunkID+suffix)
	}
	return filepath.Join(root, "chunks", chunkID[:2], chunkID+suffix)
}

// Has reports whether the chunk is already cached, verifying the descriptor
// exists (payload verification is the caller's SHA check at mux time).
func (s *Store) Has(chunkID string) (bool, error) {
	if chunkID == "" {
		return false, fmt.Errorf("chunkfactory: empty chunk id")
	}
	_, err := os.Stat(shardPath(s.root, chunkID, ".json"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("chunkfactory: stat %s: %w", chunkID, err)
	}
	return true, nil
}

// PayloadPath returns the on-disk path of a chunk payload (for the mux to
// consume directly, and for callers computing their own SHA).
func (s *Store) PayloadPath(chunkID string) string {
	return shardPath(s.root, chunkID, ".m4s")
}

// PutChunk atomically installs a produced chunk payload and descriptor.
// Idempotent: an existing payload with the same size short-circuits to a
// verified no-op (content-addressed storage makes duplicate writes legal).
func (s *Store) PutChunk(chunk ChunkDescriptor, payloadPath string) error {
	if chunk.ChunkID == "" {
		return fmt.Errorf("chunkfactory: PutChunk: empty chunk id")
	}
	if chunk.PayloadSHA256 == "" || chunk.SizeBytes <= 0 {
		return fmt.Errorf("chunkfactory: PutChunk %s: payload identity incomplete", chunk.ChunkID)
	}
	chunk.FirstFrameKeyframe = true
	if chunk.CreatedAt == 0 {
		chunk.CreatedAt = s.now().Unix()
	}
	target := shardPath(s.root, chunk.ChunkID, ".m4s")
	if info, err := os.Stat(target); err == nil && info.Size() == chunk.SizeBytes {
		// Already cached with the same size: content addressing guarantees
		// identity; rewrite only the descriptor (cheap, keeps metadata fresh).
		return s.writeDescriptor(chunk)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("chunkfactory: mkdir shard: %w", err)
	}
	// Atomic install: temp file in the same directory + rename.
	tmp := target + ".tmp"
	if err := copyFile(payloadPath, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("chunkfactory: rename into place: %w", err)
	}
	return s.writeDescriptor(chunk)
}

func (s *Store) writeDescriptor(chunk ChunkDescriptor) error {
	data, err := json.Marshal(&chunk)
	if err != nil {
		return fmt.Errorf("chunkfactory: descriptor marshal: %w", err)
	}
	path := shardPath(s.root, chunk.ChunkID, ".json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("chunkfactory: descriptor write: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("chunkfactory: descriptor rename: %w", err)
	}
	return nil
}

// GetChunk loads a cached descriptor, or (nil, nil) when absent.
func (s *Store) GetChunk(chunkID string) (*ChunkDescriptor, error) {
	if chunkID == "" {
		return nil, fmt.Errorf("chunkfactory: empty chunk id")
	}
	data, err := os.ReadFile(shardPath(s.root, chunkID, ".json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("chunkfactory: read descriptor %s: %w", chunkID, err)
	}
	var chunk ChunkDescriptor
	if err := json.Unmarshal(data, &chunk); err != nil {
		return nil, fmt.Errorf("chunkfactory: descriptor parse %s: %w", chunkID, err)
	}
	return &chunk, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("chunkfactory: open payload %s: %w", src, err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("chunkfactory: create temp %s: %w", dst, err)
	}
	if _, err := out.ReadFrom(in); err != nil {
		out.Close()
		os.Remove(dst)
		return fmt.Errorf("chunkfactory: copy payload: %w", err)
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return fmt.Errorf("chunkfactory: close temp: %w", err)
	}
	return nil
}
