package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// verifiedManifestCache is the canonical metadata cache for assets whose
// bytes were already SHA-256 verified by the downloader/cache promotion
// boundary. It deliberately caches media metadata only; it never weakens the
// output-artifact manifest path, where ComputeLocalManifest still performs a
// fresh SHA pass before publication.
var verifiedManifestCache sync.Map // map[string]verifiedManifestCacheEntry

type verifiedManifestCacheEntry struct {
	SchemaVersion int    `json:"schema_version"`
	SHA256Hex     string `json:"sha256"`
	SizeBytes     int64  `json:"size_bytes"`
	ModTimeNS     int64  `json:"mod_time_ns"`

	MIMEType        string  `json:"mime_type"`
	Format          string  `json:"format,omitempty"`
	Codec           string  `json:"codec,omitempty"`
	AudioCodec      string  `json:"audio_codec,omitempty"`
	DurationSec     float64 `json:"duration_sec,omitempty"`
	Width           int     `json:"width,omitempty"`
	Height          int     `json:"height,omitempty"`
	HasVideoStream  bool    `json:"has_video_stream"`
	HasAudioStream  bool    `json:"has_audio_stream"`
	AudioTrackCount int     `json:"audio_track_count,omitempty"`
	FfprobeOK       bool    `json:"ffprobe_ok"`
	FfprobeValid    bool    `json:"ffprobe_valid"`
	FfprobeErr      string  `json:"ffprobe_error,omitempty"`
}

const verifiedManifestCacheSchema = 1

// ComputeVerifiedLocalManifest builds media metadata for a file whose content
// identity has already been verified at the canonical asset-cache promotion
// boundary. Unlike ComputeLocalManifest it never re-reads the whole file to
// recompute SHA-256 when expectedSHA256 is present. The SHA remains part of
// the returned manifest because the caller supplied the already-verified
// content identity.
//
// Media metadata is cached both in-process and in a small durable sidecar next
// to the content-addressed cache. A cache entry is accepted only when SHA,
// size and file mtime still match the verified blob. On any ambiguity the
// metadata is probed again; when no verified SHA is available the function
// falls back to ComputeLocalManifest and therefore preserves the strict
// integrity contract.
func ComputeVerifiedLocalManifest(ctx context.Context, path, expectedSHA256 string, expectedSize int64) (*OutputManifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is empty", ErrFileMissing)
	}
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrFileMissing, path)
		}
		return nil, fmt.Errorf("publisher.ComputeVerifiedLocalManifest: stat: %w", err)
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("publisher.ComputeVerifiedLocalManifest: %s is not a regular file", path)
	}
	if expectedSize > 0 && st.Size() != expectedSize {
		return nil, fmt.Errorf("publisher.ComputeVerifiedLocalManifest: size mismatch got=%d want=%d", st.Size(), expectedSize)
	}

	expectedSHA256 = normalizeVerifiedSHA(expectedSHA256)
	if expectedSHA256 == "" {
		// No previously verified content identity: do not invent trust. The
		// canonical full manifest path recomputes SHA and remains authoritative.
		return ComputeLocalManifest(ctx, path)
	}

	cacheKey := verifiedManifestMemoryKey(path, expectedSHA256, st.Size(), st.ModTime().UnixNano())
	if cached, ok := verifiedManifestCache.Load(cacheKey); ok {
		entry := cached.(verifiedManifestCacheEntry)
		return entry.output(path), nil
	}
	if entry, ok := loadVerifiedManifestSidecar(path, expectedSHA256, st); ok {
		verifiedManifestCache.Store(cacheKey, entry)
		return entry.output(path), nil
	}

	started := time.Now()
	m := &OutputManifest{
		LocalPath: path,
		SizeBytes: st.Size(),
		SHA256Hex: expectedSHA256,
	}

	metadataStarted := time.Now()
	head, err := readHead(path, 512)
	if err != nil {
		return nil, fmt.Errorf("publisher.ComputeVerifiedLocalManifest: readHead: %w", err)
	}
	m.MIMEType = http.DetectContentType(head)
	if m.MIMEType == "" || m.MIMEType == "application/octet-stream" {
		m.MIMEType = "application/octet-stream"
	}
	if looksLikeMP4(head) {
		m.Format = "mp4"
	}
	m.Timings.MetadataMS = time.Since(metadataStarted).Milliseconds()

	probeStarted := time.Now()
	probe, probeErr := ProbeMediaDetails(ctx, path)
	m.Timings.FfprobeMS = time.Since(probeStarted).Milliseconds()
	if probeErr != nil {
		m.FfprobeErr = probeErr.Error()
	} else {
		m.Codec = probe.VideoCodec
		m.AudioCodec = probe.AudioCodec
		m.DurationSec = probe.DurationSec
		m.Width = probe.Width
		m.Height = probe.Height
		m.HasVideoStream = probe.HasVideo
		m.HasAudioStream = probe.HasAudio
		m.AudioTrackCount = probe.AudioTrackCount
		m.FfprobeOK = true
		m.FfprobeValid = probe.HasVideo && probe.HasAudio && probe.VideoCodec != "" && probe.Width > 0 && probe.Height > 0
	}
	m.Timings.TotalMS = time.Since(started).Milliseconds()

	entry := entryFromOutputManifest(m, st.ModTime().UnixNano())
	verifiedManifestCache.Store(cacheKey, entry)
	// Metadata acceleration is opportunistic. A read-only cache mount must not
	// turn a valid prepared asset into a failed job merely because the sidecar
	// cannot be persisted.
	_ = persistVerifiedManifestSidecar(path, entry)
	return m, nil
}

func normalizeVerifiedSHA(value string) string {
	value = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(value), "sha256:")))
	if len(value) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(value); err != nil {
		return ""
	}
	return value
}

func verifiedManifestMemoryKey(path, sha string, size, mtime int64) string {
	return fmt.Sprintf("%s|%s|%d|%d", path, sha, size, mtime)
}

func verifiedManifestSidecarPath(path, sha string) string {
	// Keep sidecars out of the asset namespace so directory scans and cache
	// eviction continue to see only real blobs. Hash the full path as well as
	// the content SHA so two independent cache roots cannot alias accidentally.
	pathHash := sha256.Sum256([]byte(path))
	name := sha + "-" + hex.EncodeToString(pathHash[:8]) + ".json"
	return filepath.Join(filepath.Dir(path), ".velox-meta", name)
}

func loadVerifiedManifestSidecar(path, sha string, st os.FileInfo) (verifiedManifestCacheEntry, bool) {
	data, err := os.ReadFile(verifiedManifestSidecarPath(path, sha))
	if err != nil {
		return verifiedManifestCacheEntry{}, false
	}
	var entry verifiedManifestCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return verifiedManifestCacheEntry{}, false
	}
	if entry.SchemaVersion != verifiedManifestCacheSchema || entry.SHA256Hex != sha || entry.SizeBytes != st.Size() || entry.ModTimeNS != st.ModTime().UnixNano() {
		return verifiedManifestCacheEntry{}, false
	}
	return entry, true
}

func persistVerifiedManifestSidecar(path string, entry verifiedManifestCacheEntry) error {
	dst := verifiedManifestSidecarPath(path, entry.SHA256Hex)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, dst)
}

func entryFromOutputManifest(m *OutputManifest, modTimeNS int64) verifiedManifestCacheEntry {
	return verifiedManifestCacheEntry{
		SchemaVersion:   verifiedManifestCacheSchema,
		SHA256Hex:       m.SHA256Hex,
		SizeBytes:       m.SizeBytes,
		ModTimeNS:       modTimeNS,
		MIMEType:        m.MIMEType,
		Format:          m.Format,
		Codec:           m.Codec,
		AudioCodec:      m.AudioCodec,
		DurationSec:     m.DurationSec,
		Width:           m.Width,
		Height:          m.Height,
		HasVideoStream:  m.HasVideoStream,
		HasAudioStream:  m.HasAudioStream,
		AudioTrackCount: m.AudioTrackCount,
		FfprobeOK:       m.FfprobeOK,
		FfprobeValid:    m.FfprobeValid,
		FfprobeErr:      m.FfprobeErr,
	}
}

func (e verifiedManifestCacheEntry) output(path string) *OutputManifest {
	return &OutputManifest{
		LocalPath:       path,
		SizeBytes:       e.SizeBytes,
		SHA256Hex:       e.SHA256Hex,
		MIMEType:        e.MIMEType,
		Format:          e.Format,
		Codec:           e.Codec,
		AudioCodec:      e.AudioCodec,
		DurationSec:     e.DurationSec,
		Width:           e.Width,
		Height:          e.Height,
		HasVideoStream:  e.HasVideoStream,
		HasAudioStream:  e.HasAudioStream,
		AudioTrackCount: e.AudioTrackCount,
		FfprobeOK:       e.FfprobeOK,
		FfprobeValid:    e.FfprobeValid,
		FfprobeErr:      e.FfprobeErr,
	}
}
