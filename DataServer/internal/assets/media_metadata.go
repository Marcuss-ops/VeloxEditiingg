// Package assets / media_metadata.go
//
// Canonical asset media metadata (Fase C1): the registry is authoritative
// for the media description of every asset. MediaMetadataResolver is the
// SINGLE component allowed to spawn ffprobe for asset-metadata extraction;
// it runs once at ingestion (see registration.go) and persists the result
// into asset_media_metadata. Every job-time consumer reads the registry
// instead of spawning its own probe.
package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"
)

// MediaMetadataSchemaVersion is the canonical schema version stamped on
// every verified asset_media_metadata row. Bump ONLY with a migration
// (the column is pinned by a CHECK constraint in 143_asset_media_metadata.sql).
const MediaMetadataSchemaVersion = 1

// MediaMetadata is the canonical, registry-authoritative media description
// of an asset, captured once at ingestion. All fields derive from a single
// ffprobe invocation.
type MediaMetadata struct {
	Container       string `json:"container"`
	DurationMs      int64  `json:"duration_ms"`
	VideoCodec      string `json:"video_codec,omitempty"`
	PixelFormat     string `json:"pix_fmt,omitempty"`
	Width           int    `json:"width,omitempty"`
	Height          int    `json:"height,omitempty"`
	FPSNum          int    `json:"fps_num,omitempty"`
	FPSDen          int    `json:"fps_den,omitempty"`
	TimeBaseNum     int    `json:"time_base_num,omitempty"`
	TimeBaseDen     int    `json:"time_base_den,omitempty"`
	AudioCodec      string `json:"audio_codec,omitempty"`
	AudioSampleRate int    `json:"sample_rate,omitempty"`
	AudioChannels   int    `json:"channels,omitempty"`
}

// MediaMetadataRecord is the storage projection of an asset_media_metadata row.
type MediaMetadataRecord struct {
	AssetID               string
	Container             string
	DurationMs            int64
	VideoCodec            string
	PixelFormat           string
	Width                 int
	Height                int
	FPSNum                int
	FPSDen                int
	TimeBaseNum           int
	TimeBaseDen           int
	AudioCodec            string
	AudioSampleRate       int
	AudioChannels         int
	MetadataVerifiedAt    string
	MetadataSchemaVersion int
}

// Verified reports whether the row carries a full verified probe: a
// verification timestamp AND the canonical schema version. A missing row
// (or a row with an empty verification timestamp) is the authoritative
// metadata_verified=false signal.
func (r *MediaMetadataRecord) Verified() bool {
	return r != nil && strings.TrimSpace(r.MetadataVerifiedAt) != "" &&
		r.MetadataSchemaVersion == MediaMetadataSchemaVersion
}

// ToDomain converts the storage record into the canonical domain metadata.
func (r *MediaMetadataRecord) ToDomain() *MediaMetadata {
	if r == nil {
		return nil
	}
	return &MediaMetadata{
		Container:       r.Container,
		DurationMs:      r.DurationMs,
		VideoCodec:      r.VideoCodec,
		PixelFormat:     r.PixelFormat,
		Width:           r.Width,
		Height:          r.Height,
		FPSNum:          r.FPSNum,
		FPSDen:          r.FPSDen,
		TimeBaseNum:     r.TimeBaseNum,
		TimeBaseDen:     r.TimeBaseDen,
		AudioCodec:      r.AudioCodec,
		AudioSampleRate: r.AudioSampleRate,
		AudioChannels:   r.AudioChannels,
	}
}

// mediaProbeRunner is the exec seam for MediaMetadataResolver. Production
// uses execVideoCommandRunner (video_trimmer.go); tests inject a fake.
type mediaProbeRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// MediaMetadataResolver is the single canonical component that extracts
// media metadata from a master-local file. It is an extraction only —
// never a mutation: no downloads, no staging, no registry writes.
type MediaMetadataResolver struct {
	runner mediaProbeRunner
	bin    string
}

// NewMediaMetadataResolver returns the production resolver (ffprobe from PATH).
func NewMediaMetadataResolver() *MediaMetadataResolver {
	return &MediaMetadataResolver{runner: execVideoCommandRunner{}, bin: "ffprobe"}
}

// newMediaMetadataResolverForTest injects a fake runner (nil falls back to exec).
func newMediaMetadataResolverForTest(runner mediaProbeRunner) *MediaMetadataResolver {
	if runner == nil {
		runner = execVideoCommandRunner{}
	}
	return &MediaMetadataResolver{runner: runner, bin: "ffprobe"}
}

// Resolve runs ffprobe exactly once over inputPath and returns the canonical
// media metadata. It fails when the file has no video or audio stream
// (e.g. fonts, subtitles, project files) — callers decide whether to store
// the result; nothing is invented when probing fails.
func (r *MediaMetadataResolver) Resolve(ctx context.Context, inputPath string) (*MediaMetadata, error) {
	if r == nil || r.runner == nil {
		return nil, fmt.Errorf("media metadata resolver unavailable")
	}
	if strings.TrimSpace(inputPath) == "" {
		return nil, fmt.Errorf("media metadata input path is required")
	}
	bin := strings.TrimSpace(r.bin)
	if bin == "" {
		bin = "ffprobe"
	}
	args := []string{
		"-v", "error",
		"-show_entries", "stream=codec_type,codec_name,width,height,r_frame_rate,time_base,pix_fmt,duration,sample_rate,channels:format=format_name,duration",
		"-of", "json",
		inputPath,
	}
	output, err := r.runner.Run(ctx, bin, args...)
	if err != nil {
		return nil, fmt.Errorf("ffprobe %s: %w: %s", inputPath, err, strings.TrimSpace(string(output)))
	}
	var document ffprobeDocument
	if err := json.Unmarshal(output, &document); err != nil {
		return nil, fmt.Errorf("parse ffprobe %s: %w", inputPath, err)
	}

	var firstVideo, firstAudio *ffprobeStream
	for i := range document.Streams {
		stream := &document.Streams[i]
		switch stream.CodecType {
		case "video":
			if firstVideo == nil {
				firstVideo = stream
			}
		case "audio":
			if firstAudio == nil {
				firstAudio = stream
			}
		}
	}
	if firstVideo == nil && firstAudio == nil {
		return nil, fmt.Errorf("ffprobe %s: no media streams", inputPath)
	}

	meta := &MediaMetadata{
		Container: canonicalContainerName(jsonString(document.Format.FormatName)),
		DurationMs: int64(firstPositive(
			jsonFloat(document.Format.Duration),
			streamDuration(firstVideo),
			streamDuration(firstAudio),
		) * 1000),
	}
	if firstVideo != nil {
		meta.VideoCodec = strings.ToLower(strings.TrimSpace(firstVideo.CodecName))
		meta.PixelFormat = strings.ToLower(strings.TrimSpace(firstVideo.PixelFormat))
		meta.Width = firstVideo.Width
		meta.Height = firstVideo.Height
		meta.FPSNum, meta.FPSDen = parseRatio(firstVideo.FrameRate)
		meta.TimeBaseNum, meta.TimeBaseDen = parseRatio(firstVideo.TimeBase)
	}
	if firstAudio != nil {
		meta.AudioCodec = strings.ToLower(strings.TrimSpace(firstAudio.CodecName))
		meta.AudioSampleRate = jsonInt(firstAudio.SampleRate)
		meta.AudioChannels = jsonInt(firstAudio.Channels)
	}
	if meta.Container == "" {
		return nil, fmt.Errorf("ffprobe %s: empty container format", inputPath)
	}
	return meta, nil
}

// streamDuration returns the duration (seconds) of a stream, or 0 when the
// stream is nil or carries no duration.
func streamDuration(stream *ffprobeStream) float64 {
	if stream == nil {
		return 0
	}
	return jsonFloat(stream.Duration)
}

// canonicalContainerName normalizes ffprobe's format_name (e.g.
// "mov,mp4,m4a,3gp,3g2,mj2") to the first canonical container token.
func canonicalContainerName(raw string) string {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if index := strings.Index(trimmed, ","); index >= 0 {
		trimmed = strings.TrimSpace(trimmed[:index])
	}
	return trimmed
}

// jsonString decodes a RawMessage carrying a string.
func jsonString(raw json.RawMessage) string {
	return strings.Trim(strings.TrimSpace(string(raw)), `"`)
}

// isMediaMIME reports whether a MIME type describes a video or audio asset —
// the only kinds that can carry canonical media metadata. Non-media assets
// (fonts, subtitles, project files) never get a metadata row.
func isMediaMIME(mimeType string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(mimeType))
	return strings.HasPrefix(trimmed, "video/") || strings.HasPrefix(trimmed, "audio/")
}

// persistMediaMetadata runs the canonical one-time probe for a just-registered
// media asset and stores the verified metadata. Best-effort by contract: a
// failed probe leaves the row absent (metadata_verified=false) and NEVER
// rolls back the registration — inputsecurity has already validated the
// bytes, so metadata is an enhancement, not the trust boundary.
func (s *AssetService) persistMediaMetadata(ctx context.Context, assetID, finalPath, mimeType string) {
	if s == nil || s.repo == nil || s.mediaMetadata == nil {
		return
	}
	if !isMediaMIME(mimeType) {
		return
	}
	meta, err := s.mediaMetadata.Resolve(ctx, finalPath)
	if err != nil {
		log.Printf("[ASSETS] media metadata probe skipped asset=%s mime=%s: %v", assetID, mimeType, err)
		s.observeMetadataOutcome(MetadataOutcomeProbeFailed)
		return
	}
	if err := s.upsertMediaMetadataRecord(ctx, assetID, meta); err != nil {
		s.observeMetadataOutcome(MetadataOutcomePersistFailed)
		return
	}
	s.observeMetadataOutcome(MetadataOutcomeVerified)
}

// mediaMetadataRecord stamps the canonical schema version + verified-at
// timestamp without persisting it. Registration uses this value to perform
// an atomic asset+metadata insert; the generic path passes it to Upsert.
func (s *AssetService) mediaMetadataRecord(assetID string, meta *MediaMetadata) (MediaMetadataRecord, error) {
	if meta == nil {
		return MediaMetadataRecord{}, fmt.Errorf("media metadata record requires a probe result")
	}
	if s == nil || s.clock == nil {
		return MediaMetadataRecord{}, fmt.Errorf("media metadata clock unavailable")
	}
	now := s.clock.Now().UTC().Format(time.RFC3339)
	return MediaMetadataRecord{
		AssetID:               assetID,
		Container:             meta.Container,
		DurationMs:            meta.DurationMs,
		VideoCodec:            meta.VideoCodec,
		PixelFormat:           meta.PixelFormat,
		Width:                 meta.Width,
		Height:                meta.Height,
		FPSNum:                meta.FPSNum,
		FPSDen:                meta.FPSDen,
		TimeBaseNum:           meta.TimeBaseNum,
		TimeBaseDen:           meta.TimeBaseDen,
		AudioCodec:            meta.AudioCodec,
		AudioSampleRate:       meta.AudioSampleRate,
		AudioChannels:         meta.AudioChannels,
		MetadataVerifiedAt:    now,
		MetadataSchemaVersion: MediaMetadataSchemaVersion,
	}, nil
}

// upsertMediaMetadataRecord stamps the canonical schema version + verified-at
// timestamp and persists the row (idempotent upsert). Shared by the
// registration-time best-effort path (persistMediaMetadata) and the
// job-time fail-closed path (EnsureMediaMetadata) so both produce the SAME
// canonical record shape.
func (s *AssetService) upsertMediaMetadataRecord(ctx context.Context, assetID string, meta *MediaMetadata) error {
	record, err := s.mediaMetadataRecord(assetID, meta)
	if err != nil {
		return err
	}
	if err := s.repo.UpsertMediaMetadata(ctx, assetID, record); err != nil {
		log.Printf("[ASSETS] media metadata persist failed asset=%s: %v", assetID, err)
		return err
	}
	return nil
}

// EnsureMediaMetadata returns the verified media metadata for an asset,
// consuming the registry as authoritative (Fase C2). Rules:
//
//   - registered + verified row   → return it, NO probe (input-asset probing
//     is eliminated for already-registered assets);
//   - registered media, no verified row → probe ONCE through the canonical
//     MediaMetadataResolver and persist the result; a probe failure is
//     returned as an error so the caller can fail closed (reject) instead of
//     inventing metadata;
//   - non-media asset             → (nil, nil).
//
// Job-time consumers MUST use this accessor (or GetMediaMetadata) instead of
// spawning their own ffprobe.
func (s *AssetService) EnsureMediaMetadata(ctx context.Context, assetID string) (*MediaMetadata, error) {
	if s == nil || s.repo == nil || s.mediaMetadata == nil {
		return nil, fmt.Errorf("asset service unavailable")
	}
	assetID = strings.TrimSpace(assetID)
	if assetID == "" {
		return nil, fmt.Errorf("asset id required")
	}
	asset, err := s.Get(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if asset == nil {
		return nil, fmt.Errorf("asset %s not found", assetID)
	}
	if !isMediaMIME(asset.MimeType) {
		return nil, nil
	}
	rec, recErr := s.repo.GetMediaMetadata(ctx, assetID)
	if recErr != nil {
		return nil, recErr
	}
	if rec != nil && rec.Verified() {
		return rec.ToDomain(), nil
	}
	if s.blobStore == nil {
		return nil, fmt.Errorf("asset service unavailable: final blob store is not configured")
	}

	// No verified row: probe ONCE via the canonical component and persist.
	probePath := resolveFinalBlobPath(s.blobStore.FinalDir(), asset.StorageKey)
	if probePath == "" {
		return nil, fmt.Errorf("asset %s has no final blob path", assetID)
	}
	meta, probeErr := s.mediaMetadata.Resolve(ctx, probePath)
	if probeErr != nil {
		s.observeMetadataOutcome(MetadataOutcomeProbeFailed)
		return nil, fmt.Errorf("asset %s has no verified media metadata: %w", assetID, probeErr)
	}
	if persistErr := s.upsertMediaMetadataRecord(ctx, assetID, meta); persistErr != nil {
		// A successful probe is not enough for this fail-closed accessor: the
		// registry must be able to serve the same verified metadata to the next
		// consumer. Keep the row unverified and refuse the current operation.
		s.observeMetadataOutcome(MetadataOutcomePersistFailed)
		return nil, fmt.Errorf("asset %s verified media metadata could not be persisted: %w", assetID, persistErr)
	}
	s.observeMetadataOutcome(MetadataOutcomeVerified)
	return meta, nil
}

// resolveFinalBlobPath resolves an asset StorageKey into a filesystem path
// for probing: absolute keys are used as-is (legacy FilesystemBlobStore
// PromoteToFinal returns absolute paths); relative keys are resolved against
// the final directory.
func resolveFinalBlobPath(finalDir, storageKey string) string {
	cleaned := strings.TrimSpace(storageKey)
	if cleaned == "" {
		return ""
	}
	if filepath.IsAbs(cleaned) {
		return filepath.Clean(cleaned)
	}
	return filepath.Join(finalDir, filepath.FromSlash(cleaned))
}

// observeMetadataOutcome records a bounded probe-pipeline outcome (nil-safe
// so struct-literal test services without the metrics surface keep working).
func (s *AssetService) observeMetadataOutcome(outcome MediaMetadataOutcome) {
	if s == nil || s.mediaMetadataMetrics == nil {
		return
	}
	s.mediaMetadataMetrics.Observe(outcome)
}
