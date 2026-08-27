package prefetch

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/publisher"
)

// PreparedAssetMetadata is the worker-side evidence that one asset is
// physically ready. The path is intentionally excluded from JSON-facing
// telemetry consumers; it is retained here for the local resolver only.
type PreparedAssetMetadata struct {
	AssetKey     string    `json:"asset_key"`
	AssetID      string    `json:"asset_id"`
	SHA256       string    `json:"sha256"`
	SizeBytes    int64     `json:"size_bytes"`
	MIMEType     string    `json:"mime_type"`
	Format       string    `json:"format,omitempty"`
	Codec        string    `json:"codec,omitempty"`
	AudioCodec   string    `json:"audio_codec,omitempty"`
	DurationSec  float64   `json:"duration_sec,omitempty"`
	Width        int       `json:"width,omitempty"`
	Height       int       `json:"height,omitempty"`
	HasVideo     bool      `json:"has_video"`
	HasAudio     bool      `json:"has_audio"`
	FfprobeOK    bool      `json:"ffprobe_ok"`
	FfprobeValid bool      `json:"ffprobe_valid"`
	FfprobeError string    `json:"ffprobe_error,omitempty"`
	LocalPath    string    `json:"-"`
	PreparedAt   time.Time `json:"prepared_at"`
	// Origin is the AssetResolutionOrigin for this prepared asset:
	// warm_cache, prefetch, or runtime_download. It is set at prefetch-time
	// when the asset is materialized by a FutureAssetPlan and propagated
	// through to runtimeassets.Binding for fast-assembly certification.
	Origin       downloader.ResolutionOrigin `json:"origin,omitempty"`
}

// PreparedJob is the local worker read model. PREPARED is emitted only after
// every declared asset has passed the integrity/metadata phase. A preparation
// job never consumes an execution slot.
type PreparedJob struct {
	JobID      string                           `json:"job_id"`
	TaskID     string                           `json:"task_id"`
	State      string                           `json:"state"`
	PreparedAt time.Time                        `json:"prepared_at"`
	Assets     map[string]PreparedAssetMetadata `json:"assets"`
}

const PreparationStatePrepared = "PREPARED"

// MetadataResolver is injectable for tests and for deployments that provide
// a platform-specific media probe. Production defaults to ComputeLocalManifest
// and therefore uses the worker's ffprobe integration.
type MetadataResolver func(context.Context, futureasset.AssetManifest, downloader.CacheResolution) (PreparedAssetMetadata, error)

// defaultMetadataResolver recomputes the verified local manifest after the
// cache/download phase. This gives cache hits the same evidence as downloads:
// SHA256, byte size, MIME/container metadata and ffprobe details.
func defaultMetadataResolver(ctx context.Context, asset futureasset.AssetManifest, resolved downloader.CacheResolution) (PreparedAssetMetadata, error) {
	if strings.TrimSpace(resolved.LocalPath) == "" {
		return PreparedAssetMetadata{}, fmt.Errorf("prefetch: asset %q has no local path", asset.AssetKey)
	}
	manifest, err := publisher.ComputeLocalManifest(ctx, resolved.LocalPath)
	if err != nil {
		return PreparedAssetMetadata{}, fmt.Errorf("prefetch: asset %q metadata: %w", asset.AssetKey, err)
	}
	if asset.SizeBytes > 0 && manifest.SizeBytes != asset.SizeBytes {
		return PreparedAssetMetadata{}, fmt.Errorf("prefetch: asset %q size mismatch: got %d want %d", asset.AssetKey, manifest.SizeBytes, asset.SizeBytes)
	}
	if strings.TrimSpace(asset.SHA256) != "" {
		expected := normalizedSHA256(asset.SHA256)
		if expected == "" {
			return PreparedAssetMetadata{}, fmt.Errorf("prefetch: asset %q has invalid sha256", asset.AssetKey)
		}
		if !strings.EqualFold(manifest.SHA256Hex, expected) {
			return PreparedAssetMetadata{}, fmt.Errorf("prefetch: asset %q sha256 mismatch: got %s want %s", asset.AssetKey, manifest.SHA256Hex, expected)
		}
	}
	return PreparedAssetMetadata{
		AssetKey: asset.AssetKey, AssetID: asset.AssetID, SHA256: manifest.SHA256Hex,
		SizeBytes: manifest.SizeBytes, MIMEType: manifest.MIMEType, Format: manifest.Format,
		Codec: manifest.Codec, AudioCodec: manifest.AudioCodec, DurationSec: manifest.DurationSec,
		Width: manifest.Width, Height: manifest.Height, HasVideo: manifest.HasVideoStream,
		HasAudio: manifest.HasAudioStream, FfprobeOK: manifest.FfprobeOK,
		FfprobeValid: manifest.FfprobeValid, FfprobeError: manifest.FfprobeErr,
		LocalPath: resolved.LocalPath, PreparedAt: time.Now().UTC(),
	}, nil
}

func normalizedSHA256(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(value), "sha256:"))
	if len(value) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(value); err != nil {
		return ""
	}
	return value
}

// preparedForJob atomically records one asset and returns a PREPARED event
// only when the job's complete asset set is represented with valid metadata.
func (s *Scheduler) preparedForJob(job futureasset.Job, metadata PreparedAssetMetadata) (PreparedJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.jobs[job.JobID]
	if !ok || current.job.TaskID != job.TaskID {
		return PreparedJob{}, false
	}
	if metadata.PreparedAt.IsZero() {
		metadata.PreparedAt = s.cfg.Now()
	}
	prepared := s.prepared[job.JobID]
	if prepared.Assets == nil {
		prepared = PreparedJob{JobID: job.JobID, TaskID: job.TaskID, State: "PREPARING", Assets: make(map[string]PreparedAssetMetadata)}
	}
	prepared.Assets[metadata.AssetKey] = metadata
	if len(prepared.Assets) != len(job.Assets) {
		s.prepared[job.JobID] = prepared
		return PreparedJob{}, false
	}
	for _, asset := range job.Assets {
		if _, exists := prepared.Assets[asset.AssetKey]; !exists {
			s.prepared[job.JobID] = prepared
			return PreparedJob{}, false
		}
	}
	prepared.State = PreparationStatePrepared
	prepared.PreparedAt = metadata.PreparedAt
	s.prepared[job.JobID] = prepared
	return prepared, true
}

// PreparedJobs returns a stable copy of the current local preparation read
// model. Callers cannot mutate scheduler state through the returned maps.
func (s *Scheduler) PreparedJobs() []PreparedJob {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	jobIDs := make([]string, 0, len(s.prepared))
	for jobID := range s.prepared {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	out := make([]PreparedJob, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job := s.prepared[jobID]
		copyJob := job
		copyJob.Assets = make(map[string]PreparedAssetMetadata, len(job.Assets))
		for key, metadata := range job.Assets {
			copyJob.Assets[key] = metadata
		}
		out = append(out, copyJob)
	}
	return out
}

// InvalidatePreparedAsset removes a single asset from the PreparedJob read
// model when its integrity check fails at runtime (SHA/size mismatch after
// prefetch). This prevents stale PreparedJob metadata from misclassifying
// future cache resolutions as OriginPrefetch when the asset was actually
// re-downloaded. If the job has no remaining prepared assets, the entire
// job entry is removed.
func (s *Scheduler) InvalidatePreparedAsset(jobID, assetKey string) {
	if s == nil || jobID == "" || assetKey == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.prepared[jobID]
	if !ok || job.Assets == nil {
		return
	}
	delete(job.Assets, assetKey)
	if len(job.Assets) == 0 {
		delete(s.prepared, jobID)
	} else {
		s.prepared[jobID] = job
	}
}
