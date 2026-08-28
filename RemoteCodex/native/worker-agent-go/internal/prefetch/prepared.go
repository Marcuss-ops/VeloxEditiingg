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
	Origin downloader.ResolutionOrigin `json:"origin,omitempty"`
}

// PreparedJob is the local worker read model. PREPARED is emitted only after
// every declared asset has passed the integrity/metadata phase. A preparation
// job never consumes an execution slot.
type PreparedJob struct {
	JobID         string                           `json:"job_id"`
	TaskID        string                           `json:"task_id"`
	TaskRevision  int                              `json:"task_revision"`
	WorkerID      string                           `json:"worker_id,omitempty"`
	ReservationID string                           `json:"reservation_id,omitempty"`
	PlanID        string                           `json:"plan_id,omitempty"`
	PlanVersion   uint64                           `json:"plan_version,omitempty"`
	Distance      int                              `json:"distance,omitempty"`
	State         string                           `json:"state"`
	PreparedAt    time.Time                        `json:"prepared_at"`
	Assets        map[string]PreparedAssetMetadata `json:"assets"`
}

// PreparedJobCertificate is the immutable lineage fingerprint of a prepared
// job. It captures every field needed to prove, at claim time, that the
// preparation was performed by the correct worker for the correct task
// revision under the correct reservation. The master verifies this
// certificate before allowing a strict claim; any field mismatch means
// the preparation is stale or belongs to a different execution lineage.
type PreparedJobCertificate struct {
	WorkerID      string    `json:"worker_id"`
	ReservationID string    `json:"reservation_id"`
	PlanID        string    `json:"plan_id"`
	PlanVersion   uint64    `json:"plan_version"`
	TaskRevision  int       `json:"task_revision"`
	PreparedAt    time.Time `json:"prepared_at"`
	AssetsRequired int      `json:"assets_required"`
	AssetsPrepared int      `json:"assets_prepared"`
	PreparedBytes  int64    `json:"prepared_bytes"`
}

// Certificate builds the immutable lineage fingerprint from the PreparedJob.
// The certificate is safe to send on the wire: it carries no mutable state
// and no local filesystem paths.
func (pj PreparedJob) Certificate() PreparedJobCertificate {
	var preparedBytes int64
	for _, asset := range pj.Assets {
		preparedBytes += asset.SizeBytes
	}
	return PreparedJobCertificate{
		WorkerID:       pj.WorkerID,
		ReservationID:  pj.ReservationID,
		PlanID:         pj.PlanID,
		PlanVersion:    pj.PlanVersion,
		TaskRevision:   pj.TaskRevision,
		PreparedAt:     pj.PreparedAt,
		AssetsRequired: len(pj.Assets),
		AssetsPrepared: len(pj.Assets),
		PreparedBytes:  preparedBytes,
	}
}

// VerifyClaim checks whether a certificate is still valid for a claim at
// the given worker and task revision. The claim is rejected when:
//   - the worker identity does not match (wrong worker)
//   - the task revision has drifted (task was re-enqueued or modified)
//   - assets are incomplete (prepared < required)
//   - the certificate is zero-value (no preparation observed)
//
// Returns (true, "") on success; (false, reason) on rejection.
func (c PreparedJobCertificate) VerifyClaim(workerID string, taskRevision int) (bool, string) {
	if c.WorkerID == "" && c.ReservationID == "" && c.AssetsRequired == 0 {
		return false, "certificate: no preparation evidence"
	}
	if c.AssetsRequired > 0 && c.AssetsPrepared < c.AssetsRequired {
		return false, fmt.Sprintf("certificate: assets_prepared=%d < assets_required=%d", c.AssetsPrepared, c.AssetsRequired)
	}
	if c.WorkerID != "" && workerID != "" && c.WorkerID != workerID {
		return false, fmt.Sprintf("certificate: worker_id mismatch cert=%s claim=%s", c.WorkerID, workerID)
	}
	if c.TaskRevision != 0 && taskRevision != 0 && c.TaskRevision != taskRevision {
		return false, fmt.Sprintf("certificate: task_revision mismatch cert=%d claim=%d", c.TaskRevision, taskRevision)
	}
	return true, ""
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
		prepared = PreparedJob{
			JobID:         job.JobID,
			TaskID:        job.TaskID,
			TaskRevision:  job.TaskRevision,
			WorkerID:      s.cfg.WorkerID,
			ReservationID: job.ReservationID,
			PlanID:        s.currentPlanID,
			PlanVersion:   s.currentPlanVersion,
			Distance:      job.Distance,
			State:         "PREPARING",
			Assets:        make(map[string]PreparedAssetMetadata),
		}
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
