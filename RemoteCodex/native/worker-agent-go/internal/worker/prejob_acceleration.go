package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/prefetch"
)

const prejobWarmBudgetBytes int64 = 64 << 20

type prejobPreparationResult struct {
	JobID            string    `json:"job_id"`
	TaskID           string    `json:"task_id"`
	TaskRevision     int       `json:"task_revision"`
	ReservationID    string    `json:"reservation_id"`
	PlanID           string    `json:"plan_id"`
	PlanVersion      uint64    `json:"plan_version"`
	Distance         int       `json:"distance"`
	PreparedAt       time.Time `json:"prepared_at"`
	FinalizedAt      time.Time `json:"finalized_at"`
	AssetsPrepared   int       `json:"assets_prepared"`
	PreparedBytes    int64     `json:"prepared_bytes"`
	WarmAdvisedBytes int64     `json:"warm_advised_bytes"`
	DiskFreeBytes    int64     `json:"disk_free_bytes"`
	EvidencePath     string    `json:"evidence_path"`
	OutputDir        string    `json:"output_dir"`
	PublisherDir     string    `json:"publisher_dir"`
}

// prepareLocalPreJob completes the worker-local preparation barrier after all
// assets are verified but before the worker emits prefetch_prepared to the
// Master. Therefore strict preparation claims include not just bytes-on-disk,
// but also durable lineage evidence, output/scratch readiness and a local
// publisher-journal root.
//
// Remote multipart/progressive sessions are intentionally NOT created here:
// they require an ArtifactUploadPlan/commit token and fabricating one would
// violate the artifact commit protocol. The local journal directory is
// prepared now so BeginProgressive can start immediately when the real plan
// arrives.
func (w *Worker) prepareLocalPreJob(ctx context.Context, job prefetch.PreparedJob) (prejobPreparationResult, error) {
	if w == nil || w.config == nil {
		return prejobPreparationResult{}, fmt.Errorf("prejob: worker config unavailable")
	}
	root := strings.TrimSpace(w.config.StateDir)
	if root == "" {
		root = strings.TrimSpace(w.config.WorkDir)
	}
	if root == "" {
		return prejobPreparationResult{}, fmt.Errorf("prejob: no state/work directory configured")
	}
	identity := sha256.Sum256([]byte(job.JobID + "\x00" + job.TaskID))
	prejobDir := filepath.Join(root, "prejobs", hex.EncodeToString(identity[:16]))
	outputDir := strings.TrimSpace(w.config.OutputDir)
	if outputDir == "" {
		outputDir = filepath.Join(prejobDir, "output")
	}
	publisherDir := filepath.Join(prejobDir, "publisher", "progressive")
	for _, dir := range []string{prejobDir, outputDir, publisherDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return prejobPreparationResult{}, fmt.Errorf("prejob: mkdir %s: %w", dir, err)
		}
	}

	diskFree, err := prejobDiskFreeBytes(outputDir)
	if err != nil {
		return prejobPreparationResult{}, fmt.Errorf("prejob: disk preflight: %w", err)
	}
	if diskFree >= 0 && w.config.MinDiskFreeMB > 0 {
		floor := int64(w.config.MinDiskFreeMB) << 20
		if diskFree < floor {
			return prejobPreparationResult{}, fmt.Errorf("prejob: disk free %d below configured floor %d", diskFree, floor)
		}
	}

	// N+2/N+3 assets stay verified and pinned but do not consume page cache
	// speculatively. Only the next job receives a bounded warmup.
	var warmed int64
	if job.Distance <= 1 {
		warmed, err = prefetch.WarmPreparedJob(ctx, job, prejobWarmBudgetBytes)
		if err != nil {
			return prejobPreparationResult{}, fmt.Errorf("prejob: warm prepared assets: %w", err)
		}
	}

	evidenceRoot := filepath.Join(root, "prejobs", "evidence")
	evidencePath, err := prefetch.PersistPreparedEvidence(evidenceRoot, job)
	if err != nil {
		return prejobPreparationResult{}, fmt.Errorf("prejob: persist prepared evidence: %w", err)
	}
	cert := job.Certificate()
	result := prejobPreparationResult{
		JobID: job.JobID, TaskID: job.TaskID, TaskRevision: job.TaskRevision,
		ReservationID: job.ReservationID, PlanID: job.PlanID, PlanVersion: job.PlanVersion,
		Distance: job.Distance, PreparedAt: job.PreparedAt, FinalizedAt: time.Now().UTC(),
		AssetsPrepared: cert.AssetsPrepared, PreparedBytes: cert.PreparedBytes,
		WarmAdvisedBytes: warmed, DiskFreeBytes: diskFree, EvidencePath: evidencePath,
		OutputDir: outputDir, PublisherDir: publisherDir,
	}
	if err := persistPrejobResult(prejobDir, result); err != nil {
		return prejobPreparationResult{}, fmt.Errorf("prejob: persist finalization: %w", err)
	}
	return result, nil
}

func persistPrejobResult(dir string, result prejobPreparationResult) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".prejob-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
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
	return os.Rename(name, filepath.Join(dir, "prejob.json"))
}
