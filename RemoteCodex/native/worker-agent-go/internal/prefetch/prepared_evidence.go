package prefetch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const preparedEvidenceSchema = 1

// LocalPreparedEvidence is the durable worker-local proof that a future job
// reached PREPARED. Unlike the control-plane certificate it intentionally
// retains local paths because the file never leaves the worker. This lets
// restart diagnostics prove which exact verified blobs were bound while the
// public lifecycle event remains path-minimal.
type LocalPreparedEvidence struct {
	SchemaVersion int                    `json:"schema_version"`
	Certificate   PreparedJobCertificate `json:"certificate"`
	JobID         string                 `json:"job_id"`
	TaskID        string                 `json:"task_id"`
	TaskRevision  int                    `json:"task_revision"`
	State         string                 `json:"state"`
	PreparedAt    time.Time              `json:"prepared_at"`
	Assets        []LocalPreparedAsset   `json:"assets"`
}

type LocalPreparedAsset struct {
	AssetKey    string                      `json:"asset_key"`
	AssetID     string                      `json:"asset_id"`
	SHA256      string                      `json:"sha256"`
	SizeBytes   int64                       `json:"size_bytes"`
	LocalPath   string                      `json:"local_path"`
	PreparedAt  time.Time                   `json:"prepared_at"`
	MIMEType    string                      `json:"mime_type,omitempty"`
	Codec       string                      `json:"codec,omitempty"`
	AudioCodec  string                      `json:"audio_codec,omitempty"`
	DurationSec float64                     `json:"duration_sec,omitempty"`
	Origin      downloaderResolutionOrigin `json:"origin,omitempty"`
}

// downloaderResolutionOrigin is kept as a local alias-like string so the
// evidence schema stays stable even if the downloader's Go type moves.
type downloaderResolutionOrigin string

// PersistPreparedEvidence writes one atomic evidence document below root. The
// file name is a hash of job/task identity rather than user-controlled text,
// preventing path traversal and keeping the directory bounded to regular
// files. Callers may safely overwrite the same job after a newer plan; the
// embedded task revision/reservation/plan version is the freshness fence.
func PersistPreparedEvidence(root string, job PreparedJob) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", fmt.Errorf("prefetch evidence: root is empty")
	}
	if job.JobID == "" || job.TaskID == "" || job.State != PreparationStatePrepared {
		return "", fmt.Errorf("prefetch evidence: incomplete prepared job identity")
	}
	assets := make([]LocalPreparedAsset, 0, len(job.Assets))
	for _, a := range job.Assets {
		assets = append(assets, LocalPreparedAsset{
			AssetKey: a.AssetKey, AssetID: a.AssetID, SHA256: a.SHA256,
			SizeBytes: a.SizeBytes, LocalPath: a.LocalPath, PreparedAt: a.PreparedAt,
			MIMEType: a.MIMEType, Codec: a.Codec, AudioCodec: a.AudioCodec,
			DurationSec: a.DurationSec, Origin: downloaderResolutionOrigin(a.Origin),
		})
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].AssetKey < assets[j].AssetKey })
	evidence := LocalPreparedEvidence{
		SchemaVersion: preparedEvidenceSchema,
		Certificate: job.Certificate(),
		JobID: job.JobID, TaskID: job.TaskID, TaskRevision: job.TaskRevision,
		State: job.State, PreparedAt: job.PreparedAt, Assets: assets,
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, "prepared")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(job.JobID + "\x00" + job.TaskID))
	dst := filepath.Join(dir, hex.EncodeToString(h[:16])+".json")
	tmp, err := os.CreateTemp(dir, ".prepared-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return "", err
	}
	return dst, nil
}

// LoadPreparedEvidence reads the worker-local evidence for a job/task. It is
// primarily an audit/recovery primitive; live execution still re-enters the
// canonical CacheResolver so cache validity and eviction remain authoritative.
func LoadPreparedEvidence(root, jobID, taskID string) (LocalPreparedEvidence, error) {
	h := sha256.Sum256([]byte(jobID + "\x00" + taskID))
	path := filepath.Join(root, "prepared", hex.EncodeToString(h[:16])+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return LocalPreparedEvidence{}, err
	}
	var evidence LocalPreparedEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		return LocalPreparedEvidence{}, err
	}
	if evidence.SchemaVersion != preparedEvidenceSchema || evidence.JobID != jobID || evidence.TaskID != taskID {
		return LocalPreparedEvidence{}, fmt.Errorf("prefetch evidence: identity/schema mismatch")
	}
	return evidence, nil
}
