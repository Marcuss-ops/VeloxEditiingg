package chunkfactory

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ManifestVersion identifies the worker-local W5 manifest format. The first
// consumer is deliberately local: it records reusable CMAF-ready payloads
// without changing the legacy monolithic upload contract.
const ManifestVersion = 1

type Manifest struct {
	Version     int             `json:"version"`
	JobID       string          `json:"job_id"`
	ProfileID   string          `json:"profile_id"`
	DurationUS  int64           `json:"duration_us"`
	TimelineSHA string          `json:"timeline_sha256,omitempty"`
	Chunks      []ManifestChunk `json:"chunks"`
}

type ManifestChunk struct {
	ChunkID       string `json:"chunk_id"`
	AssetKey      string `json:"asset_key"`
	ProfileID     string `json:"profile_id"`
	ChunkIndex    int    `json:"chunk_index"`
	SourceInUS    int64  `json:"source_in_us"`
	SourceOutUS   int64  `json:"source_out_us"`
	PayloadSHA256 string `json:"payload_sha256"`
	SizeBytes     int64  `json:"size_bytes"`
	PayloadPath   string `json:"payload_path"`
}

// ManifestPath returns the durable worker-local path for one job manifest.
// Job IDs are validated by the executor before they reach this boundary.
func (s *Store) ManifestPath(jobID string) string {
	return filepath.Join(s.root, "manifests", jobID+".json")
}

// PutManifest atomically persists a manifest. It is intentionally separate
// from PutChunk: manifests are job observations, while chunks are reusable
// content-addressed payloads.
func (s *Store) PutManifest(manifest Manifest) error {
	if s == nil || strings.TrimSpace(s.root) == "" {
		return fmt.Errorf("chunkfactory: manifest store is not configured")
	}
	if manifest.Version != ManifestVersion || strings.TrimSpace(manifest.JobID) == "" || len(manifest.Chunks) == 0 {
		return fmt.Errorf("chunkfactory: invalid manifest")
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("chunkfactory: manifest marshal: %w", err)
	}
	dir := filepath.Dir(s.ManifestPath(manifest.JobID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("chunkfactory: manifest mkdir: %w", err)
	}
	tmp := s.ManifestPath(manifest.JobID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("chunkfactory: manifest write: %w", err)
	}
	if err := os.Rename(tmp, s.ManifestPath(manifest.JobID)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("chunkfactory: manifest rename: %w", err)
	}
	return nil
}
