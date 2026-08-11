// Package storage / resolver.go — canonical StorageResolver (Fase E1).
//
// A single placement abstraction over the worker's three storage classes.
// Every subsystem (asset cache, render scratch, artifact staging) resolves
// WHERE a file lands through this package instead of scattering
// os.TempDir()/filepath.Join calls across executors and handlers.
//
// Classes:
//
//	CACHE_PERSISTENT — reusable asset blobs that must survive jobs → NVMe
//	ATTEMPT_TEMP     — per-attempt scratch (manifests, concat lists, small
//	                   audio fragments, control files, segments) → tmpfs
//	                   for small files ONLY, NVMe otherwise
//	ARTIFACT_FINAL   — the final artifact staged for upload → NVMe
//
// tmpfs rule: an ATTEMPT_TEMP file is eligible for TmpfsDir only when its
// size is in [0, TmpfsThresholdBytes). Files at/above the threshold — and
// files with an UNKNOWN size (-1) — always land on TempDir (NVMe). The
// 346 MB final artifact and the 26 large intermediates NEVER go to
// tmpfs/RAM. The threshold is a benchmarked starting point, not a
// universal truth: operators tune it per fleet.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// Class is a canonical storage class.
type Class string

const (
	// CachePersistent is the long-lived asset cache (survives jobs).
	CachePersistent Class = "CACHE_PERSISTENT"
	// AttemptTemp is per-attempt scratch storage.
	AttemptTemp Class = "ATTEMPT_TEMP"
	// ArtifactFinal is where the final artifact is staged before upload.
	ArtifactFinal Class = "ARTIFACT_FINAL"
)

// Backing is the physical medium selected for a placement. It is a
// placement OUTCOME (reported in Placement for metrics/debugging), not a
// knob: callers never choose a backing directly.
type Backing string

const (
	// BackingTmpfs is the memory-backed medium (TmpfsDir).
	BackingTmpfs Backing = "tmpfs"
	// BackingNvme is the durable medium (CacheDir / TempDir / ArtifactDir).
	BackingNvme Backing = "nvme"
)

// Config describes the backing directories for each class. All paths are
// resolved from the worker configuration (see the composition root in
// internal/worker); this struct is the storage layer's own contract.
type Config struct {
	// CacheDir is the CACHE_PERSISTENT backing (NVMe). Required.
	CacheDir string
	// TempDir is the ATTEMPT_TEMP NVMe backing. Required.
	TempDir string
	// TmpfsDir is the ATTEMPT_TEMP tmpfs backing (e.g. /dev/shm/velox-worker).
	// Empty disables tmpfs entirely — all scratch lands on TempDir.
	TmpfsDir string
	// ArtifactDir is the ARTIFACT_FINAL backing (NVMe). Required.
	ArtifactDir string
	// TmpfsThresholdBytes is the size gate for tmpfs placement: only files
	// with sizeBytes in [0, threshold) are eligible. Files at/above the
	// threshold (or with unknown size) always go to TempDir. Must be > 0
	// when TmpfsDir is set.
	TmpfsThresholdBytes int64
}

// Validate fails closed on missing required backings or an inconsistent
// tmpfs configuration. It never touches the filesystem.
func (c Config) Validate() error {
	if c.CacheDir == "" {
		return fmt.Errorf("storage: cache_dir (CACHE_PERSISTENT) is required")
	}
	if c.TempDir == "" {
		return fmt.Errorf("storage: temp_dir (ATTEMPT_TEMP NVMe backing) is required")
	}
	if c.ArtifactDir == "" {
		return fmt.Errorf("storage: artifact_dir (ARTIFACT_FINAL) is required")
	}
	if c.TmpfsDir != "" && c.TmpfsThresholdBytes <= 0 {
		return fmt.Errorf("storage: tmpfs_dir is set but tmpfs_threshold_bytes must be > 0")
	}
	return nil
}

// Resolver places files onto storage classes. It is safe for concurrent
// use: it holds no mutable state beyond the immutable Config.
type Resolver struct {
	cfg Config
}

// New builds a Resolver. It does NOT touch the filesystem — call
// EnsureDirs (or rely on the doctor / bootstrap gate) to materialize the
// directories before first write.
func New(cfg Config) (*Resolver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Resolver{cfg: cfg}, nil
}

// Placement is the outcome of a placement decision: which class, which
// physical backing, and the full resolved path.
type Placement struct {
	Class   Class
	Backing Backing
	Path    string
}

// Class returns the backing directory root for a class. For
// CACHE_PERSISTENT / ARTIFACT_FINAL this is the NVMe root; for
// ATTEMPT_TEMP it is the NVMe scratch root (use Place for the
// threshold-aware decision).
func (r *Resolver) Class(class Class) (string, error) {
	if r == nil {
		return "", fmt.Errorf("storage: nil resolver")
	}
	switch class {
	case CachePersistent:
		return r.cfg.CacheDir, nil
	case AttemptTemp:
		return r.cfg.TempDir, nil
	case ArtifactFinal:
		return r.cfg.ArtifactDir, nil
	default:
		return "", fmt.Errorf("storage: unknown class %q", class)
	}
}

// Place resolves the full path for rel under class.
//
// sizeBytes is the expected file size; pass -1 when unknown. The tmpfs
// gate applies ONLY to ATTEMPT_TEMP:
//
//	0 <= sizeBytes < TmpfsThresholdBytes AND TmpfsDir configured
//	    → TmpfsDir (tmpfs)
//	anything else (unknown, at/above threshold, tmpfs disabled)
//	    → TempDir (NVMe)
//
// CACHE_PERSISTENT and ARTIFACT_FINAL are always NVMe — a final artifact
// or a cached blob can never be routed to tmpfs regardless of size.
func (r *Resolver) Place(class Class, rel string, sizeBytes int64) (Placement, error) {
	if r == nil {
		return Placement{}, fmt.Errorf("storage: nil resolver")
	}
	switch class {
	case CachePersistent:
		return Placement{Class: class, Backing: BackingNvme, Path: filepath.Join(r.cfg.CacheDir, rel)}, nil
	case ArtifactFinal:
		return Placement{Class: class, Backing: BackingNvme, Path: filepath.Join(r.cfg.ArtifactDir, rel)}, nil
	case AttemptTemp:
		if r.TmpfsEligible(sizeBytes) {
			return Placement{Class: class, Backing: BackingTmpfs, Path: filepath.Join(r.cfg.TmpfsDir, rel)}, nil
		}
		return Placement{Class: class, Backing: BackingNvme, Path: filepath.Join(r.cfg.TempDir, rel)}, nil
	default:
		return Placement{}, fmt.Errorf("storage: unknown class %q", class)
	}
}

// TmpfsEligible reports whether sizeBytes may be placed on tmpfs. Unknown
// sizes (-1) are never eligible; neither is any size when TmpfsDir is
// unset or the threshold is not positive.
func (r *Resolver) TmpfsEligible(sizeBytes int64) bool {
	if r == nil || r.cfg.TmpfsDir == "" || r.cfg.TmpfsThresholdBytes <= 0 {
		return false
	}
	return sizeBytes >= 0 && sizeBytes < r.cfg.TmpfsThresholdBytes
}

// EnsureDirs creates every configured backing directory (0755). Idempotent.
func (r *Resolver) EnsureDirs() error {
	if r == nil {
		return fmt.Errorf("storage: nil resolver")
	}
	for _, dir := range []string{r.cfg.CacheDir, r.cfg.TempDir, r.cfg.ArtifactDir, r.cfg.TmpfsDir} {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("storage: create %s: %w", dir, err)
		}
	}
	return nil
}

// Config exposes the resolved backing configuration for diagnostics and
// metrics. The returned value is a copy.
func (r *Resolver) Config() Config {
	if r == nil {
		return Config{}
	}
	return r.cfg
}

// String returns a compact diagnostic summary.
func (r *Resolver) String() string {
	if r == nil {
		return "storage{nil}"
	}
	return fmt.Sprintf("storage{cache=%s temp=%s tmpfs=%s tmpfs_threshold_bytes=%d artifact=%s}",
		r.cfg.CacheDir, r.cfg.TempDir, r.cfg.TmpfsDir, r.cfg.TmpfsThresholdBytes, r.cfg.ArtifactDir)
}
