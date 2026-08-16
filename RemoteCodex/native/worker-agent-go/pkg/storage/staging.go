// Package storage / staging.go — ArtifactStaging placement + RAM reservation.
//
// The final artifact can be staged either on durable NVMe (ArtifactDir) or
// on volatile tmpfs (ArtifactTmpfsDir). Tmpfs staging is an optimization:
// FFmpeg writes the output and the uploader reads it straight from RAM, but
// a hard crash loses the output (it must be re-rendered). This manager is
// the SINGLE place that decides the backing, so executors never scatter
// syscall.Statfs / "/dev/shm" checks around themselves.
//
// The manager keeps a reservation ledger: every tmpfs placement reserves
// its estimated byte size so concurrent renders cannot over-commit RAM
// (two jobs each seeing "4 GB free" and both reserving 3 GB). A placement
// that cannot be reserved falls back to durable NVMe instead of failing.
package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"syscall"
)

// ArtifactStagingConfig configures volatile tmpfs staging for
// ARTIFACT_STAGING placements. The zero value (Enabled=false) disables the
// feature entirely: every staging placement lands on durable ArtifactDir.
type ArtifactStagingConfig struct {
	// Enabled opts the worker into tmpfs staging. When false, Dir /
	// MaxPercent / ReserveBytes are ignored.
	Enabled bool
	// Dir is the tmpfs staging root (e.g. /dev/shm/velox-artifacts).
	// Required when Enabled.
	Dir string
	// MaxPercent caps the fraction (1-99) of the tmpfs total size that may
	// be reserved for staging. Headroom is always kept free on top of this.
	MaxPercent int
	// ReserveBytes is the headroom that must always remain free on the
	// tmpfs beyond currently reserved bytes (e.g. 512 MiB). Never reserve
	// the whole filesystem.
	ReserveBytes int64
}

// Validate fails closed on an inconsistent staging configuration. It never
// touches the filesystem.
func (c ArtifactStagingConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Dir == "" {
		return fmt.Errorf("storage: artifact_tmpfs_dir is required when artifact tmpfs staging is enabled")
	}
	if c.MaxPercent < 1 || c.MaxPercent > 99 {
		return fmt.Errorf("storage: artifact_tmpfs_max_percent must be in [1,99], got %d", c.MaxPercent)
	}
	if c.ReserveBytes <= 0 {
		return fmt.Errorf("storage: artifact_tmpfs_reserve_bytes must be > 0")
	}
	return nil
}

// FallbackReason describes why an ARTIFACT_STAGING placement fell back to
// durable NVMe instead of the volatile tmpfs backing. The empty string means
// "no fallback" (a tmpfs reservation succeeded). The values are the
// low-cardinality `reason` label set for velox_artifact_nvme_fallback_total.
type FallbackReason string

const (
	// FallbackNone means the placement reserved tmpfs successfully.
	FallbackNone FallbackReason = ""
	// FallbackTmpfsDisabled means ARTIFACT_STAGING is not enabled (or no
	// tmpfs dir is configured), so durable NVMe is the only backing.
	FallbackTmpfsDisabled FallbackReason = "tmpfs_disabled"
	// FallbackUnknownSize means the caller could not estimate the output
	// size (<= 0), so no RAM reservation is safe.
	FallbackUnknownSize FallbackReason = "unknown_size"
	// FallbackStatfsError means the tmpfs statfs probe failed.
	FallbackStatfsError FallbackReason = "statfs_error"
	// FallbackNoSpace means the tmpfs budget (free minus reserve headroom,
	// capped by MaxPercent) cannot satisfy the estimated reservation.
	FallbackNoSpace FallbackReason = "no_space"
)

// StagingMetrics is the optional observability sink for ARTIFACT_STAGING
// decisions. The concrete Prometheus implementation is wired at the worker
// composition root; nil disables collection (tests, headless harnesses).
// Keeping the interface local to the storage package preserves the
// foundation-layer isolation: pkg/storage never imports the concrete
// telemetry registry.
type StagingMetrics interface {
	RecordArtifactNvmeFallback(reason string)
	SetArtifactTmpfsReservedBytes(reserved int64)
}

// statfsProbe reports a filesystem's total and available bytes for a
// directory. Extracted as a function type so tests can inject a
// deterministic view of /dev/shm without mounting a real tmpfs.
type statfsProbe func(dir string) (totalBytes, availBytes int64, err error)

// statfsTmpfs is the production probe (syscall.Statfs).
func statfsTmpfs(dir string) (totalBytes, availBytes int64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	return int64(st.Blocks) * int64(st.Bsize), int64(st.Bavail) * int64(st.Bsize), nil
}

// ArtifactStagingManager is the RAM reservation ledger for ARTIFACT_STAGING
// tmpfs placements. It is safe for concurrent use. Reservations are tracked
// both as a running total (reserved) and per-path (byPath) so a later
// release that only knows the URI — e.g. post-commit cleanup — can free the
// exact bytes without re-deriving the original estimate.
type ArtifactStagingManager struct {
	mu       sync.Mutex
	cfg      ArtifactStagingConfig
	reserved int64
	byPath   map[string]int64
	statfs   statfsProbe
}

func newArtifactStagingManager(cfg ArtifactStagingConfig) *ArtifactStagingManager {
	return &ArtifactStagingManager{cfg: cfg, statfs: statfsTmpfs}
}

// reserve attempts to reserve estimatedBytes of tmpfs capacity and records
// the reservation under the full path it returns. On failure — or when the
// feature is disabled, or the size is unknown (-1) — it returns ok=false and
// the caller falls back to durable NVMe. Unknown sizes are never eligible:
// better to lose the optimization than risk ENOSPC at 98% of the render.
func (m *ArtifactStagingManager) reserve(rel string, estimatedBytes int64) (path string, reservedBytes int64, reason FallbackReason, ok bool) {
	if m == nil || !m.cfg.Enabled || m.cfg.Dir == "" {
		return "", 0, FallbackTmpfsDisabled, false
	}
	if estimatedBytes <= 0 {
		return "", 0, FallbackUnknownSize, false
	}
	total, avail, err := m.statfs(m.cfg.Dir)
	if err != nil || total <= 0 || avail <= 0 {
		return "", 0, FallbackStatfsError, false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Ceiling: never reserve more than MaxPercent of the tmpfs total.
	ceiling := total * int64(m.cfg.MaxPercent) / 100
	// Headroom: keep ReserveBytes free beyond what is already reserved.
	budget := avail - m.cfg.ReserveBytes
	if budget < 0 {
		budget = 0
	}
	if remaining := ceiling - m.reserved; remaining < budget {
		budget = remaining
	}
	if budget < estimatedBytes {
		return "", 0, FallbackNoSpace, false
	}
	m.reserved += estimatedBytes
	path = filepath.Join(m.cfg.Dir, rel)
	if m.byPath == nil {
		m.byPath = make(map[string]int64)
	}
	m.byPath[path] = estimatedBytes
	return path, estimatedBytes, FallbackNone, true
}

// releasePath returns a path's reservation to the pool. Unknown paths and
// nil managers are no-ops (idempotent: a second release finds nothing).
func (m *ArtifactStagingManager) releasePath(path string) {
	if m == nil || path == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byPath[path]
	if !ok || n <= 0 {
		return
	}
	delete(m.byPath, path)
	m.reserved -= n
	if m.reserved < 0 {
		m.reserved = 0
	}
}

// ReservedBytes reports the currently reserved tmpfs bytes (for metrics and
// diagnostics). Never negative.
func (m *ArtifactStagingManager) ReservedBytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reserved
}
