package doctor

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"velox-worker-agent/pkg/config"
)

// DiskFreeValidator checks that the filesystem containing OutputDir has
// at least cfg.MinDiskFreeMB free space.
// RW-PROD-002 §2 item 6.
type DiskFreeValidator struct{}

func (v *DiskFreeValidator) ID() string { return "disk.free" }

func (v *DiskFreeValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	dir := cfg.OutputDir
	if dir == "" {
		dir = "/tmp/velox/scene-composite"
	}

	// Ensure directory exists so statfs has a valid path.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fail("disk.free",
			fmt.Sprintf("cannot create output dir %s: %v", dir, err),
			"ensure the parent filesystem is mounted and writable")
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return fail("disk.free",
			fmt.Sprintf("statfs(%s) failed: %v", dir, err),
			"check that the output directory exists on a mounted filesystem")
	}

	freeMiB := int64(stat.Bavail*uint64(stat.Bsize)) / (1024 * 1024)
	threshold := int64(cfg.MinDiskFreeMB)
	if threshold <= 0 {
		threshold = 256
	}

	if freeMiB < threshold {
		return fail("disk.free",
			fmt.Sprintf("disk free=%d MiB below threshold=%d MiB at %s", freeMiB, threshold, dir),
			"free up disk space or increase VELOX_MIN_DISK_FREE_MB")
	}

	return pass("disk.free", fmt.Sprintf("%d MiB free >= %d MiB threshold at %s", freeMiB, threshold, dir))
}
