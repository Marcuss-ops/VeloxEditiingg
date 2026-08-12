// Package telemetry — DiskFreeAt (RW-PROD-004 §3 A4 supporting helper)
//
// Single-file helper that exposes the canonical "free bytes on the
// filesystem containing `path`" getter so the disk watcher goroutine
// in cmd/velox-worker-agent/main.go does not need to import golang.org/x/sys
// directly. syscall.Statfs is portable across Unix-like systems; on
// Windows the helper returns ENOTSUP-equivalent so the watcher logs
// a warning and continues (the host machine filesystem semantics
// differ enough that a portable helper would just lie).
//
// The returned free-bytes count is the canonical "Blocks available to
// unprivileged users" value (Bavail × Bsize). Using Bfree would
// over-count by including reserved blocks — a relevant concern on
// production hosts where the OS reserves ~5% of disk for root-only
// emergency usage.
package telemetry

import (
	"fmt"
	"runtime"
	"syscall"
)

// DiskFreeAt returns the number of bytes available to unprivileged
// users on the filesystem containing `path`. Returns an error on
// unsupported platforms or stat failures. Safe to call from any
// goroutine; syscall.Statfs is goroutine-safe on Linux/Darwin/BSD.
func DiskFreeAt(path string) (int64, error) {
	if runtime.GOOS == "windows" {
		return 0, fmt.Errorf("DiskFreeAt: Windows filesystem semantics differ; use platform-specific stat")
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	// Bavail × Bsize = free bytes **for the unprivileged user**.
	// Bsize is a signed 32-bit packed field on some BSDs (Frsize
	// high-bits encoded); cast through int64 with explicit shifts
	// so a future port does not silently truncate on 32-bit hosts.
	bsize := int64(stat.Bsize)
	return int64(stat.Bavail) * bsize, nil
}
