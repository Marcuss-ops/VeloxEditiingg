//go:build linux

package worker

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// preallocateFile reserves size bytes of real, non-sparse storage for f so
// concurrent chunk writes land at fixed offsets without incremental growth or
// fragmentation. It uses fallocate(2) on Linux; when the underlying
// filesystem does not support it (network mounts, some FUSE targets) it falls
// back to a sparse truncate, because physical pre-allocation is an
// optimization and must never fail the transfer. Non-Linux platforms always
// use the sparse truncate (preallocate_other.go).
func preallocateFile(f *os.File, size int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
		return f.Truncate(size)
	}
	return err
}
