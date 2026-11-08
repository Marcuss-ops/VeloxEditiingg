//go:build linux

package assets

import (
	"os"

	"golang.org/x/sys/unix"
)

// clonePreparedFile asks the filesystem for a copy-on-write clone. On
// filesystems without reflink support (or across devices) this returns an
// error and the caller falls back to a hard link or a streaming copy.
func clonePreparedFile(src, dst string) error {
	input, err := os.Open(src)
	if err != nil {
		return err
	}
	defer input.Close()
	staging, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := unix.IoctlFileClone(int(staging.Fd()), int(input.Fd())); err != nil {
		_ = staging.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := staging.Sync(); err != nil {
		_ = staging.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := staging.Close(); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}
