//go:build linux

package worker

import (
	"os"
	"syscall"
)

func statIdentity(info os.FileInfo) (dev, ino uint64, ctimeNS int64, ok bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, false
	}
	return uint64(st.Dev), uint64(st.Ino), st.Ctim.Nano(), true
}
