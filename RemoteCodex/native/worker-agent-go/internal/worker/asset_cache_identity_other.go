//go:build !linux

package worker

import "os"

func statIdentity(os.FileInfo) (dev, ino uint64, ctimeNS int64, ok bool) {
	return 0, 0, 0, false
}
