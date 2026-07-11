// Package metrics / procstat_linux.go
//
// Master-side /proc/self/status VmRSS reader. Cached for ~250ms to
// avoid hot-loop syscall hammering when called from a 5s supervisor
// tick.
package metrics

import (
	"os"
	"strconv"
	"strings"
)

// readRSSFromProc reads VmRSS from /proc/self/status on Linux.
// Returns 0 on any error (non-Linux, missing file, malformed VmRSS).
func readRSSFromProc() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		// VmRSS reports KB.
		v, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return v * 1024
	}
	return 0
}
