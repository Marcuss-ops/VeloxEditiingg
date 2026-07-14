// Package telemetry / process_sampler.go
//
// Process-level sampling split out of resource_sampler.go as part of
// the per-domain refactor (cpu / memory / disk / network / process /
// host + resource_sampler.go facade). The /proc/self/statm reader
// reports the current process's RSS in pages (col 1) and reads
// /proc/self/status for VmHWM (peak RSS) in a second best-effort
// pass.
//
// RSS is in pages of 4KiB on Linux. The facade converts pages to
// bytes (rssPages * 1024 / 4096 = pages) for the wire envelope.
package telemetry

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

// readSelfStatm reads /proc/self/statm to extract RSS bytes (col 1) and
// peak RSS in pages. RSS is in pages of 4KiB on Linux.
//
// /proc/self/statm layout:
//
//	0 size, 1 resident, 2 shared, 3 text, 4 lib, 5 data, 6 dt
func (s *Sampler) readSelfStatm() (rssPages, peakPages int64, err error) {
	data, err := readFile(filepath.Join(s.procRoot, "self", "statm"))
	if err != nil {
		return 0, 0, err
	}
	cols := strings.Fields(string(data))
	if len(cols) < 2 {
		return 0, 0, errors.New("self/statm: too few columns")
	}
	rssPages, _ = strconv.ParseInt(cols[1], 10, 64)
	// peak RSS is on /proc/self/status (VmHWM), separate read; keep
	// it best-effort and graceful.
	statusData, sErr := readFile(filepath.Join(s.procRoot, "self", "status"))
	if sErr == nil {
		for _, ln := range strings.Split(string(statusData), "\n") {
			if strings.HasPrefix(ln, "VmHWM:") {
				fields := strings.Fields(ln)
				if len(fields) >= 2 {
					kb, _ := strconv.ParseInt(fields[1], 10, 64)
					if kb > 0 {
						peakPages = kb * 1024 / 4096 // convert kB → pages
					}
				}
				break
			}
		}
	}
	return rssPages, peakPages, nil
}
