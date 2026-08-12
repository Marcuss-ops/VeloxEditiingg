// Package telemetry / memory_sampler.go
//
// Memory sampling split out of resource_sampler.go as part of the
// per-domain refactor (cpu / memory / disk / network / process / host
// + resource_sampler.go facade). The /proc/meminfo reader is the
// authoritative source for MemoryTotalBytes / MemoryAvailableBytes /
// MemoryUsedBytes / SwapUsedBytes — the latter two are derived
// (MemTotal-MemAvailable, SwapTotal-SwapFree) in the facade's Sample
// orchestrator.
//
// /proc/vmstat is grouped here because the only field the sampler
// reads (pgmajfault) is a per-system memory-pressure counter, not
// disk or network. The reader tolerates missing swap entries (some
// kernels omit them) by setting the swapTotal/swapFree sentinels
// to -1 — the facade uses that sentinel to skip SwapUsedBytes
// population rather than reporting zero.
package telemetry

import (
	"bufio"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

// meminfoSnapshot holds the parseable subset.
type meminfoSnapshot struct {
	total     int64
	available int64
	swapTotal int64
	swapFree  int64
}

func (s *Sampler) readProcMeminfo() (meminfoSnapshot, error) {
	data, err := readFile(filepath.Join(s.procRoot, "meminfo"))
	if err != nil {
		return meminfoSnapshot{}, err
	}
	var out meminfoSnapshot
	out.swapTotal = -1 // sentinel: file may not have swap entries on some kernels
	out.swapFree = -1
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := sc.Text()
		// Lines: "MemTotal:       16384000 kB"
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		rest := strings.TrimSpace(line[idx+1:])
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		valKB, _ := strconv.ParseInt(fields[0], 10, 64)
		valBytes := valKB * 1024
		switch key {
		case "MemTotal":
			out.total = valBytes
		case "MemAvailable":
			out.available = valBytes
		case "SwapTotal":
			out.swapTotal = valBytes
		case "SwapFree":
			out.swapFree = valBytes
		}
	}
	if out.total <= 0 {
		return out, errors.New("proc/meminfo: MemTotal missing")
	}
	if out.available < 0 {
		// MemAvailable missing on kernels < 3.14: fall back to MemFree.
		// Skip the free lookup here; we already accepted "no Available"
		// without aborting — caller zero-fills MemoryUsedBytes.
		out.available = 0
	}
	return out, nil
}

func (s *Sampler) readProcVmstatMajorFaults() (int64, error) {
	data, err := readFile(filepath.Join(s.procRoot, "vmstat"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "pgmajfault" {
			return strconv.ParseInt(fields[1], 10, 64)
		}
	}
	return 0, errors.New("proc/vmstat: pgmajfault not found")
}
