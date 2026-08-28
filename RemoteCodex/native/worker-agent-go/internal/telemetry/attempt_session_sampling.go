package telemetry

// attempt_session_sampling.go owns cgroup detection, process-level CPU
// sampling via getrusage, the per-tick observe() method, and the
// background sampling loop that drives Start()'s goroutine.

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ── Process CPU via getrusage ─────────────────────────────────────────────

func sampleProcessCPU() processCPUUsage {
	var self, children syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &self); err != nil {
		return processCPUUsage{}
	}
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &children); err != nil {
		return processCPUUsage{}
	}
	return processCPUUsage{
		userUsec:   timevalUsec(self.Utime) + timevalUsec(children.Utime),
		systemUsec: timevalUsec(self.Stime) + timevalUsec(children.Stime),
		valid:      true,
	}
}

func timevalUsec(tv syscall.Timeval) int64 {
	return tv.Sec*1_000_000 + tv.Usec
}

// ── Sampler wrappers ──────────────────────────────────────────────────────

func (s *AttemptTelemetrySession) sampleResources(ctx context.Context) (*SampledResources, error) {
	if len(s.samplers) == 0 || s.samplers[0] == nil {
		return nil, nil
	}
	return s.samplers[0].Sample(ctx)
}

func (s *AttemptTelemetrySession) sampleCgroup() cgroupUsage {
	if s == nil || s.cgroupRoot == "" {
		return cgroupUsage{}
	}
	return readCgroupUsage(s.cgroupRoot)
}

// ── Cgroup v2 detection and reading ───────────────────────────────────────

func detectCgroupRoot() string {
	root := os.Getenv("VELOX_CGROUP_ROOT")
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	if _, err := os.Stat(filepath.Join(root, "cgroup.controllers")); err != nil {
		return ""
	}
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return root
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, "::", 2)
		if len(parts) == 2 {
			path := filepath.Join(root, parts[1])
			if _, err := os.Stat(filepath.Join(path, "cpu.stat")); err == nil {
				return path
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, "cpu.stat")); err == nil {
		return root
	}
	return ""
}

func readCgroupUsage(root string) cgroupUsage {
	var out cgroupUsage
	if data, err := os.ReadFile(filepath.Join(root, "cpu.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && f[0] == "usage_usec" {
				out.CPUUsec, _ = strconv.ParseInt(f[1], 10, 64)
			}
		}
	}
	readInt := func(name string) int64 {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return 0
		}
		n, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		return n
	}
	out.MemoryCurrent = readInt("memory.current")
	out.MemoryPeak = readInt("memory.peak")
	if data, err := os.ReadFile(filepath.Join(root, "io.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			for _, field := range strings.Fields(line) {
				p := strings.SplitN(field, "=", 2)
				if len(p) != 2 {
					continue
				}
				n, _ := strconv.ParseInt(p[1], 10, 64)
				if p[0] == "rbytes" {
					out.DiskReadBytes += n
				}
				if p[0] == "wbytes" {
					out.DiskWriteBytes += n
				}
			}
		}
	}
	return out
}

func cgroupIOAvailable(root string) bool {
	if root == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(root, "io.stat"))
	return err == nil
}

// ── Per-tick observation ──────────────────────────────────────────────────

// observe updates peak counters from one sampling tick.  It is called by
// the background goroutine in Start and by the final sample in Stop.
func (s *AttemptTelemetrySession) observe(r *SampledResources, c cgroupUsage, p processCPUUsage, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r != nil && s.lastResources != nil && !s.lastResources.SampledAt.IsZero() {
		elapsed := now.Sub(s.lastResources.SampledAt).Seconds()
		var deltaUsec int64
		validCPU := false
		if s.cgroupRoot != "" {
			deltaUsec = c.CPUUsec - s.lastCgroup.CPUUsec
			validCPU = deltaUsec >= 0
		} else if p.valid && s.lastProcess.valid {
			deltaUsec = p.totalUsec() - s.lastProcess.totalUsec()
			validCPU = deltaUsec >= 0
		}
		if elapsed > 0 && validCPU {
			cpu := float64(deltaUsec) / (elapsed * 1e6) * 100
			if cpu > s.peakCPUPercent {
				s.peakCPUPercent = cpu
			}
		}
	}
	// memory.peak is a cgroup lifetime high-water mark and is not
	// attributable to this attempt.  Track memory.current at each sample.
	if c.MemoryCurrent > s.peakRSSBytes {
		s.peakRSSBytes = c.MemoryCurrent
	}
	if r != nil {
		if r.ProcessRSSBytes > s.peakRSSBytes {
			s.peakRSSBytes = r.ProcessRSSBytes
		}
		s.peakOpenFDs = max64(s.peakOpenFDs, countOpenFDs())
		s.lastResources = r
	}
	s.lastCgroup = c
	s.lastProcess = p
	s.sampleCount++
}
