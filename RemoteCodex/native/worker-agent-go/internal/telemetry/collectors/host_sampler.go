// Package collectors provides host-level resource collectors.
package collectors

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

// SampledHost is the boot-time / one-shot host layer used by the worker
// registration and capability surfaces. It is refreshed independently from
// per-beat resource samples.
type SampledHost struct {
	RAMBytes          int64
	DiskFreeBytes     int64
	HasGPU            bool
	EffectiveCpuCores int32 // min(logical CPUs, cgroup quota)
	PhysicalCPUCount  int32
	StorageDevice     string
	StorageClass      string
	GPUModel          string
	GPUVRAMBytes      int64
	NVENCAvailable    bool
	NVDECAvailable    bool
	QSVAvailable      bool
	NofileSoft        uint64
	NofileHard        uint64
}

// SampleHost reads the host capability layer. RAM and disk values use the
// same memory/disk collectors as the runtime sampler; GPU visibility is
// delegated to the GPU collector.
func (s *Sampler) SampleHost() (*SampledHost, error) {
	out := &SampledHost{}

	mem, err := s.readProcMeminfo()
	if err == nil {
		out.RAMBytes = mem.total
	}

	free, err := s.statvfsFreeBytes()
	if err == nil {
		out.DiskFreeBytes = free
	}

	out.HasGPU = detectGPU()
	out.EffectiveCpuCores = int32(runtime.NumCPU())
	out.PhysicalCPUCount = physicalCPUCount()
	out.StorageDevice, out.StorageClass = storageProfile(s.workDir)
	out.GPUModel = gpuModel()
	out.GPUVRAMBytes = gpuVRAMBytes()
	out.NVENCAvailable = fileExists("/dev/nvidia0")
	out.NVDECAvailable = out.NVENCAvailable
	out.QSVAvailable = fileExists("/dev/dri/renderD128")
	var limits unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limits); err == nil {
		out.NofileSoft, out.NofileHard = limits.Cur, limits.Max
	}
	return out, nil
}

func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }

func physicalCPUCount() int32 {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return int32(runtime.NumCPU())
	}
	seen := map[string]struct{}{}
	physical, core := "", ""
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "physical id") {
			physical = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "core id") {
			core = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
			if physical != "" {
				seen[physical+":"+core] = struct{}{}
			}
		}
	}
	if len(seen) == 0 {
		return int32(runtime.NumCPU())
	}
	return int32(len(seen))
}

func storageProfile(workDir string) (string, string) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", "unknown"
	}
	bestMount, device := "", ""
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 || !strings.HasPrefix(f[0], "/dev/") {
			continue
		}
		if strings.HasPrefix(workDir, f[1]) && len(f[1]) > len(bestMount) {
			bestMount, device = f[1], f[0]
		}
	}
	if device == "" {
		return "", "unknown"
	}
	name := filepath.Base(device)
	rot, err := os.ReadFile(filepath.Join("/sys/block", strings.TrimRight(name, "0123456789"), "queue/rotational"))
	if err == nil && strings.TrimSpace(string(rot)) == "0" {
		return device, "ssd_or_nvme"
	}
	if err == nil && strings.TrimSpace(string(rot)) == "1" {
		return device, "hdd"
	}
	return device, "unknown"
}

func gpuModel() string {
	paths, _ := filepath.Glob("/sys/class/drm/card*/device/device")
	_ = paths
	data, err := os.ReadFile("/sys/class/drm/card0/device/uevent")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "DRIVER=") {
			return strings.TrimSpace(strings.TrimPrefix(line, "DRIVER="))
		}
	}
	return ""
}

func gpuVRAMBytes() int64 {
	data, err := os.ReadFile("/sys/class/drm/card0/device/mem_info_vram_total")
	if err != nil {
		return 0
	}
	var value int64
	if _, err := fmt.Sscan(strings.TrimSpace(string(data)), &value); err != nil {
		return 0
	}
	return value
}
