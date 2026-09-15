// Package collectors provides host-level resource collectors.
package collectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// SampledHost is the boot-time / one-shot host layer used by the worker
// registration and capability surfaces. It is refreshed independently from
// per-beat resource samples.
type SampledHost struct {
	RAMBytes                    int64
	DiskFreeBytes               int64
	HasGPU                      bool
	EffectiveCpuCores           int32 // min(logical CPUs, cgroup quota)
	PhysicalCPUCount            int32
	StorageDevice               string
	StorageClass                string
	GPUModel                    string
	GPUVRAMBytes                int64
	NVENCAvailable              bool
	NVDECAvailable              bool
	QSVAvailable                bool
	NofileSoft                  uint64
	NofileHard                  uint64
	CapacityBenchmarkStatus     string
	DiskReadBenchmarkMbps       float64
	DiskWriteBenchmarkMbps      float64
	DownloadBenchmarkMbps       float64
	UploadBenchmarkMbps         float64
	CapacityBenchmarkDurationMS int64
}

// CapacityBenchmarkConfig controls the opt-in bootstrap ceiling probe.
// NetworkURL must point at an operator-controlled endpoint; arbitrary public
// speed-test services are intentionally unsupported.
type CapacityBenchmarkConfig struct {
	Enabled    bool
	NetworkURL string
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

// PrimeHostWithBenchmark publishes the regular host snapshot and, when
// explicitly enabled, runs the one-shot capacity probe before Hello. A probe
// failure is represented as unavailable telemetry and never prevents worker
// startup; the caller can still see the exact status in the heartbeat.
func (s *Sampler) PrimeHostWithBenchmark(ctx context.Context, cfg CapacityBenchmarkConfig) error {
	h, err := s.SampleHost()
	if h == nil {
		h = &SampledHost{}
	}
	if !cfg.Enabled {
		h.CapacityBenchmarkStatus = "disabled"
		s.host.Store(h)
		return err
	}
	started := time.Now()
	result := runCapacityBenchmark(ctx, s.workDir, cfg.NetworkURL)
	h.CapacityBenchmarkStatus = result.Status
	h.DiskReadBenchmarkMbps = result.DiskReadMbps
	h.DiskWriteBenchmarkMbps = result.DiskWriteMbps
	h.DownloadBenchmarkMbps = result.DownloadMbps
	h.UploadBenchmarkMbps = result.UploadMbps
	h.CapacityBenchmarkDurationMS = time.Since(started).Milliseconds()
	s.host.Store(h)
	if err != nil {
		return err
	}
	return result.Err
}

type capacityBenchmarkResult struct {
	Status        string
	DiskReadMbps  float64
	DiskWriteMbps float64
	DownloadMbps  float64
	UploadMbps    float64
	Err           error
}

func runCapacityBenchmark(ctx context.Context, workDir, networkURL string) capacityBenchmarkResult {
	result := capacityBenchmarkResult{Status: "unavailable"}
	if _, err := exec.LookPath("fio"); err == nil {
		result.DiskReadMbps, result.DiskWriteMbps = fioCeilings(ctx, workDir)
	}
	if strings.TrimSpace(networkURL) != "" {
		result.DownloadMbps, result.UploadMbps = networkCeilings(ctx, networkURL)
	}
	measured := result.DiskReadMbps > 0 || result.DiskWriteMbps > 0 ||
		result.DownloadMbps > 0 || result.UploadMbps > 0
	allMeasured := result.DiskReadMbps > 0 && result.DiskWriteMbps > 0 &&
		result.DownloadMbps > 0 && result.UploadMbps > 0
	if allMeasured {
		result.Status = "measured"
	} else if measured {
		result.Status = "partial"
	}
	if result.DiskReadMbps == 0 && result.DiskWriteMbps == 0 &&
		result.DownloadMbps == 0 && result.UploadMbps == 0 {
		result.Err = fmt.Errorf("capacity benchmark unavailable: install fio and configure a controlled network endpoint")
	}
	return result
}

func fioCeilings(ctx context.Context, workDir string) (float64, float64) {
	if strings.TrimSpace(workDir) == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return 0, 0
	}
	path := filepath.Join(workDir, ".velox-capacity-probe")
	defer os.Remove(path)
	read := runFio(ctx, path, "read")
	write := runFio(ctx, path, "write")
	return read, write
}

func runFio(ctx context.Context, path, mode string) float64 {
	command := exec.CommandContext(ctx, "fio", "--name=velox-capacity", "--filename="+path,
		"--size=64m", "--bs=1m", "--iodepth=16", "--direct=1", "--rw="+mode,
		"--runtime=2", "--time_based", "--output-format=json")
	output, err := command.Output()
	if err != nil {
		return 0
	}
	var report struct {
		Jobs []struct {
			Read struct {
				BWBytes float64 `json:"bw_bytes"`
			} `json:"read"`
			Write struct {
				BWBytes float64 `json:"bw_bytes"`
			} `json:"write"`
		} `json:"jobs"`
	}
	if json.Unmarshal(output, &report) != nil || len(report.Jobs) == 0 {
		return 0
	}
	bw := report.Jobs[0].Read.BWBytes
	if mode == "write" {
		bw = report.Jobs[0].Write.BWBytes
	}
	return bw / (1024 * 1024)
}

func networkCeilings(ctx context.Context, endpoint string) (float64, float64) {
	client := &http.Client{Timeout: 5 * time.Second}
	probeURL, err := url.Parse(endpoint)
	if err != nil || probeURL.Scheme == "" || probeURL.Host == "" {
		return 0, 0
	}
	query := probeURL.Query()
	query.Set("bytes", "8388608")
	probeURL.RawQuery = query.Encode()
	start := time.Now()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
	if err != nil {
		return 0, 0
	}
	resp, err := client.Do(request)
	if err != nil {
		return 0, 0
	}
	downBytes, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, 16<<20))
	resp.Body.Close()
	down := mbps(float64(downBytes), time.Since(start))

	payload := bytes.NewReader(make([]byte, 1<<20))
	start = time.Now()
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, payload)
	if err != nil {
		return down, 0
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	resp, err = client.Do(request)
	if err != nil {
		return down, 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return down, mbps(float64(payload.Size()), time.Since(start))
}

func mbps(bytes float64, elapsed time.Duration) float64 {
	if bytes <= 0 || elapsed <= 0 {
		return 0
	}
	return bytes * 8 / elapsed.Seconds() / 1_000_000
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
