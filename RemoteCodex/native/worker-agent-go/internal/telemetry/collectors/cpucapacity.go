package telemetry

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// CPUCapacity describes the CPU environment of the worker host.
// It is reported once per attempt via TypedExecutionMetrics so the
// master can compute accurate oversubscription ratios.
type CPUCapacity struct {
	LogicalCPUCount   int
	CPUQuota          float64
	EffectiveCPUCount int
}

var (
	cpuCapacityOnce     sync.Once
	cpuCapacityDetected CPUCapacity
)

// DetectCPUCapacity returns the logical CPU count, the cgroup CPU quota
// expressed in cores, and the effective number of CPUs the worker may
// use (logical count capped by the quota). Results are cached for the
// lifetime of the process.
func DetectCPUCapacity() CPUCapacity {
	cpuCapacityOnce.Do(func() {
		cpuCapacityDetected = detectCPUCapacityOnce()
	})
	return cpuCapacityDetected
}

func detectCPUCapacityOnce() CPUCapacity {
	logical := runtime.NumCPU()
	if logical <= 0 {
		logical = 1
	}

	quota := readCPUQuota(logical)

	effective := logical
	if quota > 0 && quota < float64(logical) {
		effective = int(math.Ceil(quota))
		if effective < 1 {
			effective = 1
		}
	}

	return CPUCapacity{
		LogicalCPUCount:   logical,
		CPUQuota:          quota,
		EffectiveCPUCount: effective,
	}
}

// readCPUQuota returns the cgroup CPU quota in cores, or logical if no
// quota is enforced. Supports cgroup v2 (cpu.max) and cgroup v1
// (cpu.cfs_quota_us / cpu.cfs_period_us).
func readCPUQuota(logical int) float64 {
	if q := readCgroupV2Quota(); q >= 0 {
		return q
	}
	if q := readCgroupV1Quota(); q >= 0 {
		return q
	}
	return float64(logical)
}

// readCgroupV2Quota parses /sys/fs/cgroup/cpu.max. Returns -1 if the
// file is missing or the quota is "max" (unlimited).
func readCgroupV2Quota() float64 {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return -1
	}
	parts := strings.Fields(string(data))
	if len(parts) < 2 {
		return -1
	}
	if parts[0] == "max" {
		return -1
	}
	quota, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || quota <= 0 {
		return -1
	}
	period, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || period <= 0 {
		return -1
	}
	return float64(quota) / float64(period)
}

// readCgroupV1Quota parses /sys/fs/cgroup/cpu/cpu.cfs_quota_us and
// cpu.cfs_period_us. Returns -1 if the files are missing or the quota
// is -1 (unlimited).
func readCgroupV1Quota() float64 {
	quotaData, err := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	if err != nil {
		return -1
	}
	quotaStr := strings.TrimSpace(string(quotaData))
	quota, err := strconv.ParseInt(quotaStr, 10, 64)
	if err != nil || quota <= 0 {
		return -1
	}

	periodData, err := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err != nil {
		return -1
	}
	periodStr := strings.TrimSpace(string(periodData))
	period, err := strconv.ParseInt(periodStr, 10, 64)
	if err != nil || period <= 0 {
		return -1
	}

	return float64(quota) / float64(period)
}
