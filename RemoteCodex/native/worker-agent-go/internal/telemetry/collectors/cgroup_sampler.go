package collectors

// cgroup_sampler.go reads the scheduler pressure signals that explain why
// host throughput stops scaling: CFS throttling counters and PSI stall time.
// Missing cgroup/PSI files are normal in restricted containers and result in
// zero values without making the resource sampler fail.

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type cgroupPressure struct {
	NrThrottled   int64
	ThrottledUsec int64
	CPUSomeAvg10  float64
	IOSomeAvg10   float64
}

func readCgroupPressure(procRoot, sysRoot string) cgroupPressure {
	root := os.Getenv("VELOX_CGROUP_ROOT")
	if root == "" {
		root = filepath.Join(sysRoot, "fs", "cgroup")
	}
	if rel, err := os.ReadFile(filepath.Join(procRoot, "self", "cgroup")); err == nil {
		for _, line := range strings.Split(string(rel), "\n") {
			parts := strings.SplitN(line, "::", 2)
			if len(parts) == 2 {
				candidate := filepath.Join(root, parts[1])
				if _, err := os.Stat(filepath.Join(candidate, "cpu.stat")); err == nil {
					root = candidate
					break
				}
			}
		}
	}

	var out cgroupPressure
	if data, err := os.ReadFile(filepath.Join(root, "cpu.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			value, parseErr := strconv.ParseInt(fields[1], 10, 64)
			if parseErr != nil {
				continue
			}
			switch fields[0] {
			case "nr_throttled":
				out.NrThrottled = value
			case "throttled_usec":
				out.ThrottledUsec = value
			}
		}
	}
	out.CPUSomeAvg10 = readPSIAvg10(filepath.Join(root, "cpu.pressure"))
	out.IOSomeAvg10 = readPSIAvg10(filepath.Join(root, "io.pressure"))
	if out.CPUSomeAvg10 == 0 {
		out.CPUSomeAvg10 = readPSIAvg10(filepath.Join(procRoot, "pressure", "cpu"))
	}
	if out.IOSomeAvg10 == 0 {
		out.IOSomeAvg10 = readPSIAvg10(filepath.Join(procRoot, "pressure", "io"))
	}
	return out
}

func readPSIAvg10(path string) float64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, field := range fields[1:] {
			key, value, ok := strings.Cut(field, "=")
			if !ok || key != "avg10" {
				continue
			}
			parsed, parseErr := strconv.ParseFloat(value, 64)
			if parseErr == nil && parsed >= 0 {
				return parsed
			}
		}
	}
	return 0
}
