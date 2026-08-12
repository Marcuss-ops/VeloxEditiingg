// Package telemetry / cpu_sampler.go
//
// CPU + load-average sampling split out of resource_sampler.go as part
// of the per-domain refactor (cpu / memory / disk / network / process /
// host + resource_sampler.go facade). All /proc parsers live next to
// their domain because they share the same lifetime: the cpu reader
// holds the LAST tick's aggregate in (*Sampler).lastCPU (owned by the
// facade) and computes instantaneous ratios by delta-minus-prior.
//
// The /proc/loadavg reader is grouped with CPU here because the
// load1 field is system-CPU pressure (1-minute run-queue depth), not
// per-process — it makes the file cohesive. The run-queue size is
// also CPU-pressure-derived (runnable+uninterruptible counts), so
// keeping it here matches the domain.
package telemetry

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// cpuJiffies mirrors the "cpu" aggregate row of /proc/stat. Fields
// are jiffies (kernel scheduling units) since boot.
type cpuJiffies struct {
	userJ, niceJ, systemJ, idleJ, iowaitJ, irqJ, softirqJ, stealJ uint64
}

func (c cpuJiffies) total() uint64 {
	return c.userJ + c.niceJ + c.systemJ + c.idleJ + c.iowaitJ + c.irqJ + c.softirqJ + c.stealJ
}

func (c cpuJiffies) busy() uint64 {
	return c.total() - c.idleJ - c.iowaitJ
}

// readProcStat reads /proc/stat aggregate row. Returns idle jiffies
// for delta math; defensive on missing file (containers w/o /proc).
func (s *Sampler) readProcStat() (cpuJiffies, error) {
	data, err := readFile(filepath.Join(s.procRoot, "stat"))
	if err != nil {
		return cpuJiffies{}, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		cols := strings.Fields(line)
		if len(cols) < 8 {
			return cpuJiffies{}, fmt.Errorf("proc/stat: too few columns (got %d)", len(cols))
		}
		// cpu + 7 jiffies → user, nice, system, idle, iowait, irq, softirq
		var c cpuJiffies
		var parseErr error
		c.userJ, parseErr = strconv.ParseUint(cols[1], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat user: %w", parseErr)
		}
		c.niceJ, parseErr = strconv.ParseUint(cols[2], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat nice: %w", parseErr)
		}
		c.systemJ, parseErr = strconv.ParseUint(cols[3], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat system: %w", parseErr)
		}
		c.idleJ, parseErr = strconv.ParseUint(cols[4], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat idle: %w", parseErr)
		}
		c.iowaitJ, parseErr = strconv.ParseUint(cols[5], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat iowait: %w", parseErr)
		}
		c.irqJ, parseErr = strconv.ParseUint(cols[6], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat irq: %w", parseErr)
		}
		c.softirqJ, parseErr = strconv.ParseUint(cols[7], 10, 64)
		if parseErr != nil {
			return cpuJiffies{}, fmt.Errorf("proc/stat softirq: %w", parseErr)
		}
		if len(cols) > 8 {
			c.stealJ, _ = strconv.ParseUint(cols[8], 10, 64)
		}
		return c, nil
	}
	return cpuJiffies{}, errors.New("proc/stat: cpu aggregate row missing")
}

// readProcLoadavg extracts 1-minute load average + total runnable
// processes from /proc/loadavg.
//
// /proc/loadavg: "0.50 0.40 0.30 1/123 4567"
//   - 0,1,2: load1, load5, load15
//   - 3: "running/total"
//   - 4: last PID
func (s *Sampler) readProcLoadavg() (load1 float64, runQueue int, err error) {
	data, err := readFile(filepath.Join(s.procRoot, "loadavg"))
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 4 {
		return 0, 0, errors.New("proc/loadavg: too few columns")
	}
	load1, _ = strconv.ParseFloat(fields[0], 64)
	totals := strings.Split(fields[3], "/")
	if len(totals) == 2 {
		runQueue, _ = strconv.Atoi(totals[0])
	}
	return load1, runQueue, nil
}
