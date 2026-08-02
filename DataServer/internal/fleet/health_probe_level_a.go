package fleet

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// health_probe_level_a.go: ProbeLevelA — host health (SSH).
// Split out of health_probe.go; the package doc + shared types
// live in health_probe.go.
//
// Six independent sub-checks; cascade pattern: a single ssh_up
// failure means subsequent commands ALSO fail (the ssh
// connection itself is broken), but we still attempt them so
// the operator sees WHICH host checks are broken (not just
// that ssh is unreachable — could be network OR auth).
//
// Sub-checks:
//
//	ssh_up                  — `true` exits 0; confirms ssh daemon
//	                           reachable on the worker
//	cpu_load_1m             — /proc/loadavg column 1; pass if
//	                           < ThresholdLoad1Max
//	memory_available_mb    — `free -m | awk Mem: $7` (MemAvailable
//	                           on Linux ≥ 3.14); pass if >=
//	                           ThresholdMemAvailMin MB
//	disk_used_pct           — `df --output=pcent /var/lib/docker`
//	                           last row; pass if < ThresholdDiskUsedMax
//	docker_active           — `docker info --format {{.ServerVersion}}`
//	                           non-empty; confirms docker daemon
//	                           responsive on the worker
//	ntp_synced              — `timedatectl show -p NTPSynchronized
//	                           --value` reads "yes"; confirms clock
//	                           is in sync (avoids cert + lease expiry
//	                           bugs)
func ProbeLevelA(ctx context.Context, ssh BackendSSHClient, workerID string, now time.Time) HealthReport {
	start := now
	r := newReport(workerID, HealthLevelA, now)
	if ssh == nil {
		r.Checks["ssh_deps"] = CheckResult{Passed: false, Detail: "ssh client not wired (Step 11+ dependency)"}
		finalize(&r, start)
		return r
	}
	// ssh_up
	if _, err := ssh.Run(ctx, workerID, "true"); err != nil {
		r.Checks["ssh_up"] = CheckResult{Passed: false, Detail: "ssh unreachable: " + err.Error()}
	} else {
		r.Checks["ssh_up"] = CheckResult{Passed: true, Value: "ok"}
	}
	// cpu_load_1m — read /proc/loadavg directly (no shell needed)
	if out, err := ssh.Run(ctx, workerID, "awk '{print $1}' /proc/loadavg"); err == nil {
		if v, perr := strconv.ParseFloat(trim(out), 64); perr == nil {
			r.Checks["cpu_load_1m"] = CheckResult{
				Passed:   v < ThresholdLoad1Max,
				Value:    fmt.Sprintf("%.2f", v),
				Expected: fmt.Sprintf("<%.2f", ThresholdLoad1Max),
			}
		} else {
			r.Checks["cpu_load_1m"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["cpu_load_1m"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// memory_available_mb — MemAvailable column of free -m
	if out, err := ssh.Run(ctx, workerID, "free -m | awk '/^Mem:/ {print $7}'"); err == nil {
		if v, perr := strconv.Atoi(trim(out)); perr == nil {
			r.Checks["memory_available_mb"] = CheckResult{
				Passed:   v > ThresholdMemAvailMin,
				Value:    strconv.Itoa(v),
				Expected: fmt.Sprintf(">%d", ThresholdMemAvailMin),
			}
		} else {
			r.Checks["memory_available_mb"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["memory_available_mb"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// disk_used_pct — df Used% on /var/lib/docker (the worker's data root)
	if out, err := ssh.Run(ctx, workerID, "df --output=pcent /var/lib/docker | tail -1 | tr -d '% \\n'"); err == nil {
		if v, perr := strconv.Atoi(trim(out)); perr == nil {
			r.Checks["disk_used_pct"] = CheckResult{
				Passed:   v < ThresholdDiskUsedMax,
				Value:    strconv.Itoa(v),
				Expected: fmt.Sprintf("<%d", ThresholdDiskUsedMax),
			}
		} else {
			r.Checks["disk_used_pct"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["disk_used_pct"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// docker_active — docker info ServerVersion non-empty
	if out, err := ssh.Run(ctx, workerID, "docker info --format '{{.ServerVersion}}'"); err == nil {
		v := trim(out)
		r.Checks["docker_active"] = CheckResult{Passed: v != "", Value: v}
	} else {
		r.Checks["docker_active"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// ntp_synced — systemd-timesyncd.NTPSynchronized=yes
	if out, err := ssh.Run(ctx, workerID, "timedatectl show -p NTPSynchronized --value 2>/dev/null || echo false"); err == nil {
		v := trim(out)
		r.Checks["ntp_synced"] = CheckResult{Passed: v == "yes", Value: v}
	} else {
		r.Checks["ntp_synced"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	finalize(&r, start)
	return r
}
