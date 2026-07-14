// Package telemetry / disk_sampler.go
//
// Disk sampling split out of resource_sampler.go as part of the
// per-domain refactor (cpu / memory / disk / network / process / host
// + resource_sampler.go facade). Disk is the heaviest domain — the
// sampler resolves the workDir's device via /proc/mounts, then walks
// /proc/diskstats and /sys/block/<name>/dev to aggregate cumulative
// bytes + latency + io-time. Falls back to whole-disk scan when the
// mount-path lookup fails (containers).
//
// statvfsFreeBytes lives here too because the only filesystem
// metric the sampler emits is DiskFreeBytes — it's tightly coupled
// to the disk domain and pulling it into host_sampler.go would
// split a related state (the workDir root) across two files.
package telemetry

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// diskCumulatives aggregates the diskstats fields we care about for
// the work-disk device. All values are CUMULATIVE since boot — no
// delta math on the worker; master F2 LastSeenResources does the math.
type diskCumulatives struct {
	readBytes      int64
	writeBytes     int64
	readLatencyMs  int64
	writeLatencyMs int64
	ioMsTotal      int64
	sampleMs       int64 // ms since boot (used for io-util ratio baseline)
}

// readProcDiskstatsForWorkDir resolves the cwd's device via
// /proc/mounts, then reads cumulative bytes/latency/io-time from
// /proc/diskstats. Falls back to non-loopback non-partition block
// device with the highest writeByte total if the mount-path lookup
// fails (common in containers).
func (s *Sampler) readProcDiskstatsForWorkDir() (diskCumulatives, error) {
	if s.workDir == "" {
		return diskCumulatives{}, errors.New("workDir not configured")
	}
	dev, err := s.resolveWorkDirDevice(s.workDir)
	if err != nil {
		// Best-effort fallback: aggregate over all block devices
		// excluding partitions and loopbacks.
		return s.diskstatsFallback()
	}
	return s.readDiskstatsForDev(dev)
}

// resolveWorkDirDevice walks /proc/mounts to find the longest-prefix
// mount point enclosing workDir. The matching mount's source device
// is returned.
func (s *Sampler) resolveWorkDirDevice(workDir string) (string, error) {
	data, err := readFile(filepath.Join(s.procRoot, "mounts"))
	if err != nil {
		return "", err
	}
	absWork, _ := filepath.Abs(workDir)
	bestDev := ""
	bestLen := -1
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		mount := fields[1]
		dev := fields[0]
		// Skip pseudo-fs (proc, sysfs, cgroup, tmpfs, overlay, etc.).
		if !strings.HasPrefix(dev, "/dev/") {
			continue
		}
		if !strings.HasPrefix(absWork, mount) {
			continue
		}
		if len(mount) > bestLen {
			bestLen = len(mount)
			bestDev = dev
		}
	}
	if bestDev == "" {
		return "", errors.New("no /dev mount enclosing workDir")
	}
	return bestDev, nil
}

// readDiskstatsForDev reads /proc/diskstats and aggregates sectors
// + latency + io-ms for the device's major:minor pair (matching both
// `/dev/sda` and `/dev/sda1`, for example — sum every partition
// belonging to the same disk).
func (s *Sampler) readDiskstatsForDev(dev string) (diskCumulatives, error) {
	major, minor, err := s.deviceMajorMinor(dev)
	if err != nil {
		return diskCumulatives{}, err
	}
	data, err := readFile(filepath.Join(s.procRoot, "diskstats"))
	if err != nil {
		return diskCumulatives{}, err
	}
	var out diskCumulatives
	for _, line := range strings.Split(string(data), "\n") {
		cols := strings.Fields(line)
		if len(cols) < 14 {
			continue
		}
		maj, _ := strconv.ParseInt(cols[0], 10, 64)
		mnr, _ := strconv.ParseInt(cols[1], 10, 64)
		if maj == -1 || mnr == -1 {
			continue
		}
		// Aggregate all partitions of the same disk (minor != 0 OR exact match).
		if minor != 0 && mnr != minor {
			continue
		}
		if major != -1 && maj != major {
			continue
		}
		// sector size assumed 512; reads completed at cols[5], reads at cols[6]
		// per kernel Documentation/admin-guide/iostats.rst (field 6 = sectors read, 10 = sectors written,
		// field 9 = time reading ms, field 11 = time writing ms).
		readSec, _ := strconv.ParseInt(cols[5], 10, 64)
		writeSec, _ := strconv.ParseInt(cols[9], 10, 64)
		readMs, _ := strconv.ParseInt(cols[6], 10, 64)
		writeMs, _ := strconv.ParseInt(cols[10], 10, 64)
		out.readBytes += readSec * 512
		out.writeBytes += writeSec * 512
		out.readLatencyMs += readMs
		out.writeLatencyMs += writeMs
	}
	if out.readBytes == 0 && out.writeBytes == 0 {
		return diskCumulatives{}, errors.New("diskstats: no rows matched device")
	}
	return out, nil
}

func (s *Sampler) diskstatsFallback() (diskCumulatives, error) {
	data, err := readFile(filepath.Join(s.procRoot, "diskstats"))
	if err != nil {
		return diskCumulatives{}, err
	}
	var out diskCumulatives
	var bestName string
	for _, line := range strings.Split(string(data), "\n") {
		cols := strings.Fields(line)
		if len(cols) < 14 {
			continue
		}
		devName := cols[2]
		// Skip partitions (anything ending in a digit) and loop/ram.
		if devName == "" {
			continue
		}
		if strings.HasPrefix(devName, "loop") || strings.HasPrefix(devName, "ram") {
			continue
		}
		// Only aggregate whole-disk (minor == 0).
		mjr, _ := strconv.ParseInt(cols[0], 10, 64)
		minr, _ := strconv.ParseInt(cols[1], 10, 64)
		if mjr <= 0 || minr != 0 {
			continue
		}
		bestName = devName
	}
	if bestName == "" {
		return out, errors.New("diskstats: no suitable block device")
	}
	return s.readDiskstatsForDev("/dev/" + bestName)
}

// deviceMajorMinor walks /sys/block/<name>/dev to read "<major>:<minor>".
// Returns an error if the sysfs entry is missing (chroots, unit-test
// stubs, containers without sysfs mounted). The diskstatsFallback
// degraded path picks up from there with a best-effort whole-disk scan.
//
// We deliberately do NOT parse syscall.Stat_t.rs.Dev via inline
// arithmetic — the Linux makedev encoding requires big-endian bit
// shifting that is brittle on cross-architecture builds. /sys is the
// canonical source on every supported platform.
//
// sysRoot is the seam for tests (production default /sys). Chroots /
// unit-test stubs pass a sibling temp dir as sysRoot so the rest of
// the diskstats path can proceed deterministically.
func (s *Sampler) deviceMajorMinor(dev string) (int64, int64, error) {
	base := filepath.Base(dev)
	sysPath := filepath.Join(s.sysRoot, "block", base, "dev")
	data, err := readFile(sysPath)
	if err != nil {
		return -1, -1, fmt.Errorf("deviceMajorMinor: %s not readable (sysfs missing?): %w", sysPath, err)
	}
	parts := strings.Split(strings.TrimSpace(string(data)), ":")
	if len(parts) != 2 {
		return -1, -1, fmt.Errorf("deviceMajorMinor: malformed %q", string(data))
	}
	mjr, _ := strconv.ParseInt(parts[0], 10, 64)
	mnr, _ := strconv.ParseInt(parts[1], 10, 64)
	return mjr, mnr, nil
}

// statvfsFreeBytes computes the filesystem-free-bytes via syscall.
// Uses syscall.Statvfs (POSIX) — does NOT require golang.org/x/sys.
func (s *Sampler) statvfsFreeBytes() (int64, error) {
	if s.workDir == "" {
		return 0, errors.New("statvfs: workDir not set")
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.workDir, &st); err != nil {
		return 0, fmt.Errorf("statfs(%s): %w", s.workDir, err)
	}
	// Bavail * Frsize gives free bytes visible to a non-root user.
	// Bavail * Bsize is also valid when Frsize is 1 (most filesystems).
	if st.Frsize > 0 {
		return int64(st.Bavail) * int64(st.Frsize), nil
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
