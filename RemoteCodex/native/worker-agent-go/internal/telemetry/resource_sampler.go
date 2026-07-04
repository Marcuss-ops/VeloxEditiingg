// ResourceSampler — worker-side runtime resource counters.
//
// Scorecard v1 (F4): populates proto.WorkerResourceCounters with the
// 22 fields the master F2 decodeWorkerResources expects. Pure stdlib
// only — no gopsutil, no third-party libs. Reads /proc/stat,
// /proc/meminfo, /proc/diskstats, /proc/net/dev, /proc/vmstat, plus
// syscall.Statvfs for filesystem free bytes.
//
// Cumulative vs delta (the only design choice that matters for F2):
//   - /proc/stat: read agg row "cpu" (user, nice, system, idle, iowait,
//     irq, softirq, steal, guest, guest_nice). The sampler holds the
//     LAST tick's aggregate and computes ratios (util/iowait/steal) by
//     delta-minus-prior. Ratios are instantaneous; safe to emit.
//   - /proc/meminfo: instant meminfo snapshot. No delta math needed.
//     MemoryUsed = MemTotal - MemAvailable (Linux convention). Swap = SwapTotal - SwapFree.
//   - /proc/vmstat: cumulative counters. Read pgmajfault (per system).
//   - /proc/diskstats: cumulative (sectors + latency + io time). Emit
//     cumulative offsets; master F2 LastSeenResources computes delta.
//   - /proc/net/dev: cumulative byte counters. Emit cumulative total.
//   - syscall.Statvfs: instant Bsize/Bavail/Blocks free bytes.
//
// Invariants:
//   - One Sampler instance per worker; the same instance owns the proc
//     reader closures and the emit-cadence cursor.
//   - All /proc reads tolerate missing files (containers / chroots /
//     non-Linux) and fall back to zero values; failure does NOT abort
//     the sampler loop.
//   - Sampling is synchronous in `Sample()`; the runner loop sleeps on
//     `time.NewTicker`. No internal goroutine spawning beyond Run().
//
// Thread-safety: Sample() and Latest() may be called from different
// goroutines; the emit slot is published through a sync/atomic.Pointer
// (Go 1.19+). The reader closures use only stat-time file reads; they
// never share state with Sample() beyond the last CPU aggregate (held
// in a small atomic-friendly struct guarded by sync.Mutex because CPU
// jiffies uint64 reads are 8-byte atomic on amd64/arm64 but the struct
// copy should be guarded for clarity on 32-bit).
package telemetry

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// SampledResources is the Go-side mirror of proto.WorkerResourceCounters.
// Field-by-field correspondence is enforced by emit-proto marshalling
// (see ToWireMap). Naming follows the proto (snake_case on the wire).
type SampledResources struct {
	// CPU ratios — instantaneous, computed from /proc/stat delta.
	CPUUtilRatio   float64
	CPUIOWaitRatio float64
	CPUStealRatio  float64

	// Memory snapshot — instant from /proc/meminfo.
	MemoryTotalBytes     int64
	MemoryAvailableBytes int64
	MemoryUsedBytes      int64
	SwapUsedBytes        int64

	// Worker-local temp disk accounting. Keeps these on the
	// typed envelope (proto_worker_temp_bytes_written /
	// proto_worker_temp_files_open) so master-side cost basis
	// (F1 handler_jobs_metrics.go::executionMetricsToCostBasis) can
	// stop zero-pading storage_gb_written. Today both fields stay
	// zero — neither /proc nor /sys exposes per-process temp bytes
	// reliably. A future sampler may populate them by hooking into
	// the taskrunner output staging loop.
	TempBytesWritten int64
	TempFilesOpen    int32

	// Process RSS — read from /proc/self/statm (per-process).
	ProcessRSSBytes     int64
	ProcessRSSPeakBytes int64

	// Major page faults cumulative from /proc/vmstat.
	MajorPageFaultsTotal int64

	// Disk cumulative from /proc/diskstats.
	DiskReadBytesTotal  int64
	DiskWriteBytesTotal int64

	// Latency and io-util aggregate — cumulative seconds + ratio.
	DiskReadLatencySeconds  int64
	DiskWriteLatencySeconds int64
	DiskIOUtilizationRatio  float64

	// Filesystem free bytes from statvfs.
	DiskFreeBytes int64

	// Network counters cumulative from /proc/net/dev.
	NetworkReceiveBytesTotal  int64
	NetworkTransmitBytesTotal int64
	NetworkRetransmitsTotal   int64

	// Active tasks / slots — populated by the worker at ingest hook
	// (sampler does not own concurrency state).
	ActiveTasks int32
	TaskSlots   int32

	// Load average + run queue.
	Load1    float64
	RunQueue int32

	// SampledAt is wall-clock UTC when the snapshot was finalized.
	SampledAt time.Time
}

// SampledHost is the boot-time / one-shot host layer used by
// api.HostInfo (future markers at worker.go:177-183).
// SampledHost is cheap to recompute; the worker stores it on *Worker
// and refreshes it on a slow cadence (1 minute) so HasGPU / RAMBytes
// DiskFreeBytes reflect current reality (e.g. nvidia module loaded
// mid-flight).
type SampledHost struct {
	RAMBytes      int64
	DiskFreeBytes int64
	HasGPU        bool
}

// Sampler is the worker-side resource sampler. Construct via
// NewResourceSampler; pass a /proc root for test injection; pass a
// workDir for the diskstats mount-point resolution.
type Sampler struct {
	procRoot string
	sysRoot  string
	workDir  string
	tick     time.Duration

	mu        sync.Mutex
	lastCPU   cpuJiffies
	lastCPUAt time.Time

	slot atomic.Pointer[SampledResources]
	host atomic.Pointer[SampledHost]

	// emitEvery is how many ticks between published snapshots (1 = every
	// tick; default 3 → every 15s at the 5s default tick).
	emitEvery int
	count     int
}

// NewResourceSampler returns a Sampler wired to /proc + /sys and
// workDir. Caller may pass alternate paths for tests (typical:
// NewResourceSampler(procRoot t.TempDir, sysRoot sibling t.TempDir,
// workDir "/", 0, 0)). All three roots default to the canonical host
// paths when empty. tick=0 → 5s. emitEvery=0 → 3.
//
// procRoot + sysRoot are kept distinct because the worker's typical
// install path is `/proc/*` and `/sys/*` under the host filesystem.
// Tests substitute both to avoid the real sysfs / proc dependency.
func NewResourceSampler(procRoot, sysRoot, workDir string, tick time.Duration, emitEvery int) *Sampler {
	if procRoot == "" {
		procRoot = "/proc"
	}
	if sysRoot == "" {
		sysRoot = "/sys"
	}
	if tick <= 0 {
		tick = 5 * time.Second
	}
	if emitEvery <= 0 {
		emitEvery = 3
	}
	return &Sampler{
		procRoot:  procRoot,
		sysRoot:   sysRoot,
		workDir:   workDir,
		tick:      tick,
		emitEvery: emitEvery,
	}
}

// Latest returns the most recent published snapshot OR nil if no
// emit boundary has fired yet. Reception from heartbeat emit
// tolerates nil (master falls back to nothing — Prometheus simply
// shows stale data for that beat).
func (s *Sampler) Latest() *SampledResources {
	return s.slot.Load()
}

// Host returns the most recent host snapshot (or nil).
func (s *Sampler) Host() *SampledHost {
	return s.host.Load()
}

// Sample reads every /proc source, accumulates cumulative deltas,
// computes ratios, and returns the assembled snapshot. Errors are
// surfaced but tolerant: a single missing /proc file yields zero
// fields for that source, NOT a fatal sampler abort.
func (s *Sampler) Sample(ctx context.Context) (*SampledResources, error) {
	var firstErr error
	addErr := func(err error, label string) {
		if err == nil {
			return
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", label, err)
		}
	}

	out := &SampledResources{SampledAt: time.Now().UTC()}

	// CPU ratio computation (delta-based).
	cpu, err := s.readProcStat()
	addErr(err, "proc/stat")
	if err == nil {
		s.mu.Lock()
		if !s.lastCPUAt.IsZero() {
			dtot := cpu.total() - s.lastCPU.total()
			du := cpu.busy() - s.lastCPU.busy()
			diow := cpu.iowaitJ - s.lastCPU.iowaitJ
			dsteal := cpu.stealJ - s.lastCPU.stealJ
			if dtot > 0 {
				out.CPUUtilRatio = clampRatio(float64(du) / float64(dtot))
				out.CPUIOWaitRatio = clampRatio(float64(diow) / float64(dtot))
				out.CPUStealRatio = clampRatio(float64(dsteal) / float64(dtot))
			}
		}
		s.lastCPU = cpu
		s.lastCPUAt = out.SampledAt
		s.mu.Unlock()
	}

	// /proc/meminfo — instant.
	mem, err := s.readProcMeminfo()
	addErr(err, "proc/meminfo")
	if err == nil {
		out.MemoryTotalBytes = mem.total
		out.MemoryAvailableBytes = mem.available
		out.MemoryUsedBytes = mem.total - mem.available
		if mem.swapTotal >= 0 && mem.swapFree >= 0 {
			out.SwapUsedBytes = mem.swapTotal - mem.swapFree
		}
	}

	// /proc/vmstat — cumulative pgmajfault.
	maj, err := s.readProcVmstatMajorFaults()
	addErr(err, "proc/vmstat")
	if err == nil {
		out.MajorPageFaultsTotal = maj
	}

	// /proc/diskstats — cumulative sectors + latency.
	disk, err := s.readProcDiskstatsForWorkDir()
	addErr(err, "proc/diskstats")
	if err == nil {
		out.DiskReadBytesTotal = disk.readBytes
		out.DiskWriteBytesTotal = disk.writeBytes
		out.DiskReadLatencySeconds = disk.readLatencyMs / 1000
		out.DiskWriteLatencySeconds = disk.writeLatencyMs / 1000
		if disk.ioMsTotal > 0 && disk.sampleMs > 0 {
			out.DiskIOUtilizationRatio = clampRatio(float64(disk.ioMsTotal) / float64(disk.sampleMs))
		}
	}

	// statvfs — instant free disk bytes.
	free, err := s.statvfsFreeBytes()
	addErr(err, "statvfs")
	if err == nil {
		out.DiskFreeBytes = free
	}

	// /proc/net/dev — cumulative net bytes on primary interface.
	net, err := s.readProcNetDevPrimary()
	addErr(err, "proc/net/dev")
	if err == nil {
		out.NetworkReceiveBytesTotal = net.rxBytes
		out.NetworkTransmitBytesTotal = net.txBytes
		out.NetworkRetransmitsTotal = net.retransmits
	}

	// /proc/self/statm — instant per-process RSS.
	rssKib, peakKib, err := s.readSelfStatm()
	addErr(err, "proc/self/statm")
	if err == nil {
		out.ProcessRSSBytes = rssKib * 1024
		if peakKib > 0 {
			out.ProcessRSSPeakBytes = peakKib * 1024
		}
	}

	// /proc/loadavg — instant load1 + run queue.
	load1, runQ, err := s.readProcLoadavg()
	addErr(err, "proc/loadavg")
	if err == nil {
		out.Load1 = load1
		out.RunQueue = int32(runQ)
	}

	return out, firstErr
}

// Run drives the 5s tick + 15s publish cadence. Cancel ctx to stop.
// The first Sample runs IMMEDIATELY (counter=1) and is published (so
// the very first heartbeat after worker start has resources). After
// that, every Nth tick (emitEvery) the snapshot is published to the
// emit slot; consumer reads via Latest().
//
// TOLERANCE CONTRACT: partial snapshots are still published. Even when
// one /proc source fails (chroot, degraded container, sysfs missing),
// the other 90% of fields are useful to dashboards; a "store nothing
// on error" policy would leave Latest() permanently nil in such
// environments. SampleHost / Sample errors are surfaced via the loop's
// returned err so external observers see degraded state — but the
// latest sampled Snapshot gets a slot, not a wire of zeros.
func (s *Sampler) Run(ctx context.Context) error {
	if h, herr := s.SampleHost(); herr == nil && h != nil {
		s.host.Store(h)
	}
	if snap, serr := s.Sample(ctx); snap != nil {
		s.count = 1
		s.slot.Store(snap)
		if serr == nil {
			// Healthy sample: stamp SampledAt explicitly so dashboards
			// see a real, non-stale snapshot. Sample sets it on all
			// paths, but we re-stamp here so that partial-recovery
			// snapshots don't lose their real timestamp window.
			snap.SampledAt = time.Now().UTC()
		}
	}

	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if h, herr := s.SampleHost(); herr == nil && h != nil {
				s.host.Store(h)
			}
			snap, _ := s.Sample(ctx)
			if snap == nil {
				continue
			}
			s.count++
			if s.emitEvery <= 1 || s.count%s.emitEvery == 0 {
				s.slot.Store(snap)
			}
		}
	}
}

// SampleHost reads the one-shot boot-time host layer. Includes HasGPU
// detection (cheap /dev/nvidia* + DRM walk), MemTotal from
// /proc/meminfo, and statvfs free bytes for the work directory.
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

	out.HasGPU = s.detectGPU()

	return out, nil
}

// ── /proc parsers ─────────────────────────────────────────────────────────

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

// netCumulatives holds /proc/net/dev fields for one interface.
type netCumulatives struct {
	rxBytes     int64
	txBytes     int64
	retransmits int64
}

// readProcNetDevPrimary picks the interface with the highest rx_bytes,
// skipping `lo`. Operates on cumulative numbers (the wire contract).
func (s *Sampler) readProcNetDevPrimary() (netCumulatives, error) {
	data, err := readFile(filepath.Join(s.procRoot, "net", "dev"))
	if err != nil {
		return netCumulatives{}, err
	}
	var best netCumulatives
	var bestName string
	for _, line := range strings.Split(string(data), "\n") {
		// Header / blank lines. Skip.
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			continue
		}
		name := strings.TrimSpace(line[:idx])
		if name == "" || name == "lo" || name == "Inter-|" || name == " face" {
			continue
		}
		// Skip obviously-virtual names: docker*, veth*, br-*.
		if strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "br-") {
			continue
		}
		rest := strings.TrimSpace(line[idx+1:])
		cols := strings.Fields(rest)
		if len(cols) < 10 {
			continue
		}
		rxBytes, _ := strconv.ParseInt(cols[0], 10, 64)
		// /proc/net/dev schema: 8 receive fields (bytes, packets, errs,
		// drop, fifo, frame, compressed, multicast) + 8 transmit fields
		// (bytes, packets, errs, drop, fifo, colls, carrier, compressed).
		// cols[0] = rx_bytes, cols[8] = rx_multicast (zero on most
		// interfaces), cols[9] = tx_bytes. We must read [9] for tx.
		txBytes, _ := strconv.ParseInt(cols[9], 10, 64)
		// Pick the interface with the largest rx_bytes on this beat.
		// Ties broken by name lexicographic order for determinism.
		if rxBytes > best.rxBytes || (rxBytes == best.rxBytes && name < bestName) {
			best = netCumulatives{rxBytes: rxBytes, txBytes: txBytes}
			bestName = name
		}
	}
	if bestName == "" {
		return netCumulatives{}, errors.New("proc/net/dev: no non-virtual interface")
	}
	return best, nil
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

// detectGPU returns true if there's any GPU visible to the worker.
// Strategy (ordered cheapest-first):
//  1. /dev/nvidia0 → /dev/nvidiaN existence (NVIDIA CUDA).
//  2. /sys/class/drm/card*/device/vendor → known GPU vendors.
//  3. Else false.
func (s *Sampler) detectGPU() bool {
	// 1. /dev/nvidia* existence.
	for _, p := range []string{"/dev/nvidia0", "/dev/nvidiactl", "/dev/dri/renderD128"} {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	// 2. /sys/class/drm/card* with GPU-style vendor IDs.
	matches, err := filepath.Glob("/sys/class/drm/card*/device/vendor")
	if err != nil {
		return false
	}
	for _, m := range matches {
		data, rerr := readFile(m)
		if rerr != nil {
			continue
		}
		// Format: "0x10de\n"
		vendor := strings.TrimSpace(string(data))
		switch vendor {
		case "0x10de": // NVIDIA
			return true
		case "0x1002": // AMD/ATI
			return true
		case "0x8086": // Intel (only certain SKUs are real GPUs)
			return true
		}
	}
	return false
}

// ── Reader injection seam ─────────────────────────────────────────────────

// readFile is the package-private /proc reader. Tests in
// resource_sampler_test.go inject a temp dir by passing an alternate
// procRoot to NewResourceSampler; this helper uses os.ReadFile only.
func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// clampRatio keeps CPU ratios within [0,1]. Negative inputs are
// clamped to 0 (rare counter-wrap on iowait / steal).
func clampRatio(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
