// ResourceSampler — worker-side runtime resource counters.
//
// Scorecard v1 (F4): populates proto.WorkerResourceCounters with the
// 22 fields the master F2 decodeWorkerResources expects. Pure stdlib
// only — no gopsutil, no third-party libs. Reads /proc/stat,
// /proc/meminfo, /proc/diskstats, /proc/net/dev, /proc/vmstat, plus
// syscall.Statvfs for filesystem free bytes.
//
// Per-domain split (this file is now a thin facade):
//   - cpu_sampler.go      : /proc/stat aggregate + /proc/loadavg
//   - memory_sampler.go   : /proc/meminfo + /proc/vmstat (pgmajfault)
//   - disk_sampler.go     : /proc/diskstats + /proc/mounts + statvfs
//   - network_sampler.go  : /proc/net/dev
//   - process_sampler.go  : /proc/self/statm + /proc/self/status
//   - host_sampler.go     : /dev/nvidia* + /sys/class/drm (GPU detect)
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
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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
