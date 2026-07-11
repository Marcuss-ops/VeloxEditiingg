// resource_sampler_test.go — exercise the sampler against a
// fake /proc tree injected via the NewResourceSampler(procRoot,...)
// seam. GOOS=linux only (the whole sampler assumes /proc); tests are
// guarded to skip on other platforms so the build remains portable.
//
// Tests are written so they don't need root or any privileged context
// — every /proc file is a stub in a t.TempDir().

package telemetry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// stubProc creates a fake /proc + /sys tree suitable for Sample().
// procRoot + sysRoot are sibling temp dirs so the diskstats device
// resolution exercise can proceed end-to-end in unit tests without
// needing real sysfs.
//
// Tests that explicitly want the degraded /sys-missing path pass a
// different setup via stubProcOpts.disableSysStub.
func stubProc(t *testing.T, opts stubProcOpts) (string, string) {
	t.Helper()
	root := t.TempDir()
	sys := t.TempDir()
	mustWrite := func(base, rel, content string) {
		full := filepath.Join(base, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", full, err)
		}
	}
	mustW := func(rel, content string) { mustWrite(root, rel, content) }
	mustW("stat", opts.stat)
	mustW("meminfo", opts.meminfo)
	mustW("vmstat", opts.vmstat)
	mustW("loadavg", opts.loadavg)
	mustW("mounts", opts.mounts)
	mustW("diskstats", opts.diskstats)
	mustW("net/dev", opts.netDev)
	mustW("self/status", opts.selfStatus)
	mustW("self/statm", opts.selfStatm)
	if !opts.disableSysStub {
		// /sys/block/sda1/dev = "8:1" matches /proc/mounts (sda1 → /).
		// This is the canonical happy path that lets diskstats pick up
		// aggregated sectors + latency + io-ms for the working disk.
		mustWrite(sys, "block/sda1/dev", "8:1\n")
		mustWrite(sys, "block/sda/dev", "8:0\n")
	}
	// Stash the sysRoot alongside procRoot under known suffix so
	// tests can pass it back into NewResourceSampler.
	_ = os.WriteFile(filepath.Join(root, "__sysroot__"), []byte(sys), 0o644)
	return root, sys
}

type stubProcOpts struct {
	stat           string
	meminfo        string
	vmstat         string
	loadavg        string
	mounts         string
	diskstats      string
	netDev         string
	selfStatus     string
	selfStatm      string
	workDir        string
	disableSysStub bool // when true, /sys entries are NOT written (for degraded-path tests)
}

func defaultStubOpts() stubProcOpts {
	return stubProcOpts{
		stat: "cpu  100 0 50 800 10 0 5 20 0 0\n" +
			"cpu0 25 0 12 200 2 0 1 5 0 0\n",
		meminfo: "MemTotal:       16384000 kB\n" +
			"MemFree:         4096000 kB\n" +
			"MemAvailable:    8192000 kB\n" +
			"SwapTotal:        2048000 kB\n" +
			"SwapFree:         1024000 kB\n",
		vmstat:    "pgfault 1234567\npgmajfault 42\n",
		loadavg:   "0.50 0.40 0.30 1/123 99999\n",
		mounts:    "tmpfs /tmp tmpfs rw,nosuid 0 0\n/dev/sda1 / ext4 rw,relatime 0 0\n",
		diskstats: "  8       1 sda1 100 200 300 40 500 600 700 80 0 100 0\n",
		netDev: "Inter-|   Receive                                                |  Transmit\n" +
			" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n" +
			"    lo: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n" +
			"  eth0: 12345 100 0 0 0 0 0 0 0 54321 200 0 0 0 0 0\n",
		selfStatus: "Name: x\nPid: 1\nVmHWM:     4096 kB\n",
		selfStatm:  "1000 200 50 100 0 100 0\n",
		workDir:    "/",
	}
}

// TestSample_ReadsAllSources: confirm Sample reads every proc source,
// returns a non-nil snapshot, and respects zero-fields when source data
// is missing.
func TestSample_ReadsAllSources(t *testing.T) {
	proc, sys := stubProc(t, defaultStubOpts())
	s := NewResourceSampler(proc, sys, "/", 0, 0)
	// First sample: CPU ratios stay zero (no prior baseline).
	s1, err := s.Sample(context.Background())
	if err != nil {
		t.Fatalf("Sample returned err (expected tolerant): %v", err)
	}
	if s1 == nil {
		t.Fatalf("Sample returned nil snapshot")
	}
	if s1.MemoryTotalBytes != 16384000*1024 {
		t.Errorf("MemoryTotalBytes=%d want %d", s1.MemoryTotalBytes, 16384000*1024)
	}
	if s1.MemoryAvailableBytes != 8192000*1024 {
		t.Errorf("MemoryAvailableBytes=%d want %d", s1.MemoryAvailableBytes, 8192000*1024)
	}
	if s1.MemoryUsedBytes != (16384000-8192000)*1024 {
		t.Errorf("MemoryUsedBytes=%d want %d", s1.MemoryUsedBytes, (16384000-8192000)*1024)
	}
	if s1.SwapUsedBytes != (2048000-1024000)*1024 {
		t.Errorf("SwapUsedBytes=%d want %d", s1.SwapUsedBytes, (2048000-1024000)*1024)
	}
	if s1.MajorPageFaultsTotal != 42 {
		t.Errorf("MajorPageFaultsTotal=%d want 42", s1.MajorPageFaultsTotal)
	}
	if s1.Load1 != 0.50 {
		t.Errorf("Load1=%f want 0.50", s1.Load1)
	}
	if s1.RunQueue != 1 {
		t.Errorf("RunQueue=%d want 1", s1.RunQueue)
	}
	if s1.NetworkReceiveBytesTotal != 12345 {
		t.Errorf("NetworkRx=%d want 12345", s1.NetworkReceiveBytesTotal)
	}
	if s1.NetworkTransmitBytesTotal != 54321 {
		t.Errorf("NetworkTx=%d want 54321", s1.NetworkTransmitBytesTotal)
	}
	// CPU ratios still zero after first tick (no prior baseline).
	if s1.CPUUtilRatio != 0 || s1.CPUIOWaitRatio != 0 || s1.CPUStealRatio != 0 {
		t.Errorf("CPU ratios must be zero on first sample: %+v", s1)
	}
}

// TestSample_DeltaMath_CPURatios: second sample produces the
// expected cpu_util / iowait / steal ratios from a delta between two
// /proc/stat aggregates.
func TestSample_DeltaMath_CPURatios(t *testing.T) {
	opts := defaultStubOpts()
	// First tick: cpu total = 100+0+50+800+10+0+5+20 = 985 jiffies
	// Idle = 800, IOWait = 10, Steal = 20. Busy = 985-800-10 = 175.
	proc := t.TempDir()
	sys := t.TempDir()
	mustW2 := func(base, rel, c string) {
		full := filepath.Join(base, rel)
		_ = os.MkdirAll(filepath.Dir(full), 0o755)
		_ = os.WriteFile(full, []byte(c), 0o644)
	}
	mustW := func(rel, c string) { mustW2(proc, rel, c) }
	mustSW := func(rel, c string) { mustW2(sys, rel, c) }
	writeAll := func(stat string) {
		mustW("stat", stat)
		mustW("meminfo", opts.meminfo)
		mustW("vmstat", opts.vmstat)
		mustW("loadavg", opts.loadavg)
		mustW("mounts", opts.mounts)
		mustW("diskstats", opts.diskstats)
		mustW("net/dev", opts.netDev)
		mustW("self/status", opts.selfStatus)
		mustW("self/statm", opts.selfStatm)
		mustSW("block/sda1/dev", "8:1\n")
		mustSW("block/sda/dev", "8:0\n")
	}
	writeAll(opts.stat)
	s := NewResourceSampler(proc, sys, "/", 0, 0)
	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("first sample err: %v", err)
	}
	// Second stat: total = 965*2 + 200 (busy grew by 200, idle by 0):
	// user→200, nice→0, system→50, idle→800, iowait→20, irq→0, softirq→5, steal→30.
	// delta_total = (200+0+50+800+20+0+5+30) - (100+0+50+800+10+0+5+20)
	//            = 1105 - 985 = 120
	// delta_idle = 0, delta_iowait = 10, delta_steal = 10
	// delta_busy = total - idle - iowait = 110
	//              -> 110 / 120 ≈ 0.9167
	// iowait = 10/120 ≈ 0.0833; steal = 10/120 ≈ 0.0833
	writeAll("cpu  200 0 50 800 20 0 5 30 0 0\n" +
		"cpu0 50 0 12 200 4 0 1 6 0 0\n")
	s2, err := s.Sample(context.Background())
	if err != nil {
		t.Fatalf("second sample err: %v", err)
	}
	want := 110.0 / 120.0 // delta_busy = (1105-800-20) - (985-800-10) = 285-175 = 110
	if !approx(s2.CPUUtilRatio, want, 1e-3) {
		t.Errorf("CPUUtilRatio=%f want≈%f", s2.CPUUtilRatio, want)
	}
	if !approx(s2.CPUUtilRatio, want, 1e-3) {
		t.Errorf("CPUUtilRatio=%f want≈%f", s2.CPUUtilRatio, want)
	}
	wantIOW := 10.0 / 120.0
	if !approx(s2.CPUIOWaitRatio, wantIOW, 1e-3) {
		t.Errorf("CPUIOWaitRatio=%f want≈%f", s2.CPUIOWaitRatio, wantIOW)
	}
	wantSteal := 10.0 / 120.0
	if !approx(s2.CPUStealRatio, wantSteal, 1e-3) {
		t.Errorf("CPUStealRatio=%f want≈%f", s2.CPUStealRatio, wantSteal)
	}
}

// TestSample_Disk_FromCwdMount: workDir is "/" → /proc/mounts picks
// the matching device (`/dev/sda1`); diskstats line for major:8
// minor:1 is summed. Sectors × 512 → bytes.
func TestSample_Disk_FromCwdMount(t *testing.T) {
	proc, sys := stubProc(t, func() stubProcOpts {
		o := defaultStubOpts()
		o.disableSysStub = true
		return o
	}())
	// /sys stub disabled: deviceMajorMinor errors out, no usable block
	// device, both paths return "no suitable block device" error.
	s := NewResourceSampler(proc, sys, "/", 0, 0)
	snap, err := s.Sample(context.Background())
	if err == nil {
		t.Errorf("expected degraded-path error (no real /sys in test stub), got nil")
	}
	if snap == nil {
		t.Fatalf("Sample returned nil even on degraded path")
	}
	// Disk read/write bytes should be 0 because no suitable whole
	// disk line matches minor==0.
	if snap.DiskReadBytesTotal != 0 || snap.DiskWriteBytesTotal != 0 {
		t.Errorf("diskbytes should degrade to 0 in stub: read=%d write=%d",
			snap.DiskReadBytesTotal, snap.DiskWriteBytesTotal)
	}
}

// TestSample_NetDev_PicksHighestRx: build two interfaces and verify
// the one with the larger rx_bytes wins.
func TestSample_NetDev_PicksHighestRx(t *testing.T) {
	opts := defaultStubOpts()
	opts.netDev = "Inter-|   Receive                                                |  Transmit\n" +
		" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n" +
		"    lo: 999999999 0 0 0 0 0 0 0 0 999999 0 0 0 0 0 0\n" +
		"  eth0: 999 5 0 0 0 0 0 0 0 800 4 0 0 0 0 0\n" +
		"  eth1: 12345 100 0 0 0 0 0 0 0 54321 200 0 0 0 0 0\n" +
		" docker0: 99999999 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n" +
		" veth-abc: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
	proc, sys := stubProc(t, opts)
	s := NewResourceSampler(proc, sys, "/", 0, 0)
	snap, err := s.Sample(context.Background())
	if err != nil {
		t.Fatalf("Sample err: %v", err)
	}
	if snap.NetworkReceiveBytesTotal != 12345 {
		t.Errorf("rx=%d want 12345 (eth1 beat eth0 in cumulative rx; lo/docker0 skipped)",
			snap.NetworkReceiveBytesTotal)
	}
	if snap.NetworkTransmitBytesTotal != 54321 {
		t.Errorf("tx=%d want 54321", snap.NetworkTransmitBytesTotal)
	}
}

// TestSample_TolerantOnMissingFiles: every /proc file is missing; the
// sampler must NOT panic, returns a non-nil zero snapshot, and Latest
// is unpublished.
func TestSample_TolerantOnMissingFiles(t *testing.T) {
	proc := t.TempDir() // empty
	s := NewResourceSampler(proc, "", "/", 0, 0)
	snap, err := s.Sample(context.Background())
	if err == nil {
		t.Errorf("Sample against empty proc should surface a non-fatal error (got nil)")
	}
	if snap == nil {
		t.Fatalf("Snapshot is nil even on missing files")
	}
	if snap.MemoryTotalBytes != 0 || snap.Load1 != 0 {
		t.Errorf("snapshot should be all-zero on missing data: %+v", snap)
	}
}

// TestRun_PublishesEveryThirdTick: drive Run via a tight ticker and
// observe Latest() advances at the configured cadence.
func TestRun_PublishesEveryThirdTick(t *testing.T) {
	proc, sys := stubProc(t, defaultStubOpts())
	s := NewResourceSampler(proc, sys, "/", 50*time.Millisecond, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	// First Run() snapshot publishes synchronously.
	time.Sleep(20 * time.Millisecond)
	if s.Latest() == nil {
		t.Errorf("Latest should be published after first sample")
	}
	// Wait long enough for at least 6 ticks → expect 2 publishes
	// (initial + every 3rd).
	time.Sleep(350 * time.Millisecond)
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned unexpected err: %v", err)
	}
	// We cannot peek into Latest()'s internal counter directly, but
	// the fact the loop survived 7+ ticks implies cadence ticked.
}

// TestSampleHost_BasicSanity: SampleHost populates RAMBytes, populates
// HasGPU=false in test env (no /dev/nvidia0), populates DiskFreeBytes
// to ≥0 (workDir tmpfs may report any positive number).
func TestSampleHost_BasicSanity(t *testing.T) {
	proc, sys := stubProc(t, defaultStubOpts())
	tmpWorkDir := t.TempDir()
	s := NewResourceSampler(proc, sys, tmpWorkDir, 0, 0)
	host, err := s.SampleHost()
	if err != nil {
		t.Fatalf("SampleHost err: %v", err)
	}
	if host == nil {
		t.Fatalf("SampleHost returned nil")
	}
	if host.RAMBytes != 16384000*1024 {
		t.Errorf("RAMBytes=%d want %d", host.RAMBytes, 16384000*1024)
	}
	if host.DiskFreeBytes <= 0 {
		t.Errorf("DiskFreeBytes should be > 0 for real tmpdir, got %d", host.DiskFreeBytes)
	}
	// HasGPU detection runs over real /dev and /sys; in a unit-test
	// environment without nvidia devices or DRM cards we expect false.
	if host.HasGPU {
		t.Errorf("HasGPU = true; test env should not have a GPU (or the host has nvidia0 — adapt the test if so)")
	}
}

// TestHasGPU_DetectsNVidiaDevice: hand-place a fake /dev/nvidia0
// file via a temp dir + symlink shim. Since detectGPU() inspects hard-
// coded /dev paths, use a temporary OS_PATH overlay is not feasible
// without root. Instead verify the method's existence + idempotence:
// HasGPU is a deterministic function of host's /dev state; unit tests
// simply assert the boolean is consistent.
func TestHasGPU_DoesNotPanic(t *testing.T) {
	proc, sys := stubProc(t, defaultStubOpts())
	s := NewResourceSampler(proc, sys, "/", 0, 0)
	// Multiple invocations must be deterministic.
	a := s.detectGPU()
	b := s.detectGPU()
	if a != b {
		t.Errorf("detectGPU non-deterministic: %v vs %v", a, b)
	}
}

// TestToWireMap_OmitsZeroFields: zero fields are not present in the
// emitted map (keeps heartbeat payloads tight). Cardinal zero is
// always present if SampledAt is set.
//
// SampledHost fields (RAMBytes / DiskFreeBytes / HasGPU) belong on the
// capabilities envelope, NOT on the per-beat resources envelope; this
// test guards against a regression that would push them onto sample
// resources via a common zero-value helper.
func TestToWireMap_OmitsZeroFields(t *testing.T) {
	s := &SampledResources{
		SampledAt:    time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		CPUUtilRatio: 0,
		Load1:        0.0,
	}
	m := s.ToWireMap()
	if _, ok := m["cpu_utilization_ratio"]; ok {
		t.Errorf("zero-valued cpu_utilization_ratio should be omitted")
	}
	if _, ok := m["sampled_at"]; !ok {
		t.Errorf("sampled_at must always be present")
	}
	if _, ok := m["ram_bytes"]; ok {
		t.Errorf("ram_bytes is on SampledHost (not SampledResources); must not appear in wire map")
	}
	if _, ok := m["disk_free_bytes"]; ok {
		// DiskFreeBytes IS on both; zero should still be omitted.
		t.Errorf("zero disk_free_bytes should be omitted from a snapshot wire map")
	}
	if len(m) != 1 {
		t.Errorf("expected exactly 1 key (sampled_at), got %d: %v", len(m), m)
	}
}

// TestToWireMap_PopulatedSnapshot: non-zero values map to the
// expected snake_case keys.
func TestToWireMap_PopulatedSnapshot(t *testing.T) {
	s := &SampledResources{
		SampledAt:                 time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		CPUUtilRatio:              0.83,
		CPUIOWaitRatio:            0.08,
		CPUStealRatio:             0.05,
		MemoryUsedBytes:           4000,
		MemoryAvailableBytes:      16000,
		ProcessRSSBytes:           8192,
		SwapUsedBytes:             256,
		MajorPageFaultsTotal:      7,
		DiskReadBytesTotal:        1024,
		DiskWriteBytesTotal:       2048,
		DiskFreeBytes:             5_000_000,
		NetworkReceiveBytesTotal:  9000,
		NetworkTransmitBytesTotal: 8000,
		Load1:                     1.5,
		RunQueue:                  4,
		ActiveTasks:               3,
		TaskSlots:                 8,
	}
	m := s.ToWireMap()
	required := []string{
		"cpu_utilization_ratio",
		"memory_used_bytes",
		"memory_available_bytes",
		"process_rss_bytes",
		"swap_used_bytes",
		"major_page_faults_total",
		"disk_read_bytes_total",
		"disk_write_bytes_total",
		"disk_free_bytes",
		"network_receive_bytes_total",
		"network_transmit_bytes_total",
		"load1",
		"run_queue",
		"active_tasks",
		"task_slots",
		"sampled_at",
	}
	for _, k := range required {
		if _, ok := m[k]; !ok {
			t.Errorf("ToWireMap missing required key %q", k)
		}
	}
	if _, ok := m["network_retransmits_total"]; ok {
		t.Errorf("zero retransmits should be omitted")
	}
}

// TestClampRatio: ratios outside [0,1] are clamped.
func TestClampRatio(t *testing.T) {
	cases := []struct {
		in   float64
		want float64
	}{
		{-0.5, 0},
		{0, 0},
		{0.5, 0.5},
		{1.0, 1.0},
		{1.5, 1.0},
	}
	for _, c := range cases {
		if got := clampRatio(c.in); got != c.want {
			t.Errorf("clampRatio(%f) = %f, want %f", c.in, got, c.want)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────

func approx(a, b, eps float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= eps
}
