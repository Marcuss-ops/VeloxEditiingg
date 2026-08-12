package native

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// readSelfStat reads our own /proc/<pid>/stat for tests that need a
// live stat line. Linux only — callers skip on other platforms.
func readSelfStat(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", os.Getpid()))
	require.NoError(t, err)
	return data
}

func TestClassifyComm(t *testing.T) {
	cases := []struct {
		comm string
		want ProcessKind
	}{
		{"ffmpeg", KindFfmpeg},
		{"ffprobe", KindFfprobe},
		{"curl", KindCurl},
		{"sh", KindShell},
		{"bash", KindShell},
		{"dash", KindShell},
		{"ash", KindShell},
		{"zsh", KindShell},
		{"sleep", KindOther},
		{"python3", KindOther},
		{"", KindOther},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, classifyComm(tc.comm), "comm %q", tc.comm)
	}
}

func TestParseProcStat(t *testing.T) {
	// /proc/<pid>/stat shape: pid (comm with spaces) state ppid pgrp …
	// Fields after comm follow Linux stat(5): rest[0]=state(3),
	// rest[2]=pgrp(5), rest[11]=utime(14), rest[12]=stime(15),
	// rest[21]=rss(24, pages).
	line := "123 (ffmpeg -i) S 45 123 123 0 -1 4194304 82 0 0 0 100 50 0 0 20 0 1 0 42 0 256 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
	pid, pgrp, comm, userTicks, sysTicks, rssBytes := parseProcStat([]byte(line))
	require.Equal(t, 123, pid)
	require.Equal(t, 123, pgrp)
	require.Equal(t, "ffmpeg -i", comm)
	require.Equal(t, int64(100), userTicks)
	require.Equal(t, int64(50), sysTicks)
	require.Equal(t, int64(256)*int64(os.Getpagesize()), rssBytes)

	// Malformed inputs must not panic and must return zero values.
	pid, pgrp, comm, userTicks, sysTicks, rssBytes = parseProcStat([]byte(""))
	require.Zero(t, pid)
	require.Zero(t, pgrp)
	require.Empty(t, comm)
	require.Zero(t, userTicks)
	require.Zero(t, sysTicks)
	require.Zero(t, rssBytes)

	pid, pgrp, comm, userTicks, sysTicks, rssBytes = parseProcStat([]byte("42"))
	require.Zero(t, pid)
	require.Zero(t, pgrp)
	require.Empty(t, comm)
	require.Zero(t, userTicks)
	require.Zero(t, sysTicks)
	require.Zero(t, rssBytes)

	// A short stat line (fewer than 22 fields after comm) must still
	// parse pid/pgrp/comm and leave the CPU/RSS counters zero.
	pid, pgrp, comm, userTicks, sysTicks, rssBytes = parseProcStat([]byte("7 (short) S 1 7 7 0"))
	require.Equal(t, 7, pid)
	require.Equal(t, 7, pgrp)
	require.Equal(t, "short", comm)
	require.Zero(t, userTicks)
	require.Zero(t, sysTicks)
	require.Zero(t, rssBytes)
}

// TestMonitorProcessGroup_CountsRealTree is a Linux integration test:
// it spawns a real process tree in its own process group and verifies
// the sampler classifies the descendants. The outer sh is the "engine
// root" (excluded); the inner sh (a background sh that keeps its own
// background sleep + wait alive for ~2s so it cannot be exec-optimized
// away) and the sleep it forked must be counted as shell and other.
func TestMonitorProcessGroup_CountsRealTree(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process group sampling requires /proc (Linux)")
	}
	cmd := exec.Command("sh", "-c", `sh -c "sleep 2 & wait" & wait`)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()

	stop := make(chan struct{})
	telCh := make(chan ProcessTelemetry, 1)
	go func() {
		telCh <- monitorProcessGroup(cmd.Process.Pid, cmd.Process.Pid, 10*time.Millisecond, stop)
	}()
	// The tree spawns within milliseconds and the sleeps live for 2s,
	// so the sampler has plenty of windows to observe them.
	time.Sleep(300 * time.Millisecond)
	close(stop)
	counts := (<-telCh).Counts

	require.GreaterOrEqual(t, counts.ShellExecCount, int64(1), "inner background shell must be counted")
	require.GreaterOrEqual(t, counts.OtherExecCount, int64(1), "sleep must be counted as other")
	require.Equal(t, counts.ShellExecCount+counts.OtherExecCount, counts.ExternalProcessCount)
}

// TestMonitorProcessGroup_ExcludesRoot verifies that the root PID is
// never counted as an external process: with `sleep 2 & sleep 2 & wait`
// only the two sleeps are external, and the outer sh (the root) is
// excluded.
func TestMonitorProcessGroup_ExcludesRoot(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process group sampling requires /proc (Linux)")
	}
	cmd := exec.Command("sh", "-c", `sleep 2 & sleep 2 & wait`)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()

	stop := make(chan struct{})
	telCh := make(chan ProcessTelemetry, 1)
	go func() {
		telCh <- monitorProcessGroup(cmd.Process.Pid, cmd.Process.Pid, 10*time.Millisecond, stop)
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	counts := (<-telCh).Counts

	require.Zero(t, counts.ShellExecCount, "root sh must be excluded")
	require.Equal(t, int64(2), counts.OtherExecCount, "the two sleeps are the only external processes")
	require.Equal(t, int64(2), counts.ExternalProcessCount)
}

func TestMonitorProcessGroup_NoProcDegradesToZero(t *testing.T) {
	// A closed stop channel with an unreachable pgid must return zero
	// counts without blocking or erroring (the sampler also handles
	// non-Linux / missing /proc by returning an empty sample).
	stop := make(chan struct{})
	close(stop)
	tel := monitorProcessGroup(99999999, 99999999, time.Millisecond, stop)
	require.Zero(t, tel.Counts.ExternalProcessCount)
	require.Zero(t, tel.IO.BytesRead)
	require.Zero(t, tel.IO.BytesWritten)
	require.Zero(t, tel.CPU.TotalMs)
	require.Zero(t, tel.CPU.PeakRSSBytes)
}

func TestSampleProcessCPU(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process CPU/RSS counters require /proc (Linux)")
	}
	// Our own process has a live /proc/<pid>/stat with readable
	// utime/stime ticks and a non-zero resident set.
	_, _, _, userTicks, sysTicks, rssBytes := parseProcStat(readSelfStat(t))
	require.GreaterOrEqual(t, userTicks, int64(0))
	require.GreaterOrEqual(t, sysTicks, int64(0))
	require.GreaterOrEqual(t, userTicks+sysTicks, int64(0))
	require.Greater(t, rssBytes, int64(0), "the test process must have a resident set")
}

// TestMonitorProcessGroup_CollectsTreeCPUAndRSS is a Linux integration
// test: a child that burns CPU while holding a large resident set must
// be reflected in the tree CPU totals and the peak RSS. The root is
// INCLUDED (the engine does real media work, so its CPU and RSS are
// part of the attempt's footprint).
func TestMonitorProcessGroup_CollectsTreeCPUAndRSS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process group sampling requires /proc (Linux)")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 required for the CPU/RSS integration fixture")
	}
	// python3 holds a 32 MiB zero-filled buffer (resident pages) while
	// busy-looping for the whole sample window, so both CPU and RSS are
	// observable.
	cmd := exec.Command("sh", "-c", `exec python3 -c 'import time; buf = bytearray(32 * 1024 * 1024); end = time.time() + 1.5; x = 0
while time.time() < end:
    x += 1'`)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()

	stop := make(chan struct{})
	telCh := make(chan ProcessTelemetry, 1)
	go func() {
		telCh <- monitorProcessGroup(cmd.Process.Pid, cmd.Process.Pid, 10*time.Millisecond, stop)
	}()
	time.Sleep(700 * time.Millisecond)
	close(stop)
	cpu := (<-telCh).CPU

	// The busy loop runs for the whole window, so the tree must burn a
	// measurable amount of CPU (≥ 100 ms of combined user+system on any
	// machine that can run the Go test suite).
	require.GreaterOrEqual(t, cpu.TotalMs, int64(100), "busy child must burn CPU")
	require.GreaterOrEqual(t, cpu.UserMs, int64(0))
	require.GreaterOrEqual(t, cpu.SystemMs, int64(0))
	require.Equal(t, cpu.UserMs+cpu.SystemMs, cpu.TotalMs)
	// The 32 MiB buffer guarantees a resident set well above the floor.
	require.GreaterOrEqual(t, cpu.PeakRSSBytes, int64(8*1024*1024), "tree peak RSS must observe the buffer")
	require.GreaterOrEqual(t, cpu.CurrentRSSBytes, int64(0))
	require.GreaterOrEqual(t, cpu.PeakRSSBytes, cpu.CurrentRSSBytes)
}

func TestSampleProcessIO(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process IO counters require /proc (Linux)")
	}
	// Our own process is guaranteed to have /proc/<pid>/io with at least
	// some bytes read/written during test bootstrap.
	io := sampleProcessIO(os.Getpid())
	require.GreaterOrEqual(t, io.BytesRead, int64(0))
	require.GreaterOrEqual(t, io.BytesWritten, int64(0))
	// rchar/wchar must never be negative.
	require.GreaterOrEqual(t, io.StorageBytesRead, int64(0))

	require.Zero(t, sampleProcessIO(99999999), "unreachable pid must yield zero IO")
	require.Zero(t, sampleProcessIO(0))
}

// TestMonitorProcessGroup_CollectsTreeIO is a Linux integration test:
// a child process that streams bytes through read()/write() must be
// reflected in the tree totals, including for the excluded engine root
// (the root does real media work, so its bytes count toward the tree).
func TestMonitorProcessGroup_CollectsTreeIO(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process group sampling requires /proc (Linux)")
	}
	// dd streams 1 GiB from /dev/zero to /dev/null so the process stays
	// alive for the whole sample window; rchar/wchar count every byte
	// through read()/write() regardless of the device.
	cmd := exec.Command("sh", "-c", `exec dd if=/dev/zero of=/dev/null bs=1M count=1024 2>/dev/null`)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()

	stop := make(chan struct{})
	telCh := make(chan ProcessTelemetry, 1)
	go func() {
		telCh <- monitorProcessGroup(cmd.Process.Pid, cmd.Process.Pid, 10*time.Millisecond, stop)
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	io := (<-telCh).IO

	// The dd child has read and written at least 64 KiB by any sample
	// window (1 MiB blocks stream at GB/s through page cache).
	require.GreaterOrEqual(t, io.BytesRead, int64(65536), "tree rchar must observe the child's reads")
	require.GreaterOrEqual(t, io.BytesWritten, int64(65536), "tree wchar must observe the child's writes")
}
