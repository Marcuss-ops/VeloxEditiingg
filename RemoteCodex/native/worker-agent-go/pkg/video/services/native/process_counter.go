package native

// process_counter.go owns the worker-side external-process accounting
// for a render. The C++ engine spawns its own tool processes (ffmpeg,
// ffprobe, /bin/sh wrappers, curl) that the Go worker never sees at the
// exec.Command level, and the engine sidecar does not report them.
// Because the engine runs in its own process group (Setpgid in
// engine_process.go), every descendant shares that group — so the
// worker can count them by scanning /proc for the engine's pgid and
// classifying each live process by its comm name.
//
// Phase-1 limitation (documented, deliberate): the scan is a poll, so
// tool processes that live for less than one sample interval can be
// missed. A count of zero is therefore NOT proof that a tool never
// ran — treat "external_ffmpeg_exec = 0" as directional evidence, not
// exact process accounting.

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ProcessCounts aggregates the distinct external processes observed in
// the engine's process group during a render. ExternalProcessCount is
// the total; the kind counters break it down.
type ProcessCounts struct {
	ExternalProcessCount int64
	FfmpegExecCount      int64
	FfprobeExecCount     int64
	ShellExecCount       int64
	CurlExecCount        int64
	OtherExecCount       int64
}

// TreeIO aggregates the engine process tree's byte counters as read
// from /proc/<pid>/io. BytesRead/BytesWritten are the logical rchar/
// wchar counters (every byte moved through read()/write(), including
// page-cache hits) — the right basis for I/O amplification math.
// StorageBytesRead/StorageBytesWritten are the block-layer read_bytes/
// write_bytes counters (actual storage I/O after the page cache). Both
// are cumulative over each process's lifetime; the sampler keeps the
// per-PID maximum and sums across the tree.
type TreeIO struct {
	BytesRead           int64
	BytesWritten        int64
	StorageBytesRead    int64
	StorageBytesWritten int64
}

// ProcessTelemetry bundles everything the sampler observes while the
// engine runs: the external spawn counts by kind and the tree's byte
// counters (engine + descendants).
type ProcessTelemetry struct {
	Counts ProcessCounts
	IO     TreeIO
}

// ProcessKind classifies an external process by its /proc comm name.
type ProcessKind string

// Canonical ProcessKind values.
const (
	KindFfmpeg  ProcessKind = "ffmpeg"
	KindFfprobe ProcessKind = "ffprobe"
	KindShell   ProcessKind = "shell"
	KindCurl    ProcessKind = "curl"
	KindOther   ProcessKind = "other"
)

// externalProcessSampleInterval is how often the worker samples the
// engine's process group while it renders.
const externalProcessSampleInterval = 50 * time.Millisecond

// classifyComm maps a /proc/<pid>/comm value to a ProcessKind. Linux
// truncates comm to 15 bytes; every known tool name fits well inside
// that limit.
func classifyComm(comm string) ProcessKind {
	comm = strings.TrimSpace(comm)
	switch comm {
	case "ffmpeg":
		return KindFfmpeg
	case "ffprobe":
		return KindFfprobe
	case "curl":
		return KindCurl
	case "sh", "bash", "dash", "ash", "zsh", "fish", "ksh", "ksh93":
		return KindShell
	default:
		return KindOther
	}
}

// addComm counts one distinct external process under its kind.
func (c *ProcessCounts) addComm(comm string) {
	c.ExternalProcessCount++
	switch classifyComm(comm) {
	case KindFfmpeg:
		c.FfmpegExecCount++
	case KindFfprobe:
		c.FfprobeExecCount++
	case KindShell:
		c.ShellExecCount++
	case KindCurl:
		c.CurlExecCount++
	default:
		c.OtherExecCount++
	}
}

// sampleProcessGroup returns pid → comm for every live process in the
// process group pgid. On non-Linux platforms, or when /proc is not
// mounted, it returns an empty map — the counters degrade to zero
// instead of failing the render.
func sampleProcessGroup(pgid int) map[int]string {
	found := map[int]string{}
	if runtime.GOOS != "linux" {
		return found
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return found
	}
	for _, entry := range entries {
		if !entry.IsDir() || !isNumeric(entry.Name()) {
			continue
		}
		stat, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}
		pid, pgrp, comm := parseStatPGRP(stat)
		if pid <= 0 || pgrp != pgid {
			continue
		}
		found[pid] = comm
	}
	return found
}

// parseStatPGRP extracts pid, process group id and comm from one
// /proc/<pid>/stat line. The comm field may contain spaces and
// parentheses, so it is located between the first '(' and the last ')';
// pgrp is the third whitespace-separated field after the closing ')'.
func parseStatPGRP(stat []byte) (pid, pgrp int, comm string) {
	open := bytes.IndexByte(stat, '(')
	close := bytes.LastIndexByte(stat, ')')
	if open <= 0 || close <= open {
		return 0, 0, ""
	}
	if _, err := fmt.Sscanf(string(stat[:open]), "%d", &pid); err != nil || pid <= 0 {
		return 0, 0, ""
	}
	comm = string(stat[open+1 : close])
	rest := bytes.Fields(stat[close+1:])
	// rest[0]=state, rest[1]=ppid, rest[2]=pgrp, rest[3]=session …
	if len(rest) < 3 {
		return 0, 0, ""
	}
	if _, err := fmt.Sscanf(string(rest[2]), "%d", &pgrp); err != nil {
		return 0, 0, ""
	}
	return pid, pgrp, comm
}

// monitorProcessGroup polls every live process whose pgrp == pgid each
// interval until stop is closed. It counts each distinct PID once by
// its comm (the engine subprocess itself — excludePID — is never
// counted: the worker already accounts for it via EngineSpawnCount) and
// accumulates the tree's /proc/<pid>/io byte counters, INCLUDING the
// engine's own bytes (the engine does real media work, so its I/O is
// part of the render's footprint).
//
// Callers MUST pass pgid == excludePID: the engine runs with Setpgid,
// so its process group id equals its own PID. Sampling a non-leader PID
// as pgid would match nothing and silently yield zero counts.
func monitorProcessGroup(pgid, excludePID int, interval time.Duration, stop <-chan struct{}) ProcessTelemetry {
	var telemetry ProcessTelemetry
	seen := make(map[int]struct{})
	ioPeak := make(map[int]TreeIO)
	sample := func() {
		for pid, comm := range sampleProcessGroup(pgid) {
			if pid != excludePID {
				if _, ok := seen[pid]; !ok {
					seen[pid] = struct{}{}
					telemetry.Counts.addComm(comm)
				}
			}
			// /proc/<pid>/io counters are cumulative per process lifetime;
			// keeping the per-PID maximum and summing over all PIDs seen
			// yields the tree total even for processes that exited between
			// samples.
			if io := sampleProcessIO(pid); io != (TreeIO{}) {
				peak := ioPeak[pid]
				peak.BytesRead = maxInt64(peak.BytesRead, io.BytesRead)
				peak.BytesWritten = maxInt64(peak.BytesWritten, io.BytesWritten)
				peak.StorageBytesRead = maxInt64(peak.StorageBytesRead, io.StorageBytesRead)
				peak.StorageBytesWritten = maxInt64(peak.StorageBytesWritten, io.StorageBytesWritten)
				ioPeak[pid] = peak
			}
		}
	}
	// Sample once immediately, then keep polling until told to stop.
	sample()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			for _, peak := range ioPeak {
				telemetry.IO.BytesRead += peak.BytesRead
				telemetry.IO.BytesWritten += peak.BytesWritten
				telemetry.IO.StorageBytesRead += peak.StorageBytesRead
				telemetry.IO.StorageBytesWritten += peak.StorageBytesWritten
			}
			return telemetry
		case <-ticker.C:
			sample()
		}
	}
}

// sampleProcessIO reads /proc/<pid>/io and returns the four byte
// counters. On non-Linux platforms, unreadable files, or malformed
// content it returns a zero TreeIO so the sampler degrades gracefully.
func sampleProcessIO(pid int) TreeIO {
	var io TreeIO
	if runtime.GOOS != "linux" || pid <= 0 {
		return io
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/io", pid))
	if err != nil {
		return io
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}
		switch strings.TrimSpace(key) {
		case "rchar":
			io.BytesRead = n
		case "wchar":
			io.BytesWritten = n
		case "read_bytes":
			io.StorageBytesRead = n
		case "write_bytes":
			io.StorageBytesWritten = n
		}
	}
	return io
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
