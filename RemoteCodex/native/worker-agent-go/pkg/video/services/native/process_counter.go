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
// interval until stop is closed, counting each distinct PID once by its
// comm. excludePID — the engine subprocess itself — is never counted:
// the worker already accounts for it via EngineSpawnCount.
//
// Callers MUST pass pgid == excludePID: the engine runs with Setpgid,
// so its process group id equals its own PID. Sampling a non-leader PID
// as pgid would match nothing and silently yield zero counts.
func monitorProcessGroup(pgid, excludePID int, interval time.Duration, stop <-chan struct{}) ProcessCounts {
	var counts ProcessCounts
	seen := make(map[int]struct{})
	sample := func() {
		for pid, comm := range sampleProcessGroup(pgid) {
			if pid == excludePID {
				continue
			}
			if _, ok := seen[pid]; ok {
				continue
			}
			seen[pid] = struct{}{}
			counts.addComm(comm)
		}
	}
	// Sample once immediately, then keep polling until told to stop.
	sample()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return counts
		case <-ticker.C:
			sample()
		}
	}
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
