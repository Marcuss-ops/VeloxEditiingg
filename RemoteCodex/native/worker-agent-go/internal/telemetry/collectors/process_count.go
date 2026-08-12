package collectors

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// countFFmpegProcesses counts visible processes whose command line contains
// the ffmpeg executable name. It is deliberately best-effort: restricted
// /proc mounts, short-lived processes, and permission errors are ignored.
func (s *Sampler) countFFmpegProcesses() int {
	entries, err := os.ReadDir(s.procRoot)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(s.procRoot, entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		command := strings.ToLower(strings.ReplaceAll(string(cmdline), "\x00", " "))
		if strings.Contains(command, "ffmpeg") {
			count++
		}
	}
	return count
}
