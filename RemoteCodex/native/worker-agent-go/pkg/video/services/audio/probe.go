// Package audio provides audio file probing and analysis services.
package audio

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// Probe detects audio file properties.
type Probe interface {
	// DurationSeconds returns the duration of the audio file in seconds.
	// Returns 0 if the file cannot be probed.
	DurationSeconds(path string) float64
}

// FFprobe implements Probe using ffprobe.
type FFprobe struct{}

// DurationSeconds returns the duration of the audio file using ffprobe.
func (f *FFprobe) DurationSeconds(path string) float64 {
	out, err := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path).Output()
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(out))
	if s == "" || s == "N/A" {
		return 0
	}
	dur, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	if dur <= 0 {
		return 0
	}
	return dur
}

// ProbeResult holds the result of probing a media file.
type ProbeResult struct {
	DurationSeconds float64
	Format          string
}

// ProbeFile probes a media file and returns its properties.
func ProbeFile(path string) (*ProbeResult, error) {
	out, err := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration,format_name",
		"-of", "default=noprint_wrappers=1",
		path).Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe %s: %w", path, err)
	}

	result := &ProbeResult{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "duration=") {
			s := strings.TrimPrefix(line, "duration=")
			if s != "N/A" {
				result.DurationSeconds, _ = strconv.ParseFloat(s, 64)
			}
		}
		if strings.HasPrefix(line, "format_name=") {
			result.Format = strings.TrimPrefix(line, "format_name=")
		}
	}
	return result, nil
}
