// Package media provides utilities for detecting multimedia metadata
// (audio/video duration) and URL resolution for ffprobe/ffmpeg.
package media

import (
	"os/exec"
	"strconv"
	"strings"
)

// DetectAudioDurationSecs detects the duration of an audio file via ffprobe.
// Supports direct URLs and Google Drive (resolved automatically).
// Returns 0 if detection fails.
func DetectAudioDurationSecs(url string) float64 {
	if url == "" {
		return 0
	}

	resolved := ResolveAudioURL(url)

	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		resolved,
	)
	out, err := cmd.Output()
	if err != nil {
		return 0
	}

	duration, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || duration <= 0 {
		return 0
	}

	return duration
}

// ResolveAudioURL converts Google Drive URLs into direct download format
// for ffprobe compatibility. Leaves other URLs unchanged.
func ResolveAudioURL(url string) string {
	const drivePrefix = "https://drive.google.com/file/d/"
	if strings.HasPrefix(url, drivePrefix) {
		rest := strings.TrimPrefix(url, drivePrefix)
		if idx := strings.Index(rest, "/"); idx > 0 {
			fileID := rest[:idx]
			return "https://drive.google.com/uc?export=download&id=" + fileID + "&confirm=t"
		}
	}
	if strings.Contains(url, "drive.google.com/uc") {
		return url + "&confirm=t"
	}
	return url
}
