// Package media fornisce utility per il rilevamento di metadati multimediali
// (durata audio/video) e la risoluzione di URL per ffprobe/ffmpeg.
package media

import (
	"os/exec"
	"strconv"
	"strings"
)

// DetectAudioDurationSecs rileva la durata di un file audio tramite ffprobe.
// Supporta URL diretti e Google Drive (risolti automaticamente).
// Restituisce 0 se il rilevamento fallisce.
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

// ResolveAudioURL converte URL Google Drive in formato di download diretto
// per compatibilità con ffprobe. Lascia invariati gli altri URL.
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
