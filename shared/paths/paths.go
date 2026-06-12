// Package paths fornisce utility per la manipolazione di path, slug di nomi video
// e normalizzazione di URL (in particolare Google Drive).
package paths

import (
	"path/filepath"
	"strings"
)

// SanitizeVideoName converte un nome video in uno slug safe per filesystem.
// Mantiene solo lettere minuscole e numeri, sostituendo tutto il resto con underscore.
// Esempio: "My Awesome Video! (2024)" → "my_awesome_video_2024"
func SanitizeVideoName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('_')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

// SanitizeStrings trima e rimuove stringhe vuote da una slice.
// Restituisce nil se non rimangono elementi validi.
func SanitizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// DefaultOutputPath genera un path di output predefinito per un video.
// Usa videosDir se specificato, altrimenti dataDir/generated_videos, altrimenti ./generated_videos.
// Il nome file è {slug}.mp4 nella sottodirectory specificata.
func DefaultOutputPath(videosDir, dataDir, videoName, subdir string) string {
	base := strings.TrimSpace(videosDir)
	if base == "" {
		if dataDir != "" {
			base = filepath.Join(dataDir, "generated_videos")
		} else {
			base = filepath.Join(".", "generated_videos")
		}
	}
	slug := SanitizeVideoName(videoName)
	if slug == "" {
		slug = "video"
	}
	if subdir == "" {
		subdir = "default"
	}
	return filepath.Join(base, subdir, slug+".mp4")
}

// VideoNameFromPath estrae il nome base del video (senza estensione) da un path.
// Esempio: "/output/videos/my_video.mp4" → "my_video"
func VideoNameFromPath(outputPath string) string {
	base := filepath.Base(outputPath)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

// SanitizeDriveFolderName converte un nome in un formato accettabile per cartelle Google Drive
// (solo lowercase, numeri, trattini e underscore).
func SanitizeDriveFolderName(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else if r >= 'A' && r <= 'Z' {
			b.WriteRune(r - 'A' + 'a')
		} else if r == ' ' || r == '/' || r == '\\' {
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

// NormalizeDriveURL converte URL di Google Drive in URL di download diretto.
// Supporta formati:
//   - https://drive.google.com/file/d/FILE_ID/view → https://drive.google.com/uc?export=download&id=FILE_ID&confirm=t
//   - https://drive.google.com/uc?id=FILE_ID → +&confirm=t
func NormalizeDriveURL(url string) string {
	s := strings.TrimSpace(url)
	if s == "" {
		return ""
	}
	// /file/d/FILE_ID/view
	const drivePrefix = "https://drive.google.com/file/d/"
	if strings.HasPrefix(s, drivePrefix) {
		rest := strings.TrimPrefix(s, drivePrefix)
		if idx := strings.Index(rest, "/"); idx > 0 {
			fileID := rest[:idx]
			return "https://drive.google.com/uc?export=download&id=" + fileID + "&confirm=t"
		}
	}
	// /uc?id=FILE_ID
	if strings.Contains(s, "drive.google.com/uc") {
		return s + "&confirm=t"
	}
	return s
}

// ExtractDriveID estrae il file ID da un URL di Google Drive.
// Cerca pattern /d/FILE_ID e id=FILE_ID, validando lunghezza >= 10.
// Restituisce stringa vuota se non trova un ID valido.
func ExtractDriveID(url string) string {
	s := strings.TrimSpace(url)
	if s == "" {
		return ""
	}
	// /d/FILE_ID pattern — Google Drive file ID is typically 28+ chars, min 10
	if idx := strings.Index(s, "/d/"); idx >= 0 {
		rest := s[idx+3:]
		if end := strings.IndexAny(rest, "/?"); end > 0 {
			if len(rest[:end]) >= 10 {
				return rest[:end]
			}
		} else if len(rest) >= 10 {
			return rest
		}
	}
	// id=FILE_ID pattern
	if idx := strings.Index(s, "id="); idx >= 0 {
		rest := s[idx+3:]
		if end := strings.IndexAny(rest, "&?"); end > 0 {
			if len(rest[:end]) >= 10 {
				return rest[:end]
			}
		} else if len(rest) >= 10 {
			return rest
		}
	}
	return ""
}
