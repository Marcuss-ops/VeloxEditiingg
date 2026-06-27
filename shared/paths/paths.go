// Package paths provides utilities for path manipulation, video name slugs,
// and URL normalization (particularly Google Drive URLs).
package paths

import (
	"path/filepath"
	"strings"
)

// SanitizeVideoName converts a video name into a filesystem-safe slug.
// Keeps only lowercase letters and digits, replacing everything else with underscores.
// Example: "My Awesome Video! (2024)" → "my_awesome_video_2024"
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

// SanitizeStrings trims and removes empty strings from a slice.
// Returns nil if no valid elements remain.
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

// DefaultOutputPath generates a default output path for a video.
// Uses videosDir if specified, otherwise dataDir/generated_videos, otherwise ./generated_videos.
// The filename is {slug}.mp4 in the specified subdirectory.
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

// VideoNameFromPath extracts the base video name (without extension) from a path.
// Example: "/output/videos/my_video.mp4" → "my_video"
func VideoNameFromPath(outputPath string) string {
	base := filepath.Base(outputPath)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

// SanitizeDriveFolderName converts a name into an acceptable format for Google Drive folders
// (lowercase, digits, dashes, and underscores only).
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

// NormalizeDriveURL converts Google Drive URLs into direct download URLs.
// Supports formats:
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

// ExtractDriveID extracts the file ID from a Google Drive URL.
// Looks for /d/FILE_ID and id=FILE_ID patterns, validating length >= 10.
// Returns an empty string if no valid ID is found.
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
