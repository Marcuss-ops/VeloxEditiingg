package config

import (
	"os"
	"strings"
)

func parseCommaList(s string) []string {
	if s == "" {
		return nil
	}
	parts := make([]string, 0)
	for _, p := range splitByComma(s) {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func firstExistingDir(candidates []string) string {
	for _, path := range candidates {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			return path
		}
	}
	return ""
}

// firstExistingDirWithFiles prefers a directory that contains at least one
// regular file. Runtime token directories may be provisioned eagerly, so the
// first existing directory can be empty while a fallback contains the active
// OAuth tokens.
func firstExistingDirWithFiles(candidates []string) string {
	var firstDir string
	for _, path := range candidates {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			continue
		}
		if firstDir == "" {
			firstDir = path
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				return path
			}
		}
	}
	return firstDir
}

func splitByComma(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
