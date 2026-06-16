package config

import (
	"os"
	"strconv"
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

// boolFromEnv reads key from the environment and parses it as a bool.
// Recognized truthy values (case-insensitive, trimmed): "1", "true", "t", "yes", "y".
// Recognized falsy  values (case-insensitive, trimmed): "0", "false", "f", "no", "n".
// Returns defaultVal when the variable is unset, empty, or unrecognized.
// Broader than strconv.ParseBool (also accepts "yes"/"no") and matches the
// previous `X == "1" || X == "true" || X == "yes"` sites across loaders.
func boolFromEnv(key string, defaultVal bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return defaultVal
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "t", "yes", "y":
		return true
	case "0", "false", "f", "no", "n":
		return false
	default:
		return defaultVal
	}
}

// intFromEnv reads key from the environment and parses it as an int.
// Returns defaultVal when the variable is unset, empty, fails to parse, or
// when the parsed value is below min. Pass min=1 to enforce "> 0", min=5
// for ">= 5", min=0 for ">= 0". Preserves the previous
// `if n, _ := strconv.Atoi(os.Getenv(...)); n > 0 { ... }` pattern.
func intFromEnv(key string, defaultVal, min int) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < min {
		return defaultVal
	}
	return n
}
