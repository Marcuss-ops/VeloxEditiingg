package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Getenv is the single environment-read boundary for application packages.
// Keeping this operation here makes the composition root/config package the
// owner of process configuration while allowing leaf packages to remain
// independent of os.Getenv semantics.
func Getenv(key string) string { return os.Getenv(key) }

// LookupEnv is the presence-aware counterpart to Getenv.
func LookupEnv(key string) (string, bool) { return os.LookupEnv(key) }

// loadEnvFile reads a .env-style file and sets the key/value pairs as
// environment variables using os.Setenv. It returns the keys it actually
// loaded, allowing RawConfigFromEnvFile to preserve provenance without
// global mutable state. It is intentionally simple and does not support
// every edge case of the dotenv format; it handles:
//   - KEY=VALUE lines
//   - KEY="VALUE" lines (double quotes only)
//   - lines starting with # or empty lines (ignored)
//   - inline comments starting with #
//
// Existing environment variables are NOT overwritten, so shell-exported
// values keep precedence. The function is safe to call with a missing
// file: it returns nil so that production deployments that do not use
// a .env file are unaffected.
func loadEnvFile(path string) (map[string]struct{}, error) {
	loaded := make(map[string]struct{})
	if path == "" {
		return loaded, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return loaded, nil
		}
		return nil, fmt.Errorf("config: cannot stat env file %q: %w", path, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("config: env file %q is a directory", path)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: cannot open env file %q: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Strip inline comments, but respect comments inside quoted values.
		if idx := strings.Index(line, "#"); idx != -1 {
			// Only strip if the # is not inside a quoted value.
			// This is a best-effort heuristic; full dotenv parsing is overkill.
			quoteCount := strings.Count(line[:idx], "\"")
			if quoteCount%2 == 0 {
				line = strings.TrimSpace(line[:idx])
			}
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove surrounding double quotes.
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}

		if key == "" {
			continue
		}

		// Do not overwrite an already-set environment variable.
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return nil, fmt.Errorf("config: set env %q: %w", key, err)
			}
			loaded[key] = struct{}{}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("config: error reading env file %q: %w", path, err)
	}
	return loaded, nil
}

// LoadEnvFile is retained as a compatibility helper for callers that only
// need to populate the process environment. New bootstrap code should call
// RawConfigFromEnvFile so source provenance stays attached to the snapshot.
func LoadEnvFile(path string) error {
	_, err := loadEnvFile(path)
	return err
}

// EnvFilePath returns the path to the .env file to load. Precedence:
// 1. VELOX_ENV_FILE environment variable
// 2. .env in the current working directory
func EnvFilePath() string {
	if path := os.Getenv("VELOX_ENV_FILE"); path != "" {
		return path
	}
	if cwd, err := os.Getwd(); err == nil {
		return filepath.Join(cwd, ".env")
	}
	return ""
}
