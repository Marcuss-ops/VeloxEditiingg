// auth.go — token resolution per design Q4.
//
// Precedence:
//   1. --token-file=<PATH> (if explicitly set on the flag set)
//   2. $VELOX_ADMIN_TOKEN (env var; trimmed)
//   3. /opt/velox/secrets/admin-token (canonical Master-side file,
//      chmod 600)
//
// If all three are absent, resolveToken returns an error and the
// handler maps it to ExitMisuse (2). The file in (3) is the same
// path the velox-server binary reads from per deploy/runtime/*.
//
// File-mode validation: enforce that any path-loaded token file
// is chmod 600 (owner-only). Catches operator mistakes where a
// loose-mode file leaks credentials into the audit table or
// shared home-directory backups.

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"velox-server/internal/config"
)

// canonicalTokenPaths is the default lookup order for token
// resolution. --token-file is prepended at runClientConfig
// time, so this list is purely the "implicit" fallback chain.
var canonicalTokenPaths = []string{
	"/opt/velox/secrets/admin-token",
}

// loadTokenFromFile reads + validates the chmod-mode of `path`.
// Returns the trimmed token string on success; errors if the
// file mode is looser than 0600 OR the file is unreadable OR the
// contents are empty.
func loadTokenFromFile(path string) (string, error) {
	if path == "" {
		return "", errors.New("empty token file path")
	}
	st, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat token file %q: %w", path, err)
	}
	mode := st.Mode().Perm()
	// Reject anything where group/other bits are set. Owner
	// bits 0o600 (rw for owner, no execute) is the only
	// permissible mode; 0o700 counts as OK because it
	// disables group/other access (POSIX-strict semantics).
	if mode&^0o600 != 0 && mode&0o077 != 0 {
		return "", fmt.Errorf("token file %q has group/other permissions (mode=0%o); chmod 600 required", path, mode)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file %q: %w", path, err)
	}
	tok := strings.TrimSpace(string(raw))
	if tok == "" {
		return "", fmt.Errorf("token file %q is empty", path)
	}
	return tok, nil
}

// envToken reads $VELOX_ADMIN_TOKEN and trims whitespace.
// Returns ("", nil) when env var is unset so the caller can
// fall through to the file-based resolver.
func envToken() (string, bool) {
	v := config.Getenv("VELOX_ADMIN_TOKEN")
	if v == "" {
		return "", false
	}
	tok := strings.TrimSpace(v)
	if tok == "" {
		return "", false
	}
	return tok, true
}

// resolveTokenAdvanced is a richer-than-raw resolver exposed
// for future callers; today's loadClientConfig uses the
// simpler inline path it ships in client.go (kept here for
// testability + future mTLS surface).
func resolveTokenAdvanced(explicitFile string) (string, error) {
	if explicitFile != "" {
		return loadTokenFromFile(explicitFile)
	}
	if tok, ok := envToken(); ok {
		return tok, nil
	}
	for _, p := range canonicalTokenPaths {
		tok, err := loadTokenFromFile(p)
		if err == nil {
			return tok, nil
		}
	}
	return "", errors.New("admin token not found via --token-file, $VELOX_ADMIN_TOKEN, or canonical TokenPaths")
}
