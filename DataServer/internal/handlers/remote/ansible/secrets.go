package ansible

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SecretResolver resolves secret_ref values to actual secrets.
// It supports two modes:
//   - File-based: secret_ref points to a file containing the secret
//   - Environment: secret_ref is an env var name to look up
type SecretResolver struct {
	secretsDir string
}

// NewSecretResolver creates a resolver that looks for secrets in the given directory.
func NewSecretResolver(secretsDir string) *SecretResolver {
	return &SecretResolver{secretsDir: secretsDir}
}

// Resolve resolves a secret_ref to the actual secret value.
// secret_ref formats:
//   - "file:/path/to/secret"  — reads the file
//   - "file:ssh_host_<host>"  — reads secretsDir/ssh_host_<host>
//   - "env:VAR_NAME"          — reads from environment
//   - ""                      — no secret
func (r *SecretResolver) Resolve(secretRef string) (string, error) {
	if secretRef == "" {
		return "", nil
	}

	parts := strings.SplitN(secretRef, ":", 2)
	if len(parts) != 2 {
		// Legacy: bare filename in secrets dir
		return r.resolveFile(filepath.Join(r.secretsDir, secretRef))
	}

	scheme, value := parts[0], parts[1]
	switch scheme {
	case "file":
		// If value is an absolute path, use it directly
		if filepath.IsAbs(value) {
			return r.resolveFile(value)
		}
		// Otherwise, resolve relative to secrets dir
		return r.resolveFile(filepath.Join(r.secretsDir, value))
	case "env":
		if v := os.Getenv(value); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("environment variable %s not set", value)
	default:
		return "", fmt.Errorf("unknown secret_ref scheme: %s", scheme)
	}
}

func (r *SecretResolver) resolveFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret file %s: %w", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}

// StoreSSHPassword stores an SSH password for a host as a secret file.
// The file is created with restricted permissions (0600).
func (r *SecretResolver) StoreSSHPassword(host, password string) (string, error) {
	if r.secretsDir == "" {
		return "", fmt.Errorf("secrets directory not configured")
	}

	if err := os.MkdirAll(r.secretsDir, 0700); err != nil {
		return "", fmt.Errorf("create secrets dir: %w", err)
	}

	filename := fmt.Sprintf("ssh_host_%s", sanitizeSecretFilename(host))
	path := filepath.Join(r.secretsDir, filename)

	if err := os.WriteFile(path, []byte(password), 0600); err != nil {
		return "", fmt.Errorf("write secret file: %w", err)
	}

	log.Printf("[SECRET] Stored SSH password for %s at %s", host, path)
	return fmt.Sprintf("file:%s", filename), nil
}

// MigrateSSHPassword migrates a plaintext SSHPassword from the in-memory model
// to a secret file. Returns the new secret_ref.
func (r *SecretResolver) MigrateSSHPassword(host, sshPassword string) (string, error) {
	if sshPassword == "" {
		return "", nil
	}
	return r.StoreSSHPassword(host, sshPassword)
}

// BuildSecretRef creates a secret_ref string for a host.
// If the host already has a secret file, returns the existing ref.
// Otherwise, returns empty.
func (r *SecretResolver) BuildSecretRef(host string) string {
	if r.secretsDir == "" {
		return ""
	}

	filename := fmt.Sprintf("ssh_host_%s", sanitizeSecretFilename(host))
	path := filepath.Join(r.secretsDir, filename)

	if _, err := os.Stat(path); err == nil {
		return fmt.Sprintf("file:%s", filename)
	}
	return ""
}

// sanitizeSecretFilename makes a hostname safe for use as a filename.
func sanitizeSecretFilename(host string) string {
	replacer := strings.NewReplacer(
		":", "_",
		"/", "_",
		"\\", "_",
		" ", "_",
	)
	return replacer.Replace(host)
}

// CleanupOldSecrets removes secret files for hosts that no longer exist.
func (r *SecretResolver) CleanupOldSecrets(activeHosts map[string]bool) (int, error) {
	if r.secretsDir == "" {
		return 0, nil
	}

	entries, err := os.ReadDir(r.secretsDir)
	if err != nil {
		return 0, err
	}

	removed := 0
	cutoff := time.Now().Add(-24 * time.Hour) // Only clean files older than 24h

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "ssh_host_") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().After(cutoff) {
			continue // Too new
		}

		// Extract host from filename
		hostname := strings.TrimPrefix(entry.Name(), "ssh_host_")
		if activeHosts[hostname] {
			continue // Still active
		}

		path := filepath.Join(r.secretsDir, entry.Name())
		if err := os.Remove(path); err != nil {
			log.Printf("[SECRET] Failed to remove stale secret %s: %v", path, err)
			continue
		}
		log.Printf("[SECRET] Removed stale secret for host %s", hostname)
		removed++
	}

	return removed, nil
}
