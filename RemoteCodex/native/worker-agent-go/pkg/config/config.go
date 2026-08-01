// Package config provides configuration management for the Velox Worker Agent.
//
// PR 1 (`codex/grpc-config-single-source`): every TLS-related field is
// resolved in ONE place — this package — through LoadConfig() and
// Validate(). The transport factory receives an already-validated
// `GRPCTLSConfig` struct (see `WorkerConfig.GRPCTLS()`) and is no longer
// allowed to make its own env-var reads or combinatorial decisions.
//
// Precedence (highest wins): CLI flags > environment variables >
// worker_config.json > built-in defaults. This package handles
// everything below "CLI flags" — main.go applies the CLI flag overrides
// BEFORE Validate() is called.
//
// File layout (per concern):
//   - config.go          — package doc + shared helpers (GRPCTLS, NormalizeWorkerID,
//     IsCreatorProfile*, String)
//   - config_types.go    — GRPCTLSConfig, WorkerConfig, ErrInvalidConfig, constants
//   - config_load_save.go — LoadConfig / SaveConfig (JSON persistence)
//   - config_defaults.go — DefaultConfig / applyDefaults / GenerateWorkerID
//   - config_validate.go — Validate (TLS combinatorial + RW-PROD-001 A1/A2 rules)
//   - env.go             — env-var overlay (second precedence layer)
//   - remote_endpoint.go — ValidateRemoteMasterEndpoint (production self-execution gate)
package config

import (
	"fmt"
	"strings"

	"velox-shared/identity"
)

// GRPCTLS bundles the four TLS-related WorkerConfig fields into a
// transport-friendly struct. Callers MUST consume TLS configuration
// through this accessor — never reconstruct it from individual fields,
// or combinatorial invariants get re-implemented in the wrong place.
func (c *WorkerConfig) GRPCTLS() GRPCTLSConfig {
	if c == nil {
		return GRPCTLSConfig{}
	}
	return GRPCTLSConfig{
		CertFile:         c.TLSCertFile,
		KeyFile:          c.TLSKeyFile,
		CAFile:           c.TLSCAFile,
		AllowInsecureDev: c.AllowInsecureGRPC,
	}
}

// NormalizeWorkerID normalizes IP-derived worker IDs by stripping all leading
// "host_" prefixes and replacing dots with underscores.
//
// Implementation lives in shared/identity so the canonical rules are shared
// with the Velox master server. Test cases live in shared/identity_test.go.
func NormalizeWorkerID(id string) string {
	return identity.NormalizeWorkerID(id)
}

// IsCreatorProfile reports whether this worker is configured for the
// creator profile, with case-insensitive matching and whitespace trimming.
func (c *WorkerConfig) IsCreatorProfile() bool {
	if c == nil {
		return false
	}
	return IsCreatorProfileValue(c.WorkerProfile)
}

// IsCreatorProfileValue reports whether the supplied profile string
// identifies the creator profile, with case-insensitive matching and
// whitespace trimming.
func IsCreatorProfileValue(profile string) bool {
	return strings.ToLower(strings.TrimSpace(profile)) == CreatorProfile
}

// String returns a formatted string representation of the config (for logging).
func (c *WorkerConfig) String() string {
	if c == nil {
		return "WorkerConfig{nil}"
	}
	return fmt.Sprintf("WorkerConfig{WorkerID: %s, WorkerName: %s, MasterURL: %s, WorkDir: %s, Environment: %s}",
		c.WorkerID, c.WorkerName, c.MasterURL, c.WorkDir, c.Environment)
}
