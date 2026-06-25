// Package config — environmental-variable overlay.
//
// PR 1 (`codex/grpc-config-single-source`): the env-vars in this file
// are the SECOND precedence layer (above worker_config.json, below CLI
// flags). They are the API surface through which containerised / k8s
// deployments inject TLS material without baking it into worker_config.json.
//
// Every env var listed below is checked exactly here — never re-read
// from os.Getenv() in the transport factory, cmd/velox-worker-agent, or
// any other consumer.
package config

import (
	"os"
	"strconv"
	"strings"
)

// Canonical mapping — keep this section aligned with applyEnvOverrides().
const (
	// EnvEnvironment tags the deployment lifecycle: dev / staging / production.
	EnvEnvironment = "VELOX_ENV"
	// EnvTLSCertFile is the worker's client leaf certificate (PEM).
	EnvTLSCertFile = "VELOX_GRPC_TLS_CERT_FILE"
	// EnvTLSKeyFile is the worker's private key (PEM).
	EnvTLSKeyFile = "VELOX_GRPC_TLS_KEY_FILE"
	// EnvTLSCAFile is the CA certificate that signed the master's cert (PEM).
	EnvTLSCAFile = "VELOX_GRPC_TLS_CA_FILE"
	// EnvAllowInsecureGRPCDev toggles plaintext gRPC for local dev only.
	EnvAllowInsecureGRPCDev = "VELOX_ALLOW_INSECURE_GRPC_DEV"
	// EnvMinDiskFreeMB overrides the readiness disk floor (RW-PROD-004 §3 A4).
	// Operators set this per-host to match the actual scratch-disk size;
	// the disk watcher in main.go downsamples MiB → bytes for ReadyState.
	EnvMinDiskFreeMB = "VELOX_MIN_DISK_FREE_MB"
	// EnvReadyzEndpoint overrides the /health/ready mount path (RW-PROD-004 §3 A9).
	// Default empty ⇒ /health/ready stays canonical. Operators set this on a
	// Kubernetes podspec that wants /readyz to keep the canonical mount out
	// of network policy scope.
	EnvReadyzEndpoint = "VELOX_READYZ_ENDPOINT"
	// EnvVideoEngineCppBin is the path to the C++ video render binary.
	// Defaults to "velox-render-cpp" (resolved via exec.LookPath in worker).
	EnvVideoEngineCppBin = "VELOX_VIDEO_ENGINE_CPP_BIN"
	// EnvWorkerClass is the fleet class (cpu-xlarge, gpu-a100, ...).
	EnvWorkerClass = "VELOX_WORKER_CLASS"
	// EnvRolloutGroup is the rollout cohort (v3.4, canary, holdout, ...).
	EnvRolloutGroup = "VELOX_ROLLOUT_GROUP"
)

// EnvBindings is the set of env-var names this package inspects.
// main.go may consult this slice (e.g. for debug dumps) but never to
// re-implement binding.
var EnvBindings = []string{
	EnvEnvironment,
	EnvTLSCertFile,
	EnvTLSKeyFile,
	EnvTLSCAFile,
	EnvAllowInsecureGRPCDev,
	EnvMinDiskFreeMB,
	EnvReadyzEndpoint,
	EnvVideoEngineCppBin,
	EnvWorkerClass,
	EnvRolloutGroup,
}

// envTruthy reports whether a string from os.Getenv should be interpreted
// as "true". Recognised truthy values: "1", "true", "TRUE", "yes", "on".
// Exposed at package level so tests and CLI docs can use the same set.
func envTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// applyEnvOverrides binds environment variables onto an already-parsed
// WorkerConfig. Called by LoadConfig() after applyDefaults().
//
// Mapping (PR 1 spec):
//
//	VELOX_ENV                          → cfg.Environment
//	VELOX_GRPC_TLS_CERT_FILE           → cfg.TLSCertFile
//	VELOX_GRPC_TLS_KEY_FILE            → cfg.TLSKeyFile
//	VELOX_GRPC_TLS_CA_FILE             → cfg.TLSCAFile
//	VELOX_ALLOW_INSECURE_GRPC_DEV      → cfg.AllowInsecureGRPC
//
// Order matters only w.r.t. each individual field — an env var always
// overrides whatever the JSON had for that field. Cross-field consistency
// is then enforced by Validate().
func applyEnvOverrides(cfg *WorkerConfig) {
	if cfg == nil {
		return
	}
	if v := os.Getenv(EnvEnvironment); v != "" {
		cfg.Environment = v
	}
	if v := os.Getenv(EnvTLSCertFile); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv(EnvTLSKeyFile); v != "" {
		cfg.TLSKeyFile = v
	}
	if v := os.Getenv(EnvTLSCAFile); v != "" {
		cfg.TLSCAFile = v
	}
	if v := os.Getenv(EnvAllowInsecureGRPCDev); v != "" {
		cfg.AllowInsecureGRPC = envTruthy(v)
	}
	// RW-PROD-004 §3 A4: MinDiskFreeMB takes the env-var lane (per-host
	// resource floor). The disk watcher applies cfg.MinDiskFreeMB to the
	// ready snapshot; we still want operators to ship a different floor
	// per cluster without re-baking worker_config.json on every node.
	if v := strings.TrimSpace(os.Getenv(EnvMinDiskFreeMB)); v != "" {
		if mb, perr := strconv.Atoi(v); perr == nil && mb > 0 {
			cfg.MinDiskFreeMB = mb
		}
	}
	// RW-PROD-004 §3 A9: VELOX_READYZ_ENDPOINT chooses the /health/ready
	// mount path. Empty string = canonical /health/ready; anything else
	// overrides and main.go wires the secondary mux accordingly.
	if v := strings.TrimSpace(os.Getenv(EnvReadyzEndpoint)); v != "" {
		cfg.ReadyzEndpoint = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvVideoEngineCppBin)); v != "" {
		cfg.VideoEngineCppBin = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvWorkerClass)); v != "" {
		cfg.WorkerClass = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvRolloutGroup)); v != "" {
		cfg.RolloutGroup = v
	}
}
