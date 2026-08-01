// Package config / config_validate.go — configuration validation.
//
// Validate() enforces the PR 1 TLS combinatorial invariants, the
// RW-PROD-001 A1/A2 cert-lifecycle checks, and the required-field
// rules. It is the single validation surface for WorkerConfig; callers
// must not re-implement these rules elsewhere.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"velox-shared/identity"
)

// Validate checks that all required configuration fields are set.
// Returns an error with details if validation fails.
//
// PR 1 invariants added for GRPCTLS:
//   - Exactly one of {cert+key+ca full triple WITH matching key, allow_insecure_grpc_dev=true in non-prod}
//     must be configured. Anything in between (only cert, cert+key no ca, mismatched key, etc.)
//     is REJECTED with a precise error.
//   - Setting both TLS AND allow_insecure_grpc_dev is REJECTED.
//   - allow_insecure_grpc_dev=true in `production` environment is REJECTED.
//   - The tls_cert_file path must exist on disk AND tls_key_file must pair against it.
//   - environment field, if set, must be one of dev|staging|production.
//
// SPEC NOTE — migration breaking change:
//
//	Previously the transport factory accepted a plain (no-TLS / no-insecure) WorkerConfig
//	when VELOX_ALLOW_INSECURE_GRPC_DEV=true was set in the environment, even in
//	production-like deployments. PR 1 tightens this: workers MUST either (a) provide
//	the full TLS triple, or (b) opt into dev-only plaintext explicitly by setting
//	VELOX_ENV=dev (or `environment: "dev"` / `"staging"` in JSON). Existing
//	deployments that relied on the undocumented env-only bypass need to add
//	VELOX_ENV=dev or `environment: "dev"` to keep working. See
//	docs/operations/PR-1-migration.md for the operator recipe.
func (c *WorkerConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: config is nil", ErrInvalidConfig)
	}

	var errs []string

	// Reset any previous validation warnings (re-Validate() should not
	// accumulate duplicate findings).
	c.Warnings = nil

	if c.MasterURL == "" {
		errs = append(errs, "master_url is required")
	}

	// RW-PROD-001 A4: canonicalize the worker_id and enforce strict shape.
	// Run AFTER the empty-check so a missing worker_id produces the more
	// helpful "worker_id is required" error rather than a cryptic regex miss.
	c.WorkerID = NormalizeWorkerID(c.WorkerID)
	if c.WorkerID == "" {
		errs = append(errs, "worker_id is required")
	} else if !identity.IsValidWorkerID(c.WorkerID) {
		// Caller (cmd/velox-worker-agent/main.go) logs via
		// logger.LogCertRejected once validation completes; here we record
		// both as an error (production-stop) AND carry the structured
		// reason for downstream emission.
		errs = append(errs, fmt.Sprintf("invalid worker_id shape: %q (RW-PROD-001 A4 enforces ^[a-z][a-z0-9-]{2,62}$)", c.WorkerID))
	}

	if c.WorkDir == "" {
		errs = append(errs, "work_dir is required")
	}

	// Validate log level if set
	validLogLevels := map[string]bool{
		"":      true, // empty is ok, will use default
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.LogLevel] {
		errs = append(errs, fmt.Sprintf("invalid log_level: %s (valid: debug, info, warn, error)", c.LogLevel))
	}

	// Velox uses a gRPC-push-only architecture; control_grpc_url is mandatory.
	if c.ControlGRPCURL == "" {
		errs = append(errs, "control_grpc_url is required")
	}

	// ---- PR 1: TLS combinatorial checks ----
	// Local var named `tlsCfg` to avoid shadowing the imported
	// `crypto/tls` package — bare `tls.` later refers to that package.
	tlsCfg := c.GRPCTLS()

	// Environment gate. Default to "production" so missing declaration is
	// safe-by-default. The user's spec said "ambiente != production" for
	// the insecure dev-flag allow-list — we honour that literally here.
	env := c.Environment
	if env == "" {
		env = "production"
	}
	if env != "dev" && env != "staging" && env != "production" {
		errs = append(errs, fmt.Sprintf(
			"invalid environment: %q (valid: dev, staging, production)", c.Environment))
	}

	// Validate worker profile when set. Only "creator" (case-insensitive)
	// is recognized; an empty value keeps the historical video-worker path.
	if c.WorkerProfile != "" && !c.IsCreatorProfile() {
		errs = append(errs, fmt.Sprintf(
			"invalid worker_profile: %q (valid: %q or empty)", c.WorkerProfile, CreatorProfile))
	}

	hasCert := tlsCfg.CertFile != ""
	hasKey := tlsCfg.KeyFile != ""
	hasCA := tlsCfg.CAFile != ""
	hasFullTLS := hasCert && hasKey && hasCA

	// Rule: TLS AND insecure cannot both be active.
	if hasFullTLS && tlsCfg.AllowInsecureDev {
		errs = append(errs, "tls_cert_file/tls_key_file/tls_ca_file and "+
			"allow_insecure_grpc_dev cannot be active simultaneously")
	}
	// Rule: insecure is forbidden in `production` only. Spec: "ambiente != production".
	if tlsCfg.AllowInsecureDev && env == "production" {
		errs = append(errs, fmt.Sprintf(
			"allow_insecure_grpc_dev=true is only valid in non-production environments (got %q); "+
				"never use insecure gRPC in production", env))
	}
	// Rule: partial TLS is rejected (cert only, cert+key no ca, ca only, etc.).
	if (hasCert || hasKey || hasCA) && !hasFullTLS {
		missing := []string{}
		if !hasCert {
			missing = append(missing, "tls_cert_file")
		}
		if !hasKey {
			missing = append(missing, "tls_key_file")
		}
		if !hasCA {
			missing = append(missing, "tls_ca_file")
		}
		errs = append(errs, fmt.Sprintf(
			"partial TLS configuration: provide all three of tls_cert_file/tls_key_file/tls_ca_file. "+
				"Missing: %s", strings.Join(missing, ", ")))
	}
	// Rule: with no TLS at all, must opt into insecure dev mode.
	if !hasFullTLS && !tlsCfg.AllowInsecureDev {
		errs = append(errs, "no TLS configured and insecure dev flag not enabled. "+
			"Either set tls_cert_file+tls_key_file+tls_ca_file, "+
			"or set allow_insecure_grpc_dev=true with environment=dev|staging")
	}
	// Rule: full TLS means cert + key must be present on disk AND the key
	// must actually pair against the cert (the user's spec listed
	// "chiave non compatibile col certificato" as a test case).
	//
	// RW-PROD-001 additions layered on top of the existing pair-incompat
	// guard, in this exact order:
	//  1. stat the key for permissions (A2) — production hard-fails,
	//     dev/staging records a non-fatal Warning.
	//  2. stat the cert (existence + is-not-directory).
	//  3. LoadX509KeyPair for cert/key compatibility.
	//  4. parse the leaf certificate and reject if (a) expired OR
	//     (b) expires in less than minCertValidity (14d).
	if hasFullTLS {
		// -------- A2: key file permissions enforcement --------
		if runtime.GOOS != "windows" {
			if keyInfo, err := os.Stat(tlsCfg.KeyFile); err == nil {
				// POSIX mode bitmask & 0o077 isolates "group" + "other"
				// rwx bits — anything non-zero is world-or-group readable,
				// which is unsafe for a private key.
				perm := keyInfo.Mode().Perm()
				if perm&0o077 != 0 {
					if env == "production" {
						errs = append(errs, fmt.Sprintf(
							"tls_key_file %q has insecure permissions %04o (must be 0600); "+
								"RW-PROD-001 A2 fail-closed in production",
							tlsCfg.KeyFile, perm))
					} else {
						// Non-production: record a non-fatal Warning so the
						// caller can log via logger.LogCertRejected once
						// validation completes. DO NOT add to errs — we want
						// the worker to start so local development isn't
						// blocked by chmod.
						c.Warnings = append(c.Warnings, fmt.Sprintf(
							"weak_permissions_warn: tls_key_file %q has %04o, must be 0600 (RW-PROD-001 A2)",
							tlsCfg.KeyFile, perm))
					}
				}
			}
			// Stat failures on the key (NotExist, etc.) are reported later
			// by the LoadX509KeyPair guard below — a duplicate check here
			// would mask the more descriptive error.
		}

		// -------- existence + directory check on cert file --------
		if info, err := os.Stat(tlsCfg.CertFile); err != nil {
			if os.IsNotExist(err) {
				errs = append(errs, fmt.Sprintf(
					"tls_cert_file not found at %q", tlsCfg.CertFile))
			} else {
				errs = append(errs, fmt.Sprintf(
					"tls_cert_file inaccessible at %q: %v", tlsCfg.CertFile, err))
			}
		} else if info.IsDir() {
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file %q is a directory, not a PEM file", tlsCfg.CertFile))
		} else if cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile); err != nil {
			// -------- A1/cert-key compat (existing guard) --------
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file / tls_key_file pair rejected by crypto/tls: %v", err))
		} else if leaf, perr := x509.ParseCertificate(cert.Certificate[0]); perr != nil {
			// -------- A1: parse leaf cert --------
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file could not be parsed as x509 leaf: %v", perr))
		} else {
			// -------- A1: expiry window check (14-day floor) --------
			now := time.Now().UTC()
			switch {
			case now.After(leaf.NotAfter):
				errs = append(errs, fmt.Sprintf(
					"certificate has expired: not_after=%s now=%s (RW-PROD-001 A1)",
					leaf.NotAfter.UTC().Format(time.RFC3339), now.Format(time.RFC3339)))
			case leaf.NotAfter.Sub(now) < minCertValidity:
				errs = append(errs, fmt.Sprintf(
					"certificate expires too soon: not_after=%s remaining=%s floor=%s (RW-PROD-001 A1)",
					leaf.NotAfter.UTC().Format(time.RFC3339),
					leaf.NotAfter.Sub(now).Round(time.Second),
					minCertValidity))
			}
		}
	}

	if len(errs) > 0 {
		// Build error message from all validation errors
		errMsg := "validation errors: "
		for i, e := range errs {
			if i > 0 {
				errMsg += "; "
			}
			errMsg += e
		}
		return fmt.Errorf("%w: %s", ErrInvalidConfig, errMsg)
	}

	return nil
}
