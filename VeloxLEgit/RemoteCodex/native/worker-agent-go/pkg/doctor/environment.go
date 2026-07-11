package doctor

import (
	"context"
	"fmt"

	"velox-worker-agent/pkg/config"
)

// EnvironmentValidator checks production safety invariants:
// - production environment MUST NOT have allow_insecure_grpc_dev
// - production environment requires full TLS triple
// RW-PROD-002 §2 item 1.
type EnvironmentValidator struct{}

func (v *EnvironmentValidator) ID() string { return "environment" }

func (v *EnvironmentValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	env := cfg.Environment
	if env == "" {
		env = "production"
	}

	tls := cfg.GRPCTLS()
	hasCert := trim(tls.CertFile) != ""
	hasKey := trim(tls.KeyFile) != ""
	hasCA := trim(tls.CAFile) != ""
	hasFullTLS := hasCert && hasKey && hasCA

	if env == "production" && tls.AllowInsecureDev {
		return fail("environment",
			fmt.Sprintf("environment=%q with allow_insecure_grpc_dev=true is forbidden", env),
			"remove allow_insecure_grpc_dev from production config, or switch to environment=staging")
	}
	if env == "production" && !hasFullTLS {
		return fail("environment",
			fmt.Sprintf("environment=%q requires full TLS triple (tls_cert_file + tls_key_file + tls_ca_file)", env),
			"set VELOX_GRPC_TLS_CERT_FILE, VELOX_GRPC_TLS_KEY_FILE, VELOX_GRPC_TLS_CA_FILE env vars or add them to worker_config.json")
	}
	return pass("environment",
		fmt.Sprintf("environment=%q, TLS triple=%v, insecure=%v", env, hasFullTLS, tls.AllowInsecureDev))
}
