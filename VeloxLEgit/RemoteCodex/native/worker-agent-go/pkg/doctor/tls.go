package doctor

import (
	"context"
	"fmt"

	"velox-worker-agent/pkg/config"
)

// TransportTLSValidator delegates to the existing config.Validate() for
// TLS combinatorial checks. This validates:
// - cert+key pair compatibility
// - partial TLS rejection (cert without key, key without CA, etc.)
// - insecure+full-TLS mutual exclusion
// RW-PROD-002 §2 item 2.
type TransportTLSValidator struct{}

func (v *TransportTLSValidator) ID() string { return "transport.tls" }

func (v *TransportTLSValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	if err := cfg.Validate(); err != nil {
		return fail("transport.tls",
			fmt.Sprintf("config validation failed: %v", err),
			"fix the TLS configuration in worker_config.json or via VELOX_GRPC_TLS_* env vars")
	}
	return pass("transport.tls", "TLS configuration passes config.Validate()")
}
