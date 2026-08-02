package main

import (
	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/handlers/server/pipeline"
)

// newM2MJwAuthFromBundle constructs the M2M middleware for the
// /api/v1/jobs group from the RouterBundle. The middleware is
// pure (no hidden state) so the only mutable piece — the
// in-memory token-bucket ledger — is LOCAL to this function and
// dies when the master stops. SQLite is the source of truth for
// credentials; the rate-limit ledger is intentionally in-memory
// for the NoSQLiteLock contention reason documented in
// handlers/server/pipeline/m2m_auth.go.
//
// nil cfg / nil SQLiteStore returns nil — caller treats nil
// m2mAuth as "fall back to adminAuth" so unit-test mounts
// retain compatibility. Production wiring always supplies both.
func newM2MJwAuthFromBundle(cfg *config.Config, deps PipelineRouteDeps) gin.HandlerFunc {
	if cfg == nil || deps.SQLiteStore == nil {
		return nil
	}
	return pipeline.NewM2MJwAuthMiddleware(cfg, deps.SQLiteStore, nil)
}
