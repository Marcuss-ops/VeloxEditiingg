package pipeline

import (
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/m2mkeys"
)

// TestBatchIntakeIdentityAttributesBatchSurface pins the intake-source
// attribution for the batch envelope.
//
// The batch surface used to reach the canonical submitter by cloning the
// gin.Context and stamping the intake source on the CLONE before replaying the
// single-job handler. It now builds this identity once per envelope and passes
// it to the intake core directly, so this test is the guard that a batch item
// still reaches the canonical submitter as `intake_source=batch` (and not as
// `canonical`, which would silently move accepted-item counts between series
// and break the per-surface measurement the intake-source metric exists for).
func TestBatchIntakeIdentityAttributesBatchSurface(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)
	c.Set(m2mCtxKeyClientID, "client-42")
	key := &m2mkeys.M2MAPIKey{ClientID: "client-42"}
	c.Set(m2mCtxKeyM2MKey, key)

	identity := batchIntakeIdentity(c)
	if identity.IntakeSource != creatorflow.IntakeSourceBatch {
		t.Fatalf("batch intake source = %q, want %q", identity.IntakeSource, creatorflow.IntakeSourceBatch)
	}
	if identity.ClientID != "client-42" {
		t.Fatalf("batch client id = %q, want client-42", identity.ClientID)
	}
	if identity.Quota != key {
		t.Fatalf("batch quota key = %#v, want the M2M key the middleware resolved", identity.Quota)
	}
}

// TestBatchIntakeIdentityWithoutM2MContext pins the admin-auth fallback mount:
// no M2M middleware ran, so the quota is nil (quota enforcement is skipped, the
// historical semantic) and the source/client id are still well-defined.
func TestBatchIntakeIdentityWithoutM2MContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)

	identity := batchIntakeIdentity(c)
	if identity.IntakeSource != creatorflow.IntakeSourceBatch {
		t.Fatalf("batch intake source = %q, want %q", identity.IntakeSource, creatorflow.IntakeSourceBatch)
	}
	if identity.ClientID != "" {
		t.Fatalf("client id = %q, want empty without M2M middleware", identity.ClientID)
	}
	if identity.Quota != nil {
		t.Fatalf("quota = %#v, want nil without M2M middleware", identity.Quota)
	}
}

// TestBatchIntakeIdentityNilContextNeedsRealContext documents that the helper
// is only ever called from the batch handler, which always has a live
// gin.Context: the identity derivation is transport-boundary code, not core
// logic (the core receives the already-resolved identity).
func TestBatchIntakeIdentityCarriesNoContextDependencyIntoTheCore(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)
	c.Set(m2mCtxKeyClientID, "client-7")

	identity := batchIntakeIdentity(c)
	// The core consumes plain values only: mutating the request context after
	// the identity is built must not change what the core sees.
	c.Set(m2mCtxKeyClientID, "client-mutated")
	if identity.ClientID != "client-7" {
		t.Fatalf("identity resolved lazily from the context: client id = %q, want client-7", identity.ClientID)
	}
}
