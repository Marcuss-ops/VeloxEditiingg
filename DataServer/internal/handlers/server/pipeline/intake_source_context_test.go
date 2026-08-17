package pipeline

import (
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
)

// TestIntakeSourceFromContext_DefaultCanonical verifies that a request with
// no stamped intake source resolves to the canonical default (the plain
// POST /api/v1/jobs path).
func TestIntakeSourceFromContext_DefaultCanonical(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	if got := IntakeSourceFromContext(c); got != creatorflow.IntakeSourceCanonical {
		t.Fatalf("IntakeSourceFromContext = %q, want %q", got, creatorflow.IntakeSourceCanonical)
	}
}

// TestSetAndGetIntakeSource_RoundTrip verifies a stamped source is returned.
func TestSetAndGetIntakeSource_RoundTrip(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	SetIntakeSource(c, creatorflow.IntakeSourceBatch)
	if got := IntakeSourceFromContext(c); got != creatorflow.IntakeSourceBatch {
		t.Fatalf("IntakeSourceFromContext = %q, want %q", got, creatorflow.IntakeSourceBatch)
	}
}

// TestIntakeSourceFromContext_EmptyStampedFallsBack verifies that an empty
// stamped value falls back to the canonical default (defensive).
func TestIntakeSourceFromContext_EmptyStampedFallsBack(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	SetIntakeSource(c, "   ")
	if got := IntakeSourceFromContext(c); got != creatorflow.IntakeSourceCanonical {
		t.Fatalf("IntakeSourceFromContext = %q, want %q", got, creatorflow.IntakeSourceCanonical)
	}
}

// TestIntakeSourceFromContext_NilContext verifies the defensive nil path.
func TestIntakeSourceFromContext_NilContext(t *testing.T) {
	if got := IntakeSourceFromContext(nil); got != creatorflow.IntakeSourceCanonical {
		t.Fatalf("IntakeSourceFromContext(nil) = %q, want %q", got, creatorflow.IntakeSourceCanonical)
	}
}
