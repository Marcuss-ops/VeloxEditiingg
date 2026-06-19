package grpcserver

import (
	"testing"
)

func TestAllowlistAuthorizer_ExactMatch(t *testing.T) {
	a := NewAllowlistAuthorizer("velox-worker-1,velox-worker-local", false)
	if !a.IsAllowed("velox-worker-1") {
		t.Error("expected velox-worker-1 to be allowed")
	}
	if !a.IsAllowed("velox-worker-local") {
		t.Error("expected velox-worker-local to be allowed")
	}
}

func TestAllowlistAuthorizer_NotInList(t *testing.T) {
	a := NewAllowlistAuthorizer("velox-worker-1", false)
	if a.IsAllowed("velox-worker-local") {
		t.Error("expected velox-worker-local to be denied (not in allowlist)")
	}
	if a.IsAllowed("unknown-worker") {
		t.Error("expected unknown-worker to be denied")
	}
}

func TestAllowlistAuthorizer_EmptyWorkerID(t *testing.T) {
	a := NewAllowlistAuthorizer("velox-worker-1", false)
	if a.IsAllowed("") {
		t.Error("expected empty worker ID to be denied")
	}
	if a.IsAllowed("   ") {
		t.Error("expected whitespace worker ID to be denied")
	}
}

func TestAllowlistAuthorizer_EmptyListDev(t *testing.T) {
	a := NewAllowlistAuthorizer("", true) // insecure dev, empty allowlist
	if !a.IsAllowed("any-worker") {
		t.Error("expected any worker to be allowed when allowlist is empty in dev mode")
	}
	if !a.IsAllowed("random-worker-42") {
		t.Error("expected any worker to be allowed when allowlist is empty in dev mode")
	}
}

func TestAllowlistAuthorizer_EmptyListProd(t *testing.T) {
	a := NewAllowlistAuthorizer("", false) // production, empty allowlist
	if a.IsAllowed("any-worker") {
		t.Error("expected worker to be denied when allowlist is empty in production mode")
	}
}

func TestAllowlistAuthorizer_StarWildcard(t *testing.T) {
	a := NewAllowlistAuthorizer("*", false)
	// * is treated as empty list (same behavior).
	if a.IsAllowed("any-worker") {
		t.Error("expected * wildcard to be treated as empty list → denied in production")
	}
}

func TestAllowlistAuthorizer_WhitespaceTrimming(t *testing.T) {
	a := NewAllowlistAuthorizer("  velox-worker-1 , velox-worker-local  ", false)
	if !a.IsAllowed("velox-worker-1") {
		t.Error("expected velox-worker-1 to be allowed (whitespace in CSV)")
	}
	if !a.IsAllowed("velox-worker-local") {
		t.Error("expected velox-worker-local to be allowed (whitespace in CSV)")
	}
}

func TestAllowlistAuthorizer_InsecureDoesNotBypassAllowlist(t *testing.T) {
	// Even in insecure dev, a non-empty allowlist MUST be enforced.
	// mTLS valid but worker not in ACL → denied.
	a := NewAllowlistAuthorizer("velox-worker-1", true) // insecure dev, explicit allowlist
	if !a.IsAllowed("velox-worker-1") {
		t.Error("expected velox-worker-1 to be allowed (explicitly in allowlist)")
	}
	if a.IsAllowed("velox-worker-local") {
		t.Error("expected velox-worker-local to be denied (not in allowlist, even in insecure dev)")
	}
}

func TestValidateWorkerAllowlist_NonEmpty(t *testing.T) {
	if err := ValidateWorkerAllowlist("velox-worker-1", false); err != nil {
		t.Errorf("expected no error for non-empty allowlist, got: %v", err)
	}
	if err := ValidateWorkerAllowlist("velox-worker-1", true); err != nil {
		t.Errorf("expected no error for non-empty allowlist (dev), got: %v", err)
	}
}

func TestValidateWorkerAllowlist_EmptyDev(t *testing.T) {
	if err := ValidateWorkerAllowlist("", true); err != nil {
		t.Errorf("expected no error for empty allowlist in dev, got: %v", err)
	}
	if err := ValidateWorkerAllowlist("*", true); err != nil {
		t.Errorf("expected no error for * wildcard in dev, got: %v", err)
	}
}

func TestValidateWorkerAllowlist_EmptyProd(t *testing.T) {
	if err := ValidateWorkerAllowlist("", false); err == nil {
		t.Error("expected error for empty allowlist in production")
	}
	if err := ValidateWorkerAllowlist("*", false); err == nil {
		t.Error("expected error for * wildcard in production")
	}
}
