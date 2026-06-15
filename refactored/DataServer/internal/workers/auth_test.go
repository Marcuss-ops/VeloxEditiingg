package workers

import "testing"

func TestAuthorizeWorkerTokenAllowsMissingToken(t *testing.T) {
	tm := NewTokenManager()

	if !AuthorizeWorkerToken(tm, "", "worker-1", "203.0.113.10") {
		t.Fatal("expected missing token to be allowed for non-local worker")
	}
}

func TestAuthorizeWorkerTokenAllowsUnknownTokenAfterRestart(t *testing.T) {
	tm := NewTokenManager()

	if !AuthorizeWorkerToken(tm, "stale-token", "worker-1", "203.0.113.10") {
		t.Fatal("expected unknown token to be tolerated for non-local worker")
	}
}

func TestAuthorizeWorkerTokenRejectsMissingWorkerID(t *testing.T) {
	if AuthorizeWorkerToken(NewTokenManager(), "anything", "", "203.0.113.10") {
		t.Fatal("expected empty worker id to be rejected")
	}
}
