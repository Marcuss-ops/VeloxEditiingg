package workers

import "testing"

func TestAuthorizeWorkerTokenAllowsMissingToken(t *testing.T) {
	tm := NewTokenManager(nil)

	if !AuthorizeWorkerToken(tm, "", "worker-1", "203.0.113.10") {
		t.Fatal("expected missing token to be allowed for non-local worker")
	}
}

func TestAuthorizeWorkerTokenRejectsUnknownToken(t *testing.T) {
	tm := NewTokenManager(nil)

	if AuthorizeWorkerToken(tm, "stale-token", "worker-1", "203.0.113.10") {
		t.Fatal("expected unknown token to be rejected with nil store")
	}
}

func TestAuthorizeWorkerTokenRejectsMissingWorkerID(t *testing.T) {
	if AuthorizeWorkerToken(NewTokenManager(nil), "anything", "", "203.0.113.10") {
		t.Fatal("expected empty worker id to be rejected")
	}
}

func TestAuthorizeWorkerTokenAllowsLocalIP(t *testing.T) {
	tm := NewTokenManager(nil)

	if !AuthorizeWorkerToken(tm, "", "worker-1", "127.0.0.1") {
		t.Fatal("expected local IP to bypass token check")
	}
}
