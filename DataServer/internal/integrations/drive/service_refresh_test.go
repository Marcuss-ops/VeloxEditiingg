// Package drive / service_refresh_test.go
//
// Unit tests for the double-checked OAuth refresh in Service.getToken.
// The thundering-herd regression (N parallel refresh-token grants,
// last-writer-wins) is pinned by TestGetToken_ConcurrentRefreshHappensOnce:
// refreshTokenFn is a test seam because the production RefreshToken
// hardcodes the Google token endpoint and builds its own HTTP client.
// Run with -race; the test spawns concurrent getToken callers against one
// Service.
package drive

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingRefresh returns a refreshTokenFn that records every invocation and
// blocks briefly so concurrent callers all reach the slow path before the
// winner completes, deterministically widening the race window the fix
// serializes.
func countingRefresh(calls *atomic.Int32) func(context.Context, *OAuth2Config, string) (*Token, error) {
	return func(ctx context.Context, cfg *OAuth2Config, refreshToken string) (*Token, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return &Token{
			AccessToken:  "fresh-access",
			RefreshToken: refreshToken,
			TokenType:    "Bearer",
			Expiry:       time.Now().Add(time.Hour),
			AccountEmail: "ops@example.com",
		}, nil
	}
}

// TestGetToken_ConcurrentRefreshHappensOnce is the core regression test: N
// goroutines racing into the 5-minute refresh window must trigger EXACTLY ONE
// refresh-token grant, and every caller must observe the winner's fresh token.
// Before the mutex-serialized double-check, each goroutine ran its own
// RefreshToken and the last writer silently won.
func TestGetToken_ConcurrentRefreshHappensOnce(t *testing.T) {
	svc := &Service{oauthCfg: &OAuth2Config{}}
	var calls atomic.Int32
	svc.refreshTokenFn = countingRefresh(&calls)

	svc.SetToken(&Token{
		AccessToken:  "expiring-access",
		RefreshToken: "rt-1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(2 * time.Minute), // inside the 5-minute refresh window
		AccountEmail: "ops@example.com",
	})

	const callers = 32
	var wg sync.WaitGroup
	errs := make([]error, callers)
	tokens := make([]*Token, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tokens[i], errs[i] = svc.getToken(context.Background())
		}(i)
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh-token grants = %d, want exactly 1 (thundering herd regression)", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: unexpected error: %v", i, err)
		}
		if tokens[i].AccessToken != "fresh-access" {
			t.Fatalf("caller %d got access token %q, want the winner's %q", i, tokens[i].AccessToken, "fresh-access")
		}
	}
	if tokens[0].Expiry != tokens[callers-1].Expiry {
		t.Fatalf("callers observed different fresh tokens: %v vs %v", tokens[0].Expiry, tokens[callers-1].Expiry)
	}
}

// TestGetToken_NoRefreshWhileTokenValid pins the fast path: a token with more
// than 5 minutes of validity must be returned without any OAuth round-trip.
func TestGetToken_NoRefreshWhileTokenValid(t *testing.T) {
	svc := &Service{oauthCfg: &OAuth2Config{}}
	var calls atomic.Int32
	svc.refreshTokenFn = countingRefresh(&calls)

	svc.SetToken(&Token{
		AccessToken:  "valid-access",
		RefreshToken: "rt-1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
		AccountEmail: "ops@example.com",
	})

	tok, err := svc.getToken(context.Background())
	if err != nil {
		t.Fatalf("getToken: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("refresh-token grants = %d, want 0 for a valid token", got)
	}
	if tok.AccessToken != "valid-access" {
		t.Fatalf("access token = %q, want the stored valid token", tok.AccessToken)
	}
}

// TestGetToken_SecondCallAfterRefreshSkipsRefresh pins that a refresh
// followed by an immediate call (fresh token still valid) performs no second
// grant — the double-check must serve the cached winner.
func TestGetToken_SecondCallAfterRefreshSkipsRefresh(t *testing.T) {
	svc := &Service{oauthCfg: &OAuth2Config{}}
	var calls atomic.Int32
	svc.refreshTokenFn = countingRefresh(&calls)

	svc.SetToken(&Token{
		AccessToken:  "expiring-access",
		RefreshToken: "rt-1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(2 * time.Minute),
		AccountEmail: "ops@example.com",
	})

	if _, err := svc.getToken(context.Background()); err != nil {
		t.Fatalf("first getToken: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh grants after first call = %d, want 1", got)
	}

	if _, err := svc.getToken(context.Background()); err != nil {
		t.Fatalf("second getToken: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh grants after second call = %d, want still 1 (fresh token must be served from cache)", got)
	}
}

// TestGetToken_RefreshErrorPreservesStoredToken pins the fail-closed
// behavior: a failed refresh must surface as ErrNotAuthenticated and must
// NOT clobber or nil the stored token — the next caller can retry against
// the same refresh credential.
func TestGetToken_RefreshErrorPreservesStoredToken(t *testing.T) {
	svc := &Service{oauthCfg: &OAuth2Config{}}
	svc.refreshTokenFn = func(ctx context.Context, cfg *OAuth2Config, refreshToken string) (*Token, error) {
		return nil, errors.New("oauth endpoint unreachable")
	}

	original := &Token{
		AccessToken:  "expiring-access",
		RefreshToken: "rt-1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(2 * time.Minute),
		AccountEmail: "ops@example.com",
	}
	svc.SetToken(original)

	_, err := svc.getToken(context.Background())
	if err == nil || !errors.Is(err, ErrNotAuthenticated) {
		t.Fatalf("getToken() error = %v, want ErrNotAuthenticated", err)
	}

	svc.mu.RLock()
	current := svc.currentToken
	svc.mu.RUnlock()
	if current == nil || current.RefreshToken != "rt-1" || current.AccessToken != "expiring-access" {
		t.Fatalf("stored token after failed refresh = %#v, want unchanged original", current)
	}
}
