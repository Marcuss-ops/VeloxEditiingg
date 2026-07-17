// Package providers / social_gateway_test.go
//
// Test set is intentionally minimal: it covers ONLY the six scenarios
// of the Social-HTTP boundary Velox owns. Implementation details
// (payload shape, header semantics, omitted-fields contract) live
// instead in the socialclient unit tests and the deliveries/delivery
// runner tests.
//
// Scenarios (see the per-task test names for the canonical mapping):
//  1. Social API accepts delivery → TestSocialGatewayProvider_SocialAcceptsDelivery
//  2. Social API returns auth error → TestSocialGatewayProvider_SocialReturnsAuthError
//  3. Social API returns rate limit → TestSocialGatewayProvider_SocialReturnsRateLimit
//  4. Social API returns remote media ID → TestSocialGatewayProvider_ReturnsRemoteMediaID
//  5. Social API unreachable → TestSocialGatewayProvider_SocialUnreachable
//  6. Retry does not produce a second delivery → TestSocialGatewayProvider_RetryIdempotencyStable
package providers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/deliveries"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// ─────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────

// newLiveProviderForServer builds a provider whose socialclient BaseURL
// points at a running httptest server. Sets + clears env vars so tests
// do not race against process-wide state.
func newLiveProviderForServer(t *testing.T, baseURL, callbackBase, apiKey string) *SocialGatewayProvider {
	t.Helper()
	t.Setenv("SOCIAL_API_URL", baseURL)
	t.Setenv("SOCIAL_GATEWAY_URL", baseURL)
	if callbackBase != "" {
		t.Setenv("SOCIAL_CALLBACK_BASE_URL", callbackBase)
		t.Setenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL", callbackBase)
	} else {
		t.Setenv("SOCIAL_CALLBACK_BASE_URL", "")
		t.Setenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL", "")
	}
	if apiKey != "" {
		t.Setenv("SOCIAL_API_TOKEN", apiKey)
		t.Setenv("SOCIAL_GATEWAY_API_KEY", apiKey)
	} else {
		t.Setenv("SOCIAL_API_TOKEN", "")
		t.Setenv("SOCIAL_GATEWAY_API_KEY", "")
	}
	cfg := socialclient.ConfigFromEnv()
	return NewSocialGatewayProvider(cfg)
}

func sampleArtifact() *store.Artifact {
	return &store.Artifact{ID: "art-1", SHA256: "abc", SizeBytes: 123, MimeType: "video/mp4"}
}

func sampleDestination() *deliveries.Destination {
	return &deliveries.Destination{
		DestinationID:     "dest-1",
		DeliveryMetadataJSON: `{"title":"hello"}`,
		ConfigurationJSON:    `{"platform":"youtube","account_id":"acc_1"}`,
		ChannelID:            "UC_chan_1",
	}
}

// ─────────────────────────────────────────────────────────────────────
// Scenario 1 — Social API accepts delivery (HTTP 2xx with social_delivery_id)
// ─────────────────────────────────────────────────────────────────────

func TestSocialGatewayProvider_SocialAcceptsDelivery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"social_delivery_id":"social_accepted_1","status":"accepted"}`))
	}))
	defer server.Close()

	p := newLiveProviderForServer(t, server.URL, "", "")
	res, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-1", "idem-1")
	if err != nil {
		t.Fatalf("unexpected error on 2xx: %v", err)
	}
	if !res.Success {
		t.Fatal("want Success=true on HTTP 2xx acceptance")
	}
	if res.RemoteID != "social_accepted_1" {
		t.Fatalf("want RemoteID=social_accepted_1, got %q", res.RemoteID)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Scenario 2 — Social API returns auth error (HTTP 401/403)
// The provider must surface deliveries.ErrProviderAuth so the runner
// blocks the destination with BLOCKED_AUTH without retrying.
// ─────────────────────────────────────────────────────────────────────

func TestSocialGatewayProvider_SocialReturnsAuthError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	p := newLiveProviderForServer(t, server.URL, "", "secret-token")
	_, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-2", "idem-2")
	if !errors.Is(err, deliveries.ErrProviderAuth) {
		t.Fatalf("want ErrProviderAuth (BLOCKED_AUTH), got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Scenario 3 — Social API returns rate limit (HTTP 429)
// The provider must surface deliveries.ErrProviderRateLimit so the
// runner applies retry-with-backoff using the retry budget.
// ─────────────────────────────────────────────────────────────────────

func TestSocialGatewayProvider_SocialReturnsRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	p := newLiveProviderForServer(t, server.URL, "", "")
	_, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-3", "idem-3")
	if !errors.Is(err, deliveries.ErrProviderRateLimit) {
		t.Fatalf("want ErrProviderRateLimit, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Scenario 4 — Social API returns remote media ID
// When the social_repo accepts publish, it returns the canonical
// `social_delivery_id` we persist on `job_deliveries.remote_id`. This
// test pins the field name + that the provider exposes it as Result.RemoteID.
// ─────────────────────────────────────────────────────────────────────

func TestSocialGatewayProvider_ReturnsRemoteMediaID(t *testing.T) {
	const wantRemoteID = "media_remote_42"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"` + wantRemoteID + `","status":"published"}`))
	}))
	defer server.Close()

	p := newLiveProviderForServer(t, server.URL, "https://velox.internal", "")
	res, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-4", "idem-4")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RemoteID != wantRemoteID {
		t.Fatalf("want RemoteID=%q, got %q", wantRemoteID, res.RemoteID)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Scenario 5 — Social API unreachable (network / timeout / DNS failure)
// The provider must surface deliveries.ErrProviderTransient so the
// runner applies the transient retry budget. We simulate this by
// pointing at a TCP listener that we immediately close so any dial
// attempt is rejected with ECONNREFUSED — the realistic shape of a
// social_repo that is down vs. an internal server error (covered in
// socialclient unit tests with explicit 5xx).
// ─────────────────────────────────────────────────────────────────────

func TestSocialGatewayProvider_SocialUnreachable(t *testing.T) {
	// Bind a listener on an ephemeral port, then close it. The port is
	// now free and unbound — every dial to it returns ECONNREFUSED,
	// which mirrors the production "social_repo is down" case.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadURL := "http://" + ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	// Use a short client timeout so the test runs quickly even if the
	// dial itself doesn't immediately refuse on this OS.
	cfg := socialclient.Config{
		BaseURL: deadURL,
		Timeout: 200 * time.Millisecond,
	}
	p := NewSocialGatewayProvider(cfg)

	_, err = p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-5", "idem-5")
	if !errors.Is(err, deliveries.ErrProviderTransient) {
		t.Fatalf("want ErrProviderTransient when social_repo unreachable, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Scenario 6 — Retry does not produce a second delivery
//
// The Velocity-side guarantee is: when the runner retries a delivery
// (after a transient error from the Social API), it must not produce
// a fresh `social_delivery_id` for the same (artifact, destination)
// pair. The contract is enforced by the provider's idempotency_key:
// the runner passes the SAME idempotency_key on each retry attempt,
// and the social_repo dedupes server-side based on it.
//
// This test pins the contract at the provider boundary by counting
// how many POSTs the social_repo receives when Deliver is called
// twice with the same idempotency_key. The mock server responds
// with the SAME `social_delivery_id` both times — which is exactly
// what a real social_repo does under idempotent submission.
// ─────────────────────────────────────────────────────────────────────

func TestSocialGatewayProvider_RetryIdempotencyStable(t *testing.T) {
	const stableRemoteID = "social_stable_99"

	var (
		postCount    atomic.Int32
		seenIdemKeys []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		postCount.Add(1)

		// Inline JSON sniff on `idempotency_key` (same pattern as
		// client_test.go's IdempotencyKeyStable). We avoid pulling in
		// a shared helper so the test stays self-contained.
		raw, _ := io.ReadAll(r.Body)
		var key string
		if i := bytes.Index(raw, []byte(`"idempotency_key":"`)); i >= 0 {
			rest := raw[i+len(`"idempotency_key":"`):]
			if j := bytes.IndexByte(rest, '"'); j >= 0 {
				key = string(rest[:j])
			}
		}
		seenIdemKeys = append(seenIdemKeys, key)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"` + stableRemoteID + `","status":"idempotent"}`))
	}))
	defer server.Close()

	p := newLiveProviderForServer(t, server.URL, "", "")

	artifact := sampleArtifact()
	destination := sampleDestination()
	const idempotencyKey = "stable-idem-99"

	// Simulate two successive Deliver calls with the SAME idempotency
	// key, mirroring the runner's retry-loop pattern. The provider's
	// obligation is to forward the key unchanged on every attempt so
	// the social_repo can dedupe server-side. The mock returns the
	// SAME social_delivery_id both times, which is exactly the
	// observable behavior a real social_repo exhibits when it
	// recognizes the idempotency_key — i.e. retry produces no second
	// logical delivery, even though it produces two HTTP requests.
	res1, err := p.Deliver(context.Background(), artifact, destination, "delivery-6", idempotencyKey)
	if err != nil {
		t.Fatalf("1st Deliver: %v", err)
	}
	res2, err := p.Deliver(context.Background(), artifact, destination, "delivery-6", idempotencyKey)
	if err != nil {
		t.Fatalf("2nd Deliver: %v", err)
	}

	// Contract assertions:
	//   1. Two POSTs received (no client-side short-circuit between
	//      retries — the runner's retry budget is the gate, not the
	//      provider's).
	//   2. Both POSTs carried the SAME idempotency_key (provider is
	//      idempotent-key stable across attempts).
	//   3. Both attempts resolve to the SAME social_delivery_id
	//      (mock-side dedup reflects what the real social_repo does
	//      under idempotent submission → "retry does not produce a
	//      second delivery").
	if got := postCount.Load(); got != 2 {
		t.Fatalf("want 2 POSTs received (one per attempt), got %d", got)
	}
	for i, k := range seenIdemKeys {
		if k != idempotencyKey {
			t.Fatalf("attempt %d: want idempotency_key=%q, got %q", i, idempotencyKey, k)
		}
	}
	if res1.RemoteID != stableRemoteID || res2.RemoteID != stableRemoteID {
		t.Fatalf("want stable remote_id on both attempts, got res1=%q res2=%q", res1.RemoteID, res2.RemoteID)
	}
}
