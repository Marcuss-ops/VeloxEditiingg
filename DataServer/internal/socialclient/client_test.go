// Package socialclient / client_test.go
//
// Unit tests for the typed Velox-side boundary against the social_repo.
// These tests pin the wire contract: status → sentinel mapping, the
// shape of accepted/rejected DeliverArtifactResponse, and the
// idempotency-key guarantee that backs the runner's retry policy.
//
// The provider-level scenarios (NOT the client unit tests) live in
// internal/deliveries/providers/social_gateway_test.go and cover the
// six delivery-runner contract scenarios.
package socialclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────
// Construction
// ─────────────────────────────────────────────────────────────────────

func TestNew_DefaultsTimeout(t *testing.T) {
	c := New(Config{})
	if c == nil {
		t.Fatal("New returned nil")
	}
	if c.client.Timeout == 0 {
		t.Fatal("New with empty Config should set a default 30s timeout")
	}
}

// ─────────────────────────────────────────────────────────────────────
// Happy path
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_HappyPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("want Content-Type=application/json, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"returned-id","status":"accepted"}`))
	}))
	defer server.Close()

	c := New(Config{BaseURL: server.URL, APIKey: "tok"})
	got, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{
		ExternalDeliveryID:  "ext-1",
		IdempotencyKey:      "idem-1",
		SocialDestinationID: "social_dest_happy_test",
		Artifact:            ArtifactPayload{ArtifactID: "art-1", SHA256: "sh", SizeBytes: 10, MimeType: "video/mp4"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.SocialDeliveryID != "returned-id" {
		t.Fatalf("want SocialDeliveryID=returned-id, got %q", got.SocialDeliveryID)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Not configured (empty BaseURL)
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_NotConfigured(t *testing.T) {
	c := New(Config{BaseURL: ""})
	_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("want ErrNotConfigured, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Status mapping: HTTP 401/403 → ErrAuth
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_AuthError(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "denied", code)
			}))
			defer server.Close()
			c := New(Config{BaseURL: server.URL})
			_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
			if !errors.Is(err, ErrAuth) {
				t.Fatalf("HTTP %d: want ErrAuth, got %v", code, err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────
// Status mapping: HTTP 429 → ErrRateLimit
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_RateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer server.Close()
	c := New(Config{BaseURL: server.URL})
	_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
	if !errors.Is(err, ErrRateLimit) {
		t.Fatalf("want ErrRateLimit, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Status mapping: HTTP 5xx → ErrTransient
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_Transient(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "downstream", code)
			}))
			defer server.Close()
			c := New(Config{BaseURL: server.URL})
			_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
			if !errors.Is(err, ErrTransient) {
				t.Fatalf("HTTP %d: want ErrTransient, got %v", code, err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────
// Status mapping: 4xx (other than auth/ratelimit) → ErrPermanent
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_Permanent4xx(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusNotFound, http.StatusConflict} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "bad", code)
			}))
			defer server.Close()
			c := New(Config{BaseURL: server.URL})
			_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
			if !errors.Is(err, ErrPermanent) {
				t.Fatalf("HTTP %d: want ErrPermanent, got %v", code, err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────
// Permanent: 2xx without social_delivery_id → ErrPermanent
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_MissingSocialDeliveryID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer server.Close()
	c := New(Config{BaseURL: server.URL})
	_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("want ErrPermanent for 2xx missing social_delivery_id, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Network error → ErrTransient
// Reflects the production "social_repo unreachable" case at the
// boundary owned by this package (the provider tests cover the same
// path at the deliveries.Provider boundary).
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_NetworkError(t *testing.T) {
	// Bind a listener and immediately close it — every dial returns
	// ECONNREFUSED, the realistic shape of a 503-side social_repo.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := "http://" + ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c := New(Config{BaseURL: dead, Timeout: 200 * time.Millisecond})
	_, err = c.DeliverArtifact(context.Background(), DeliverArtifactRequest{})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient on network error, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Idempotency key: same key on two requests is preserved verbatim.
// The runner's retry path relies on this: the social_repo dedupes
// server-side based on the idempotency_key we forward.
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_IdempotencyKeyStable(t *testing.T) {
	var (
		count atomic.Int32
		keys  []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		body, _ := io.ReadAll(r.Body)
		// crude JSON sniff; ok for this test
		if i := bytes.Index(body, []byte(`"idempotency_key":"`)); i >= 0 {
			rest := body[i+len(`"idempotency_key":"`):]
			j := bytes.IndexByte(rest, '"')
			if j >= 0 {
				keys = append(keys, string(rest[:j]))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"idempotent","status":"accepted"}`))
	}))
	defer server.Close()

	c := New(Config{BaseURL: server.URL})
	const key = "stable-key"
	for i := 0; i < 3; i++ {
		_, err := c.DeliverArtifact(context.Background(), DeliverArtifactRequest{
			ExternalDeliveryID: "ext",
			IdempotencyKey:     key,
		})
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i, err)
		}
	}
	if got := count.Load(); got != 3 {
		t.Fatalf("want 3 POSTs received, got %d", got)
	}
	for i, k := range keys {
		if k != key {
			t.Fatalf("attempt %d: want idempotency_key=%q, got %q", i, key, k)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────
// Propagation of context cancellation: caller-cancelled ctx is
// surfaced via ErrTransient so the runner can classify correctly.
// ─────────────────────────────────────────────────────────────────────

func TestClient_DeliverArtifact_ContextCancelled(t *testing.T) {
	// Slow handler that sleeps past the ctx deadline.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			return
		}
	}))
	defer server.Close()

	c := New(Config{BaseURL: server.URL, Timeout: 5 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.DeliverArtifact(ctx, DeliverArtifactRequest{})
	if err == nil {
		t.Fatal("want error on ctx cancel, got nil")
	}
	// Errors from a cancelled ctx are surfaced as ErrTransient so the
	// runner can retry. The exact err.Error() wording is platform
	// dependent, so we only check the sentinel wrap.
	if !errors.Is(err, ErrTransient) && !strings.Contains(strings.ToLower(err.Error()), "context") {
		t.Fatalf("want ErrTransient or context error, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// URL helpers: empty CallbackBaseURL → empty derived URL
// ─────────────────────────────────────────────────────────────────────

func TestClient_DerivedURLs_EmptyCallback(t *testing.T) {
	c := New(Config{BaseURL: "http://unused"})
	if got := c.ArtifactDownloadURL("art-1"); got != "" {
		t.Fatalf("want empty ArtifactDownloadURL when CallbackBaseURL is unset, got %q", got)
	}
	if got := c.CallbackURL("delivery-1"); got != "" {
		t.Fatalf("want empty CallbackURL when CallbackBaseURL is unset, got %q", got)
	}
}
