// Package integration_test holds end-to-end integration tests for the
// full social_repo boundary. Unlike the unit tests in:
//
//   - internal/socialclient/client_test.go (httptest + bare Client)
//   - internal/deliveries/providers/social_gateway_test.go (httptest + bare provider)
//   - internal/jobs/enqueue/delivery_plan_validator_test.go (stubValidator)
//
// this package wires the FULL pipeline end-to-end:
//
//	httptest mock social_repo
//	  ↓
//	real socialclient.Client  (New(Config{BaseURL: server.URL}))
//	  ↓
//	  ├─→ Enqueuer.WithSocialValidator(client)
//	  │     → validateDeliveryPlanRequires pre-flight loop
//	  └─→ SocialGatewayProvider registered in deliveries.Registry
//	        → DeliverArtifact path (mocked at the provider boundary,
//	          NOT through DeliveryRunner.Deliver — the runner's
//	          retry/backoff/lease machinery is out of scope here)
//
// The 6 documented scenarios (acceptance / auth / rate-limit / transient
// 5xx / network unreachable / retry idempotency dedup) are exercised on
// BOTH paths, asserting the cross-package error sentinel contract
// end-to-end. The package exists only to host `*_test.go` files and
// imports from every layer above (socialclient → deliveries → enqueue
// → store) so it deliberately lives outside those packages to avoid
// an import cycle.
package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/deliveries"
	"velox-server/internal/deliveries/providers"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// ═══════════════════════════════════════════════════════════════════
// Programmable mock social_repo
// ═══════════════════════════════════════════════════════════════════

// mockSocialRepo is a programmable httptest.Server that returns the 6
// documented social_repo responses based on a per-test `mode`. The
// same server can also be flipped into "dedup" mode to simulate the
// social_repo's server-side idempotency contract: same
// `idempotency_key` ⇒ same `social_delivery_id` across calls.
type mockSocialRepo struct {
	server *httptest.Server

	mu        sync.Mutex
	mode      string // "ok", "auth", "rate_limit", "server_error", "dedup"
	postCount int32

	// dedupCache: idempotency_key → social_delivery_id (used in "dedup"
	// mode so the runner's retry loop observes the SAME
	// social_delivery_id on every attempt).
	dedupCache map[string]string

	// stableRemoteID is the fixed social_delivery_id returned for the
	// "dedup" mode (and for the "ok" mode unless the caller overrides).
	stableRemoteID string
}

func newMockSocialRepo(t *testing.T, mode string) *mockSocialRepo {
	t.Helper()
	m := &mockSocialRepo{
		mode:           mode,
		stableRemoteID: "social_" + mode + "_remote_id",
		dedupCache:     make(map[string]string),
	}
	m.server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockSocialRepo) URL() string { return m.server.URL }

// setMode flips the response mode mid-test (useful for the dedup test
// that exercises success → success → transient → success).
func (m *mockSocialRepo) setMode(mode string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode = mode
}

func (m *mockSocialRepo) handle(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&m.postCount, 1)
	raw, _ := io.ReadAll(r.Body)
	idemKey := sniffJSONString(raw, "idempotency_key")

	m.mu.Lock()
	mode := m.mode
	cache := m.dedupCache
	stableID := m.stableRemoteID
	m.mu.Unlock()

	switch mode {
	case "auth":
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case "rate_limit":
		http.Error(w, "slow down", http.StatusTooManyRequests)
	case "server_error":
		http.Error(w, "downstream unavailable", http.StatusInternalServerError)
	case "dedup":
		// Server-side dedup: same idempotency_key ⇒ same social_delivery_id.
		if id, ok := cache[idemKey]; ok {
			writeJSON(w, http.StatusOK, map[string]string{"social_delivery_id": id, "status": "idempotent"})
			return
		}
		id := stableID
		cache[idemKey] = id
		writeJSON(w, http.StatusOK, map[string]string{"social_delivery_id": id, "status": "accepted"})
	default: // "ok"
		writeJSON(w, http.StatusOK, map[string]string{"social_delivery_id": stableID, "status": "accepted"})
	}
}

func (m *mockSocialRepo) postCount32() int32 { return atomic.LoadInt32(&m.postCount) }

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// sniffJSONString extracts a top-level string field from raw JSON
// without bringing in a full JSON dep. Cheap regex-ish scan is fine
// for these tests because the payload shape is fixed (socialclient
// produces deterministic JSON).
func sniffJSONString(raw []byte, key string) string {
	needle := []byte(`"` + key + `":"`)
	i := strings.Index(string(raw), string(needle))
	if i < 0 {
		return ""
	}
	rest := raw[i+len(needle):]
	j := 0
	for j < len(rest) && rest[j] != '"' {
		j++
	}
	return string(rest[:j])
}

// ═══════════════════════════════════════════════════════════════════
// Enqueuer scaffolding
// ═══════════════════════════════════════════════════════════════════

// testPlanResolver is a no-op PlanResolver. PrepareJobAndTask calls
// validateDeliveryPlanRequires before any PlanResolver activity, so
// this stub is sufficient for pre-flight integration coverage.
type testPlanResolver struct{}

func (testPlanResolver) ResolvePlan(ctx context.Context, jobID, artifactID string) (*enqueue.ResolvedPlan, error) {
	return &enqueue.ResolvedPlan{JobID: jobID}, nil
}

// newEnqueuerWithValidator builds a real Enqueuer backed by a temp-dir
// SQLite store, with the supplied socialclient.Client wired as the
// social destination validator.
func newEnqueuerWithValidator(t *testing.T, validator enqueue.DestinationValidator) *enqueue.Enqueuer {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	creator := store.NewAtomicJobTaskCreator(db)
	enq := enqueue.NewEnqueuer(creator, nil, nil, testPlanResolver{})
	enq.WithSocialValidator(validator)
	return enq
}

// payloadWithExternalDestination returns a minimal valid enqueue
// payload containing one delivery_plan entry whose
// external_destination_id (canonical, post-Residuo-4 rename) the
// pre-flight loop will validate through the supplied client.
//
// Residuo 4: the operator-facing payload key is now
// `external_destination_id`. The legacy `social_destination_id` key
// is still honored by `shapeFromMap` (deprecated alias path) — this
// helper uses the canonical key exclusively so the canonical-only
// contract is exercised end-to-end on the integration path.
func payloadWithExternalDestination(externalDestID string) map[string]any {
	return map[string]any{
		"video_name":     "integration-test",
		"script_text":    "test script",
		"voiceover_path": "/tmp/voiceover.mp3",
		"scenes": []any{
			map[string]any{"text": "scene 1", "image_link": "https://example.com/img.png"},
		},
		"delivery_plan": []any{
			map[string]any{
				"destination_id":          "velox-dest-1",
				"external_destination_id": externalDestID,
				"platform":                "youtube",
				"retry_budget":            3,
			},
		},
	}
}

// ═══════════════════════════════════════════════════════════════════
// ENQUEUE PRE-FLIGHT PATH (Enqueuer.WithSocialValidator →
// validateDeliveryPlanRequires per-entry loop)
// ═══════════════════════════════════════════════════════════════════

// TestIntegration_Preflight_Acceptance asserts the happy path: a 2xx
// response from the mock social_repo lets the Enqueuer accept the
// payload and PrepareJobAndTask returns without error.
func TestIntegration_Preflight_Acceptance(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "ok")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})

	enq := newEnqueuerWithValidator(t, client)
	_, _, _, err := enq.PrepareJobAndTask(
		context.Background(),
		payloadWithExternalDestination("external_dest_integration_ok"),
		costmodel.DefaultRequirements(),
	)
	if err != nil {
		t.Fatalf("2xx acceptance must pass pre-flight; got %v", err)
	}
}

// TestIntegration_Preflight_AuthError asserts that HTTP 401 from the
// mock social_repo surfaces as a HARD failure: validateDeliveryPlanRequires
// returns a *validationError wrapping socialclient.ErrAuth (errors.Is
// must surface both).
func TestIntegration_Preflight_AuthError(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "auth")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})

	enq := newEnqueuerWithValidator(t, client)
	_, _, _, err := enq.PrepareJobAndTask(
		context.Background(),
		payloadWithExternalDestination("external_dest_integration_auth"),
		costmodel.DefaultRequirements(),
	)
	if err == nil {
		t.Fatal("HTTP 401 must hard-fail pre-flight; got nil error")
	}
	if !errors.Is(err, socialclient.ErrAuth) {
		t.Fatalf("errors.Is must surface ErrAuth; got %v", err)
	}
	// Field path: must be the structured external_destination_id field
	// (canonical, post-Residuo-4 rename). Cross-package access goes
	// through the enqueue.ValidationErrorField helper so the test does
	// not need to import the unexported *validationError type.
	if got := enqueue.ValidationErrorField(err); got != "delivery_plan[0].external_destination_id" {
		t.Errorf("ValidationErrorField = %q; want delivery_plan[0].external_destination_id", got)
	}
}

// TestIntegration_Preflight_RateLimit asserts that HTTP 429 from the
// mock social_repo surfaces as a SOFT pass: validateDeliveryPlanRequires
// returns nil (enqueue continues; the runner's retry budget is the
// recovery path).
func TestIntegration_Preflight_RateLimit(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "rate_limit")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})

	enq := newEnqueuerWithValidator(t, client)
	_, _, _, err := enq.PrepareJobAndTask(
		context.Background(),
		payloadWithExternalDestination("external_dest_integration_rl"),
		costmodel.DefaultRequirements(),
	)
	if err != nil {
		t.Fatalf("HTTP 429 must soft-pass pre-flight (ErrRateLimit logs + continues); got %v", err)
	}
}

// TestIntegration_Preflight_ServerError asserts that HTTP 5xx surfaces
// as SOFT pass (ErrTransient logs + continues).
func TestIntegration_Preflight_ServerError(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "server_error")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})

	enq := newEnqueuerWithValidator(t, client)
	_, _, _, err := enq.PrepareJobAndTask(
		context.Background(),
		payloadWithExternalDestination("external_dest_integration_5xx"),
		costmodel.DefaultRequirements(),
	)
	if err != nil {
		t.Fatalf("HTTP 5xx must soft-pass pre-flight (ErrTransient logs + continues); got %v", err)
	}
}

// TestIntegration_Preflight_Unreachable asserts that a network error
// (server not listening) surfaces as SOFT pass (ErrTransient via the
// dial failure path).
func TestIntegration_Preflight_Unreachable(t *testing.T) {
	t.Parallel()
	// Bind a listener on an ephemeral port, then close it. Every dial
	// returns ECONNREFUSED — the realistic shape of a social_repo
	// outage that the runner would later retry against.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadURL := "http://" + ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	client := socialclient.New(socialclient.Config{BaseURL: deadURL, Timeout: 200 * time.Millisecond})
	enq := newEnqueuerWithValidator(t, client)
	_, _, _, err = enq.PrepareJobAndTask(
		context.Background(),
		payloadWithExternalDestination("external_dest_integration_unreachable"),
		costmodel.DefaultRequirements(),
	)
	if err != nil {
		t.Fatalf("network error must soft-pass pre-flight (ErrTransient logs + continues); got %v", err)
	}
}

// TestIntegration_Preflight_RetryIdempotency asserts that the mock
// returns 200 + social_delivery_id on the FIRST pre-flight call, so
// the runner's retry loop is built on a stable observation (the
// pre-flight does NOT itself retry — the runner does, post-finalize).
func TestIntegration_Preflight_RetryIdempotency(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "dedup")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})

	enq := newEnqueuerWithValidator(t, client)
	for i := 0; i < 3; i++ {
		_, _, _, err := enq.PrepareJobAndTask(
			context.Background(),
			payloadWithExternalDestination("external_dest_integration_idem"),
			costmodel.DefaultRequirements(),
		)
		if err != nil {
			t.Fatalf("attempt %d: pre-flight must succeed under dedup; got %v", i, err)
		}
	}
	// Three PrepareJobAndTask calls with the SAME external_destination_id
	// produce three ValidateDestination POSTs against the mock — each
	// call MUST NOT mutate the pre-flight outcome (the pre-flight loop
	// is single-attempt by design).
	if got := mock.postCount32(); got != 3 {
		t.Errorf("want 3 pre-flight POSTs (one per PrepareJobAndTask), got %d", got)
	}
}

// ═══════════════════════════════════════════════════════════════════
// DELIVERYRUNNER.DeliverArtifact PATH (SocialGatewayProvider
// registered in deliveries.Registry)
// ═══════════════════════════════════════════════════════════════════

// newRegistryWithSocialProvider builds a real deliveries.Registry
// containing a SocialGatewayProvider wired to the supplied socialclient
// (which itself points at the mock).
func newRegistryWithSocialProvider(t *testing.T, client *socialclient.Client) (*deliveries.Registry, *providers.SocialGatewayProvider) {
	t.Helper()
	cfg := socialclient.Config{BaseURL: client.BaseURL(), APIKey: client.BaseURL()} // APIKey unused by mock; inert
	reg := deliveries.NewRegistry()
	p := providers.NewSocialGatewayProvider(cfg)
	reg.Register(p)
	return reg, p
}

// sampleDestination mirrors the deliveries package's Destination shape.
// Opaque-mode (Residuo 3 + Residuo 4 of the YouTube→Social closure):
//   - ChannelID is gone from the typed Destination.
//   - ExternalDestinationID is the canonical opaque identifier the
//     social_repo resolves server-side (residual 4 rename promoted
//     social_destination_id -> external_destination_id).
//   - ConfigurationJSON is operator-facing observability only — content is
//     inert in the wire contract (no longer parsed for platform/account_id).
//   - DeliveryMetadataJSON flows through verbatim as `metadata` in the wire.
func sampleDestination() *deliveries.Destination {
	return &deliveries.Destination{
		DestinationID:         "velox-dest-1",
		DeliveryMetadataJSON:  `{"title":"integration test"}`,
		ConfigurationJSON:     "{}",
		ExternalDestinationID: "external_dest_integration_youtube",
	}
}

// sampleArtifact mirrors the store package's Artifact shape (minimal fields).
func sampleArtifact() *store.Artifact {
	return &store.Artifact{ID: "art-integration-1", SHA256: "abc", SizeBytes: 123, MimeType: "video/mp4"}
}

// TestIntegration_DeliverArtifact_Acceptance asserts the 2xx happy path:
// DeliverArtifact returns Result{Success: true, RemoteID: social_delivery_id_xxx}.
func TestIntegration_DeliverArtifact_Acceptance(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "ok")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})
	_, p := newRegistryWithSocialProvider(t, client)

	res, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-int-1", "idem-int-1")
	if err != nil {
		t.Fatalf("2xx must not error; got %v", err)
	}
	if !res.Success {
		t.Fatal("want Success=true on 2xx")
	}
	if res.RemoteID != "social_ok_remote_id" {
		t.Fatalf("want RemoteID=social_ok_remote_id, got %q", res.RemoteID)
	}
}

// TestIntegration_DeliverArtifact_AuthError asserts HTTP 401 → ErrProviderAuth.
func TestIntegration_DeliverArtifact_AuthError(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "auth")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})
	_, p := newRegistryWithSocialProvider(t, client)

	_, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-int-2", "idem-int-2")
	if !errors.Is(err, deliveries.ErrProviderAuth) {
		t.Fatalf("HTTP 401 must surface ErrProviderAuth; got %v", err)
	}
}

// TestIntegration_DeliverArtifact_RateLimit asserts HTTP 429 → ErrProviderRateLimit.
func TestIntegration_DeliverArtifact_RateLimit(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "rate_limit")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})
	_, p := newRegistryWithSocialProvider(t, client)

	_, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-int-3", "idem-int-3")
	if !errors.Is(err, deliveries.ErrProviderRateLimit) {
		t.Fatalf("HTTP 429 must surface ErrProviderRateLimit; got %v", err)
	}
}

// TestIntegration_DeliverArtifact_ServerError asserts HTTP 5xx → ErrProviderTransient.
func TestIntegration_DeliverArtifact_ServerError(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "server_error")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})
	_, p := newRegistryWithSocialProvider(t, client)

	_, err := p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-int-4", "idem-int-4")
	if !errors.Is(err, deliveries.ErrProviderTransient) {
		t.Fatalf("HTTP 5xx must surface ErrProviderTransient; got %v", err)
	}
}

// TestIntegration_DeliverArtifact_Unreachable asserts a network error
// (server not listening) → ErrProviderTransient.
func TestIntegration_DeliverArtifact_Unreachable(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadURL := "http://" + ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	client := socialclient.New(socialclient.Config{BaseURL: deadURL, Timeout: 200 * time.Millisecond})
	_, p := newRegistryWithSocialProvider(t, client)

	_, err = p.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-int-5", "idem-int-5")
	if !errors.Is(err, deliveries.ErrProviderTransient) {
		t.Fatalf("network error must surface ErrProviderTransient; got %v", err)
	}
}

// TestIntegration_DeliverArtifact_RetryIdempotencyStable pins the
// server-side idempotency contract: when the runner retries the same
// (artifact, destination) pair with the same idempotency_key, the
// social_repo returns the SAME social_delivery_id. This is the
// observable "retry does not produce a second delivery" guarantee
// the velocity contract depends on.
func TestIntegration_DeliverArtifact_RetryIdempotencyStable(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "dedup")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})
	_, p := newRegistryWithSocialProvider(t, client)

	artifact := sampleArtifact()
	destination := sampleDestination()
	const stableID = "social_dedup_remote_id"
	const idemKey = "stable-idem-integration-99"

	res1, err := p.Deliver(context.Background(), artifact, destination, "delivery-int-6", idemKey)
	if err != nil {
		t.Fatalf("1st Deliver: %v", err)
	}
	res2, err := p.Deliver(context.Background(), artifact, destination, "delivery-int-6", idemKey)
	if err != nil {
		t.Fatalf("2nd Deliver: %v", err)
	}

	// Both attempts received an HTTP POST (no client-side short-circuit).
	if got := mock.postCount32(); got != 2 {
		t.Errorf("want 2 POSTs received, got %d", got)
	}
	// Both attempts resolved to the SAME social_delivery_id (server-side dedup).
	if res1.RemoteID != stableID {
		t.Errorf("res1.RemoteID = %q; want %q", res1.RemoteID, stableID)
	}
	if res2.RemoteID != stableID {
		t.Errorf("res2.RemoteID = %q; want %q", res2.RemoteID, stableID)
	}
}

// TestIntegration_RegistryContainsSocialGateway asserts the social_gateway
// provider is registered under its canonical name after WithSocialValidator
// + Register wiring. This is the "pushed into the registry" contract the
// user-facing PR-15.9 changelog entry promises.
func TestIntegration_RegistryContainsSocialGateway(t *testing.T) {
	t.Parallel()
	mock := newMockSocialRepo(t, "ok")
	client := socialclient.New(socialclient.Config{BaseURL: mock.URL()})
	reg, _ := newRegistryWithSocialProvider(t, client)

	names := reg.Names()
	found := false
	for _, n := range names {
		if n == "social_gateway" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("registry must contain 'social_gateway'; got %v", names)
	}
}
