package providers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

func TestSocialGatewayProvider_Name(t *testing.T) {
	p := NewSocialGatewayProvider(nil)
	if p.Name() != "social_gateway" {
		t.Fatalf("want 'social_gateway', got %q", p.Name())
	}
}

func TestSocialGatewayProvider_NotConfigured(t *testing.T) {
	t.Setenv("SOCIAL_GATEWAY_URL", "")
	p := NewSocialGatewayProvider(nil)
	_, err := p.Deliver(context.Background(), &store.Artifact{}, &deliveries.Destination{}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderNotConfigured) {
		t.Fatalf("want ErrProviderNotConfigured, got %v", err)
	}
}

func TestSocialGatewayProvider_NilArtifact(t *testing.T) {
	t.Setenv("SOCIAL_GATEWAY_URL", "http://unused.example")
	p := NewSocialGatewayProvider(nil)

	_, err := p.Deliver(context.Background(), nil, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("want ErrProviderPermanent, got %v", err)
	}
}

func TestSocialGatewayProvider_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"social_123","status":"accepted"}`))
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	t.Setenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL", "https://velox.internal")

	p := NewSocialGatewayProvider(server.Client())
	artifact := &store.Artifact{ID: "art-1", SHA256: "abc", SizeBytes: 123, MimeType: "video/mp4"}
	dest := &deliveries.Destination{DestinationID: "dest-1", DeliveryMetadataJSON: `{"title":"hello"}`}

	res, err := p.Deliver(context.Background(), artifact, dest, "delivery-1", "idem-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Success {
		t.Fatal("want Success=true")
	}
	if res.RemoteID != "social_123" {
		t.Fatalf("want social_123, got %q", res.RemoteID)
	}
}

func TestSocialGatewayProvider_MissingSocialDeliveryID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	p := NewSocialGatewayProvider(server.Client())

	_, err := p.Deliver(context.Background(), &store.Artifact{ID: "art-1"}, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("want ErrProviderPermanent, got %v", err)
	}
}

func TestSocialGatewayProvider_ClientError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	p := NewSocialGatewayProvider(server.Client())

	_, err := p.Deliver(context.Background(), &store.Artifact{ID: "art-1"}, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("want ErrProviderPermanent, got %v", err)
	}
}

func TestSocialGatewayProvider_AuthError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	p := NewSocialGatewayProvider(server.Client())

	_, err := p.Deliver(context.Background(), &store.Artifact{ID: "art-1"}, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderAuth) {
		t.Fatalf("want ErrProviderAuth, got %v", err)
	}
}

func TestSocialGatewayProvider_RateLimitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	p := NewSocialGatewayProvider(server.Client())

	_, err := p.Deliver(context.Background(), &store.Artifact{ID: "art-1"}, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderRateLimit) {
		t.Fatalf("want ErrProviderRateLimit, got %v", err)
	}
}

func TestSocialGatewayProvider_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	p := NewSocialGatewayProvider(server.Client())

	_, err := p.Deliver(context.Background(), &store.Artifact{ID: "art-1"}, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if !errors.Is(err, deliveries.ErrProviderTransient) {
		t.Fatalf("want ErrProviderTransient, got %v", err)
	}
}

func TestSocialGatewayProvider_AuthorizationHeader(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"social_789","status":"accepted"}`))
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	t.Setenv("SOCIAL_GATEWAY_API_KEY", "secret-token")

	p := NewSocialGatewayProvider(server.Client())
	_, err := p.Deliver(context.Background(), &store.Artifact{ID: "art-1"}, &deliveries.Destination{DestinationID: "dest-1"}, "delivery-1", "idem-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authHeader != "Bearer secret-token" {
		t.Fatalf("want Bearer secret-token, got %q", authHeader)
	}
}

func TestSocialGatewayProvider_OmittedFields(t *testing.T) {
	var captured map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"social_999","status":"accepted"}`))
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	// No callback base URL set.

	p := NewSocialGatewayProvider(server.Client())
	artifact := &store.Artifact{ID: "art-3", SHA256: "ghi", SizeBytes: 789, MimeType: "video/mp4"}
	dest := &deliveries.Destination{DestinationID: "dest-3", DeliveryMetadataJSON: `{"title":"no publish"}`}

	_, err := p.Deliver(context.Background(), artifact, dest, "delivery-3", "idem-3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := captured["callback_url"]; ok {
		t.Fatalf("callback_url should be omitted when callback base URL is empty")
	}
	if _, ok := captured["publish_at"]; ok {
		t.Fatalf("publish_at should be omitted when not present in metadata")
	}
	art, ok := captured["artifact"].(map[string]interface{})
	if !ok {
		t.Fatalf("artifact payload missing")
	}
	if _, ok := art["download_url"]; ok {
		t.Fatalf("download_url should be omitted when callback base URL is empty")
	}
}

func TestSocialGatewayProvider_Payload(t *testing.T) {
	var captured struct {
		IdempotencyKey string                 `json:"idempotency_key"`
		Artifact       map[string]interface{} `json:"artifact"`
		DestinationID  string                 `json:"destination_id"`
		CallbackURL    string                 `json:"callback_url"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"social_delivery_id":"social_456","status":"accepted"}`))
	}))
	defer server.Close()

	t.Setenv("SOCIAL_GATEWAY_URL", server.URL)
	t.Setenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL", "https://velox.internal")

	p := NewSocialGatewayProvider(server.Client())
	artifact := &store.Artifact{ID: "art-2", SHA256: "def", SizeBytes: 456, MimeType: "video/mp4"}
	dest := &deliveries.Destination{DestinationID: "dest-2", DeliveryMetadataJSON: `{"title":"payload test"}`}

	_, err := p.Deliver(context.Background(), artifact, dest, "delivery-2", "idem-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if captured.IdempotencyKey != "idem-2" {
		t.Fatalf("want idempotency_key idem-2, got %q", captured.IdempotencyKey)
	}
	if captured.DestinationID != "dest-2" {
		t.Fatalf("want destination_id dest-2, got %q", captured.DestinationID)
	}
	if captured.Artifact["artifact_id"] != "art-2" {
		t.Fatalf("want artifact_id art-2, got %v", captured.Artifact["artifact_id"])
	}
	if captured.CallbackURL != "https://velox.internal/api/internal/deliveries/delivery-2/callback" {
		t.Fatalf("want callback URL, got %q", captured.CallbackURL)
	}
}
