package providers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/socialclient"
)

func TestSocialGatewayProvider_ReconcileCanonicalResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/deliveries/social-1" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"delivery_id":"social-1","publish_status":"published","thumbnail_status":"applied","youtube_video_id":"yt-1"}`))
	}))
	defer server.Close()

	provider := NewSocialGatewayProvider(socialclient.Config{BaseURL: server.URL})
	result, err := provider.Reconcile(context.Background(), "velox-delivery-1", "social-1")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Success || result.Status != "published" || result.RemoteID != "yt-1" {
		t.Fatalf("result = %+v", result)
	}
	if got := result.ProviderMeta["thumbnail_status"]; got != "applied" {
		t.Fatalf("thumbnail_status metadata = %#v", got)
	}
}

func TestSocialGatewayProvider_ReconcilePublishedWithoutMediaIDIsNotComplete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"delivery_id":"social-1","publish_status":"PUBLISHED","thumbnail_status":"APPLIED"}`))
	}))
	defer server.Close()

	provider := NewSocialGatewayProvider(socialclient.Config{BaseURL: server.URL})
	result, err := provider.Reconcile(context.Background(), "velox-delivery-1", "social-1")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Success {
		t.Fatalf("published response without youtube_video_id must not be terminal: %+v", result)
	}
	if result.Status != "published" || result.RemoteID != "" {
		t.Fatalf("result = %+v", result)
	}
}

func TestSocialGatewayProvider_ReconcileRejectsMismatchedDeliveryID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"delivery_id":"other","publish_status":"published","thumbnail_status":"applied","youtube_video_id":"yt-1"}`))
	}))
	defer server.Close()

	provider := NewSocialGatewayProvider(socialclient.Config{BaseURL: server.URL})
	_, err := provider.Reconcile(context.Background(), "velox-delivery-1", "social-1")
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("want permanent mismatch error, got %v", err)
	}
}
