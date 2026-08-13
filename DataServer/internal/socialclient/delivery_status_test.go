package socialclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetDelivery_CanonicalResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/v1/deliveries/delivery-1" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"delivery_id":"delivery-1","publish_status":"published","thumbnail_status":"applied","remote_media_id":"yt-1","last_error_code":"","last_error_message":""}`))
	}))
	defer server.Close()

	got, err := New(Config{BaseURL: server.URL}).GetDelivery(context.Background(), "delivery-1")
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if got.DeliveryID != "delivery-1" || got.PublishStatus != "published" || got.ThumbnailStatus != "applied" || got.RemoteMediaID != "yt-1" {
		t.Fatalf("canonical response = %+v", got)
	}
}

func TestClient_GetDelivery_RejectsMissingCanonicalDeliveryID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"publish_status":"published","thumbnail_status":"applied","remote_media_id":"yt-1"}`))
	}))
	defer server.Close()

	if _, err := New(Config{BaseURL: server.URL}).GetDelivery(context.Background(), "delivery-1"); err == nil {
		t.Fatal("response without delivery_id must be rejected")
	}
}

func TestDeliveryStatusResponse_DoesNotDecodeV0Aliases(t *testing.T) {
	var got DeliveryStatusResponse
	if err := json.Unmarshal([]byte(`{"id":"old-id","status":"published","platform_media_id":"old-media","platform_url":"https://old.example"}`), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.DeliveryID != "" || got.PublishStatus != "" || got.ThumbnailStatus != "" || got.RemoteMediaID != "" {
		t.Fatalf("v0 aliases populated canonical response: %+v", got)
	}
}
