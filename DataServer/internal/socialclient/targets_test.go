package socialclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListPublishingTargets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/destinations/resolve-target" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization = %q", got)
		}
		var body PublishingTargetCatalogRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body.WorkspaceID != 12 || body.Platform != "youtube" || body.Target.Type != "catalog" {
			t.Fatalf("request body = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []PublishingTarget{{
				PlatformAccountID:     381,
				Platform:              "youtube",
				ChannelID:             "UCready",
				ChannelName:           "Ready",
				ExternalDestinationID: "extdst_ready",
				Status:                "active",
				Enabled:               true,
				CanPost:               true,
				Capabilities: PublishingCapabilities{
					UploadVideo:  true,
					SetThumbnail: true,
					Publish:      true,
					Schedule:     true,
				},
			}},
		})
	}))
	defer server.Close()

	client := New(Config{BaseURL: server.URL, APIKey: "test-token"})
	catalog, err := client.ListPublishingTargets(t.Context(), 12, "youtube")
	if err != nil {
		t.Fatalf("ListPublishingTargets returned error: %v", err)
	}
	if len(catalog.ResolvedTargets) != 1 || catalog.ResolvedTargets[0].ExternalDestinationID != "extdst_ready" {
		t.Fatalf("catalog = %#v", catalog)
	}
}

func TestListPublishingTargetsRequiresConfiguration(t *testing.T) {
	client := New(Config{})
	if _, err := client.ListPublishingTargets(t.Context(), 12, "youtube"); err == nil {
		t.Fatal("expected not-configured error")
	}
}
