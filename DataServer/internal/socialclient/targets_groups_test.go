package socialclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListPublishingCatalogIncludesChannelsAndGroups(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/destinations/resolve-target" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var request PublishingCatalogRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.WorkspaceID != 123 || request.Platform != "youtube" || request.Target.Type != "catalog" {
			t.Fatalf("request = %#v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []PublishingTarget{{
				PlatformAccountID:     381,
				Platform:              "youtube",
				ChannelID:             "UC123",
				ChannelName:           "News Italia",
				ExternalDestinationID: "extdst_channel",
				Status:                "active",
				Enabled:               true,
				CanPost:               true,
				Capabilities:          PublishingCapabilities{UploadVideo: true, Publish: true},
			}},
			ResolvedGroups: []PublishingGroup{{
				GroupID:                27,
				Name:                   "Canali News Italia",
				MemberCount:            8,
				PublishableMemberCount: 8,
				CanPost:                true,
			}},
		})
	}))
	defer server.Close()

	catalog, err := New(Config{BaseURL: server.URL, APIKey: "test-token"}).ListPublishingCatalog(t.Context(), 123, "youtube")
	if err != nil {
		t.Fatalf("ListPublishingCatalog returned error: %v", err)
	}
	if len(catalog.ResolvedTargets) != 1 || len(catalog.ResolvedGroups) != 1 {
		t.Fatalf("catalog = %#v", catalog)
	}
	if catalog.ResolvedGroups[0].GroupID != 27 || catalog.ResolvedGroups[0].PublishableMemberCount != 8 {
		t.Fatalf("group = %#v", catalog.ResolvedGroups[0])
	}
}
