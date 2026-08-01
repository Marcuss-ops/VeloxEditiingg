package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/socialclient"
)

func TestListPublishingCatalogReturnsChannelsAndGroups(t *testing.T) {
	gin.SetMode(gin.TestMode)
	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []socialclient.PublishingTarget{{
				PlatformAccountID:     381,
				Platform:              "youtube",
				ChannelID:             "UC123",
				ChannelName:           "News Italia",
				ExternalDestinationID: "extdst_channel",
				Status:                "active",
				Enabled:               true,
				CanPost:               true,
				Capabilities:          socialclient.PublishingCapabilities{UploadVideo: true, Publish: true},
			}},
			ResolvedGroups: []socialclient.PublishingGroup{{
				GroupID:                27,
				Name:                   "Canali News Italia",
				MemberCount:            8,
				PublishableMemberCount: 8,
				CanPost:                true,
			}},
		})
	}))
	defer catalogServer.Close()

	s := openHandlerTestDB(t)
	defer s.Close()
	h := (&Handlers{}).
		WithStore(s).
		WithSocialClient(socialclient.New(socialclient.Config{BaseURL: catalogServer.URL, APIKey: "test-token"}))
	r := gin.New()
	r.POST("/api/v1/publishing/catalog", h.ListPublishingCatalog())

	requestBody := `{"workspace_id":123,"platform":" YouTube "}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/catalog", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var response PublishingCatalogResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.WorkspaceID != 123 || response.Platform != "youtube" {
		t.Fatalf("response scope = %#v", response)
	}
	if len(response.Channels) != 1 || response.Channels[0].Type != "channel" {
		t.Fatalf("channels = %#v", response.Channels)
	}
	if response.Channels[0].DestinationID != "instaedit_extdst_channel" {
		t.Fatalf("destination_id = %q", response.Channels[0].DestinationID)
	}
	if len(response.Groups) != 1 || response.Groups[0].Type != "group" || response.Groups[0].GroupID != 27 {
		t.Fatalf("groups = %#v", response.Groups)
	}

	row, err := s.GetDeliveryDestination(context.Background(), "instaedit_extdst_channel")
	if err != nil || row == nil {
		t.Fatalf("synced channel row = %#v, err = %v", row, err)
	}
	if !row.Enabled || row.ExternalDestinationID != "extdst_channel" {
		t.Fatalf("synced channel row = %#v", row)
	}
}

func TestProjectCatalogChannelsPreservesOpaqueExternalPrefix(t *testing.T) {
	channels, externalIDs, err := projectCatalogChannels([]socialclient.PublishingTarget{{
		PlatformAccountID:     381,
		Platform:              "youtube",
		ChannelID:             "UC-prefix",
		ChannelName:           "Prefix",
		ExternalDestinationID: "instaedit_extdst_nested",
		Status:                "active",
	}}, "youtube")
	if err != nil {
		t.Fatalf("project channels: %v", err)
	}
	if len(channels) != 1 || channels[0].DestinationID != "instaedit_instaedit_extdst_nested" {
		t.Fatalf("channels = %#v", channels)
	}
	if len(externalIDs) != 1 || externalIDs[0] != "instaedit_extdst_nested" {
		t.Fatalf("external IDs = %#v", externalIDs)
	}
}

func TestListPublishingCatalogDeduplicatesGroupAliasAndRejectsInvalidCounts(t *testing.T) {
	parentA := int64(4)
	parentB := int64(4)
	valid := &socialclient.PublishingTargetCatalogResponse{
		ResolvedGroups: []socialclient.PublishingGroup{{GroupID: 27, Name: "News", ParentGroupID: &parentA, MemberCount: 2, PublishableMemberCount: 2}},
		Groups:         []socialclient.PublishingGroup{{GroupID: 27, Name: "News", ParentGroupID: &parentB, MemberCount: 2, PublishableMemberCount: 2}},
	}
	groups, err := projectCatalogGroups(valid)
	if err != nil {
		t.Fatalf("project valid groups: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != "News" {
		t.Fatalf("groups = %#v", groups)
	}

	_, err = projectCatalogGroups(&socialclient.PublishingTargetCatalogResponse{
		ResolvedGroups: []socialclient.PublishingGroup{{GroupID: 27, Name: "News", MemberCount: 2, PublishableMemberCount: 2}},
		Groups:         []socialclient.PublishingGroup{{GroupID: 27, Name: "News duplicate", MemberCount: 2, PublishableMemberCount: 2}},
	})
	if err == nil {
		t.Fatal("expected duplicate group IDs to be rejected")
	}

	_, err = projectCatalogGroups(&socialclient.PublishingTargetCatalogResponse{
		ResolvedGroups: []socialclient.PublishingGroup{{GroupID: 27, MemberCount: 1, PublishableMemberCount: 2}},
	})
	if err == nil {
		t.Fatal("expected invalid member counts to be rejected")
	}
}

func TestListPublishingCatalogRejectsUnknownFieldsAndInvalidPlatform(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := (&Handlers{}).WithSocialClient(socialclient.New(socialclient.Config{}))
	r := gin.New()
	r.POST("/api/v1/publishing/catalog", h.ListPublishingCatalog())

	for _, test := range []struct {
		name string
		body string
		want int
	}{
		{name: "unknown field", body: `{"workspace_id":1,"platform":"youtube","include_groups":true}`, want: http.StatusBadRequest},
		{name: "unsupported platform", body: `{"workspace_id":1,"platform":"tiktok"}`, want: http.StatusUnprocessableEntity},
		{name: "missing platform", body: `{"workspace_id":1}`, want: http.StatusUnprocessableEntity},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/catalog", strings.NewReader(test.body))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != test.want {
				t.Fatalf("status = %d, want %d; body = %s", w.Code, test.want, w.Body.String())
			}
		})
	}
}

func TestListPublishingCatalogUsesRealM2MAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{Valid: true})
	}))
	defer catalogServer.Close()

	bundle := newM2MBundle(t, m2mBundleOpts{clientID: "catalog-m2m-client", rps: 10, burst: 10})
	bundle.h.WithSocialClient(socialclient.New(socialclient.Config{BaseURL: catalogServer.URL, APIKey: "test-token"}))
	r := gin.New()
	bundle.h.RegisterRoutes(r, nil, NewM2MJwAuthMiddleware(&config.Config{}, bundle.st, bundle.limiter))

	body := `{"workspace_id":1,"platform":"youtube"}`
	missing := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/catalog", strings.NewReader(body))
	missing.Header.Set("Content-Type", "application/json")
	missingResponse := httptest.NewRecorder()
	r.ServeHTTP(missingResponse, missing)
	if missingResponse.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d, want %d", missingResponse.Code, http.StatusUnauthorized)
	}

	valid := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/catalog", strings.NewReader(body))
	valid.Header.Set("Content-Type", "application/json")
	valid.Header.Set("Authorization", "Bearer "+bundle.plaintext)
	validResponse := httptest.NewRecorder()
	r.ServeHTTP(validResponse, valid)
	if validResponse.Code != http.StatusOK {
		t.Fatalf("valid auth status = %d, want %d; body=%s", validResponse.Code, http.StatusOK, validResponse.Body.String())
	}
}
