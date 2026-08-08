package pipeline

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// TestPublishingCatalogAndTargetsRoutesAreRetired verifies that Velox does not
// expose or mirror the InstaEdit-owned groups/channels catalog. The routes
// remain as deliberate compatibility tombstones so old callers fail clearly.
func TestPublishingCatalogAndTargetsRoutesAreRetired(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bundle := newM2MBundle(t, m2mBundleOpts{clientID: "catalog-m2m-client", rps: 10, burst: 10})
	catalogCalled := false
	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		catalogCalled = true
		http.Error(w, "unexpected catalog request", http.StatusInternalServerError)
	}))
	defer catalogServer.Close()

	db := openHandlerTestDB(t)
	defer db.Close()
	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         "existing-destination",
		Provider:              "social_gateway",
		ExternalDestinationID: "external-destination",
		Enabled:               true,
	}); err != nil {
		t.Fatalf("seed destination: %v", err)
	}
	bundle.h.WithStore(db).WithSocialClient(socialclient.New(socialclient.Config{BaseURL: catalogServer.URL}))

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

	for _, path := range []string{"/api/v1/publishing/catalog", "/api/v1/publishing/targets"} {
		valid := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		valid.Header.Set("Content-Type", "application/json")
		valid.Header.Set("Authorization", "Bearer "+bundle.plaintext)
		validResponse := httptest.NewRecorder()
		r.ServeHTTP(validResponse, valid)
		if validResponse.Code != http.StatusGone {
			t.Fatalf("valid auth status for %s = %d, want %d; body=%s", path, validResponse.Code, http.StatusGone, validResponse.Body.String())
		}

		var retired map[string]any
		if err := json.Unmarshal(validResponse.Body.Bytes(), &retired); err != nil {
			t.Fatalf("decode retired response for %s: %v", path, err)
		}
		if retired["error"] != "editor_catalog_removed" || retired["owner"] != "instaedit" {
			t.Fatalf("unexpected retired response for %s: %#v", path, retired)
		}
	}
	if catalogCalled {
		t.Fatal("retired catalog routes made an upstream catalog request")
	}
	var destinationCount int
	if err := db.DB().QueryRow("SELECT COUNT(*) FROM delivery_destinations").Scan(&destinationCount); err != nil {
		t.Fatalf("count destinations after retired request: %v", err)
	}
	if destinationCount != 1 {
		t.Fatalf("retired catalog route mutated delivery_destinations: count=%d, want 1", destinationCount)
	}
}
