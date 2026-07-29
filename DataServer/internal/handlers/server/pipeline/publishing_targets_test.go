package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// TestListPublishingTargets_CanPostFalse_WritesEnabledFalse is the
// cross-repo anti-regression pin for the catalog → delivery_destinations
// chain: when the InstaeditLogin catalog returns a target with
// CanPost=false (because the channel flipped to reauth_required or was
// disabled upstream), the Velox handler MUST upsert
// delivery_destinations.enabled=false on this refresh. Without this,
// Velox would keep the destination enabled and the dispatcher's
// per-destination retry budget would burn cycles uploading to a channel
// whose OAuth grant has been revoked.
//
// The handler at publishing_targets.go:131 reads target.CanPost and
// passes it to store.UpsertSyncedDeliveryDestination. This test pins
// the full HTTP round-trip — the handler MUST wire the socialclient
// response all the way to the SQLite enabled column.
//
// A failure of this test means either:
//
//   1. The handler stopped reading target.CanPost (regression at
//      publishing_targets.go:131), or
//   2. The store-level upsert stopped applying the conflict-update
//      on enabled (covered by the companion store test
//      store_delivery_destinations_sync_test.go).
//
// The test wires a real *socialclient.Client at an httptest.NewServer
// (mirrors the pattern at socialclient/targets_test.go:42) so the
// production HTTP transport is exercised end-to-end without needing
// to refactor Handlers.socialClient into an interface.
func TestListPublishingTargets_CanPostFalse_WritesEnabledFalse(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	const (
		destinationID         = "instaedit_extdst_01JCANPOSTFALSE"
		externalDestinationID = "extdst_01JCANPOSTFALSE"
	)

	// httptest catalog endpoint impersonating InstaeditLogin after the
	// channel flipped to reauth_required. Returns one target with
	// CanPost=false + Status="reauth_required" + TargetErrorCode="BLOCKED_AUTH"
	// + all-zero capabilities — the canonical catalog verdict.
	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/destinations/resolve-target" {
			t.Errorf("unexpected request path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []socialclient.PublishingTarget{{
				PlatformAccountID:     381,
				Platform:              "youtube",
				ChannelID:             "UC_reauth_42",
				ChannelName:           "Reauth Channel",
				ExternalDestinationID: externalDestinationID,
				Status:                "reauth_required",
				Enabled:               true, // binding row still healthy upstream
				CanPost:               false,
				BlockReason:           "channel authentication requires attention",
				Capabilities:          socialclient.PublishingCapabilities{}, // all false
				TargetErrorCode:       "BLOCKED_AUTH",
			}},
		})
	}))
	t.Cleanup(catalogServer.Close)

	s := openHandlerTestDB(t)
	t.Cleanup(func() { s.Close() })

	// Pre-seed an enabled row to prove the conflict-update path
	// (not INSERT-IGNORE — that would silently leave enabled=1 on disk).
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), store.DeliveryDestination{
		DestinationID:         destinationID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalDestinationID,
		Name:                  "Pre-existing enabled row",
		Enabled:               true, // stale; catalog now says can_post=false
	}); err != nil {
		t.Fatalf("seed enabled row: %v", err)
	}

	// Wire a real socialclient.Client at the httptest catalog. This
	// matches the production wire contract exactly (no mock layer).
	client := socialclient.New(socialclient.Config{
		BaseURL: catalogServer.URL,
		APIKey:  "test-token",
	})

	h := (&Handlers{}).WithStore(s).WithSocialClient(client)
	r := gin.New()
	r.POST("/api/v1/publishing/targets", h.ListPublishingTargets())

	body, _ := json.Marshal(PublishingTargetsRequest{WorkspaceID: 42, Platform: "youtube"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/targets", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("HTTP: want 200; got %d (body=%s)", w.Code, w.Body.String())
	}

	// Pin the response shape: CanPost=false MUST surface in the JSON
	// response so a sender using the can_post predicate (§0.3 in the
	// publishing runbook) skips the entry.
	var resp PublishingTargetsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, w.Body.String())
	}
	if len(resp.Targets) != 1 {
		t.Fatalf("response targets: want 1; got %d", len(resp.Targets))
	}
	if resp.Targets[0].CanPost {
		t.Fatalf("response.targets[0].CanPost: want false; got true (handler did not surface reauth verdict to sender)")
	}
	if resp.Targets[0].TargetErrorCode != "BLOCKED_AUTH" {
		t.Fatalf("response.targets[0].TargetErrorCode: want BLOCKED_AUTH; got %q", resp.Targets[0].TargetErrorCode)
	}

	// Pin the SQLite-side persistence: the upsert MUST have flipped
	// delivery_destinations.enabled=0 on this refresh. Without this,
	// the dispatcher would route subsequent jobs to a reauth-required
	// channel until an operator manually disabled the row.
	row, err := s.GetDeliveryDestination(context.Background(), destinationID)
	if err != nil || row == nil {
		t.Fatalf("GetDeliveryDestination post-refresh: row=%v err=%v", row, err)
	}
	if row.Enabled {
		t.Fatalf("delivery_destinations.enabled: want false (catalog said can_post=false); got true — reauth row would still dispatch")
	}
}

// TestListPublishingTargets_CanPostTrue_WritesEnabledTrue is the
// happy-path companion: a healthy catalog entry with CanPost=true
// MUST upsert delivery_destinations.enabled=1.
func TestListPublishingTargets_CanPostTrue_WritesEnabledTrue(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	const (
		destinationID         = "instaedit_extdst_01JCANPOSTTRUE"
		externalDestinationID = "extdst_01JCANPOSTTRUE"
	)

	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []socialclient.PublishingTarget{{
				PlatformAccountID:     381,
				Platform:              "youtube",
				ChannelID:             "UC_healthy_42",
				ChannelName:           "Healthy Channel",
				ExternalDestinationID: externalDestinationID,
				Status:                "active",
				Enabled:               true,
				CanPost:               true,
				Capabilities: socialclient.PublishingCapabilities{
					UploadVideo:  true,
					SetThumbnail: true,
					Publish:      true,
					Schedule:     true,
				},
			}},
		})
	}))
	t.Cleanup(catalogServer.Close)

	s := openHandlerTestDB(t)
	t.Cleanup(func() { s.Close() })

	client := socialclient.New(socialclient.Config{BaseURL: catalogServer.URL, APIKey: "test-token"})
	h := (&Handlers{}).WithStore(s).WithSocialClient(client)
	r := gin.New()
	r.POST("/api/v1/publishing/targets", h.ListPublishingTargets())

	body, _ := json.Marshal(PublishingTargetsRequest{WorkspaceID: 42, Platform: "youtube"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/targets", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("HTTP: want 200; got %d (body=%s)", w.Code, w.Body.String())
	}
	row, err := s.GetDeliveryDestinationByExternalID(context.Background(), externalDestinationID)
	if err != nil || row == nil {
		t.Fatalf("post-refresh: row=%v err=%v", row, err)
	}
	if !row.Enabled {
		t.Fatalf("delivery_destinations.enabled: want true; got false (healthy catalog row did not flip enabled=1)")
	}
}

// TestListPublishingTargets_PreExistingEnabledFlipsToDisabledOnReauth
// pins the state transition explicitly: a row that was enabled on a
// previous refresh MUST flip to enabled=false on the reauth refresh —
// it is NOT silently preserved by an INSERT-IGNORE.
func TestListPublishingTargets_PreExistingEnabledFlipsToDisabledOnReauth(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	const (
		destinationID         = "instaedit_extdst_01JTRANSITION"
		externalDestinationID = "extdst_01JTRANSITION"
	)

	s := openHandlerTestDB(t)
	t.Cleanup(func() { s.Close() })

	// Pre-existing enabled row from a prior refresh.
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), store.DeliveryDestination{
		DestinationID:         destinationID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalDestinationID,
		Enabled:               true,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []socialclient.PublishingTarget{{
				ExternalDestinationID: externalDestinationID,
				Status:                "reauth_required",
				CanPost:               false,
				TargetErrorCode:       "BLOCKED_AUTH",
			}},
		})
	}))
	t.Cleanup(catalogServer.Close)

	client := socialclient.New(socialclient.Config{BaseURL: catalogServer.URL, APIKey: "test-token"})
	h := (&Handlers{}).WithStore(s).WithSocialClient(client)
	r := gin.New()
	r.POST("/api/v1/publishing/targets", h.ListPublishingTargets())

	body, _ := json.Marshal(PublishingTargetsRequest{WorkspaceID: 42, Platform: "youtube"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publishing/targets", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("HTTP: want 200; got %d (body=%s)", w.Code, w.Body.String())
	}

	row, err := s.GetDeliveryDestination(context.Background(), destinationID)
	if err != nil || row == nil {
		t.Fatalf("post-refresh: row=%v err=%v", row, err)
	}
	if row.Enabled {
		t.Fatalf("enabled: want false (transition pinned — prior refresh was enabled, this refresh said can_post=false); got true — pre-existing enabled row was NOT updated")
	}
}

// --- test helpers ---------------------------------------------------------

// openHandlerTestDB opens a temp SQLite store suitable for handler
// integration tests. Mirrors the helper of the same name in
// internal/store/atomic_job_task_test.go (kept private to this package
// to avoid coupling test internals across packages).
func openHandlerTestDB(t *testing.T) *store.SQLiteStore {
	t.Helper()
	s, err := store.NewSQLiteStore(t.TempDir() + "/handler-test.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	return s
}
