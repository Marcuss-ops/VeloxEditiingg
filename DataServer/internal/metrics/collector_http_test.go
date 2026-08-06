package metrics

import (
	"strings"
	"testing"
)

// TestHTTPRouteUsage_RegisteredOnCollector pins the Phase 6 route-usage
// family to the collector so the router middleware's sink has a target.
func TestHTTPRouteUsage_RegisteredOnCollector(t *testing.T) {
	reg := NewRegistry()
	NewCollector(reg)

	out := dumpRegistryAll(t, reg)
	if !strings.Contains(out, "velox_master_http_route_requests_total") {
		t.Fatalf("route-usage family missing from collector output:\n%s", out)
	}
}

// TestHTTPRouteUsage_RecordStampsLabels verifies RecordHTTPRoute stamps
// (surface, route template) pairs and that an empty surface/route is
// dropped (no invalid series).
func TestHTTPRouteUsage_RecordStampsLabels(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)

	c.RecordHTTPRoute(RouteSurfaceLegacy, "/api/v1/workers/:worker_id")
	c.RecordHTTPRoute(RouteSurfaceAdmin, "/api/v1/admin/workers/:worker_id")
	c.RecordHTTPRoute(RouteSurfaceLegacy, "/api/v1/workers/:worker_id") // same series increments
	c.RecordHTTPRoute("", "")                                          // dropped
	c.RecordHTTPRoute(RouteSurfaceFleet, "")                           // dropped

	out := dumpRegistryAll(t, reg)
	if !strings.Contains(out, `velox_master_http_route_requests_total{route="/api/v1/workers/:worker_id",surface="legacy"} 2`) &&
		!strings.Contains(out, `velox_master_http_route_requests_total{surface="legacy",route="/api/v1/workers/:worker_id"} 2`) {
		t.Errorf("expected legacy route series with count 2, got:\n%s", out)
	}
	if !strings.Contains(out, `route="/api/v1/admin/workers/:worker_id"`) && !strings.Contains(out, `route="/api/v1/admin/workers/:worker_id"`) {
		t.Errorf("expected admin route series, got:\n%s", out)
	}
	if strings.Contains(out, `surface=""`) {
		t.Errorf("empty surface must be dropped, got:\n%s", out)
	}
}

// TestHTTPRouteUsage_ClassifyRouteSurface pins the canonical surface
// taxonomy (Phase 6): agent / admin / fleet are canonical namespaces;
// the pre-canonical worker paths are legacy; everything else is other.
func TestHTTPRouteUsage_ClassifyRouteSurface(t *testing.T) {
	cases := []struct {
		route string
		want  string
	}{
		{"/api/v1/agent/register", RouteSurfaceAgent},
		{"/api/v1/agent/assets/:asset_id", RouteSurfaceAgent},
		{"/api/v1/agent/cache/protected-assets", RouteSurfaceAgent},
		{"/api/v1/admin/workers", RouteSurfaceAdmin},
		{"/api/v1/admin/workers/:worker_id", RouteSurfaceAdmin},
		{"/api/v1/admin/workers/:worker_id/drain", RouteSurfaceAdmin},
		{"/api/v1/fleet/metrics", RouteSurfaceFleet},
		{"/api/v1/fleet/alerts/active", RouteSurfaceFleet},
		// Pre-canonical legacy surfaces — counted before removal.
		{"/api/v1/workers", RouteSurfaceLegacy},
		{"/api/v1/workers/:worker_id", RouteSurfaceLegacy},
		{"/api/v1/workers/register", RouteSurfaceLegacy},
		{"/api/v1/workers/cache/protected-assets", RouteSurfaceLegacy},
		{"/api/v1/worker-assets/:asset_id", RouteSurfaceLegacy},
		{"/worker/revoke", RouteSurfaceLegacy},
		{"/worker/drain", RouteSurfaceLegacy},
		// Pre-canonical fleet aggregates mounted under admin: the fleet-wide
		// snapshot is legacy (the per-worker one below stays admin) and the
		// fleet-wide alert ledger is legacy.
		{"/api/v1/admin/workers/metrics", RouteSurfaceLegacy},
		{"/api/v1/admin/alerts/active", RouteSurfaceLegacy},
		{"/api/v1/admin/alerts/recent", RouteSurfaceLegacy},
		// Per-worker read endpoints under admin stay ADMIN (not legacy):
		// they are the canonical operator surface.
		{"/api/v1/admin/workers/:worker_id/metrics", RouteSurfaceAdmin},
		{"/api/v1/admin/workers/:worker_id/alerts", RouteSurfaceAdmin},
		// Everything else.
		{"/api/v1/jobs", RouteSurfaceOther},
		{"/metrics", RouteSurfaceOther},
		{"/api/health", RouteSurfaceOther},
	}
	for _, tc := range cases {
		if got := ClassifyRouteSurface(tc.route); got != tc.want {
			t.Errorf("ClassifyRouteSurface(%q) = %q, want %q", tc.route, got, tc.want)
		}
	}
}
