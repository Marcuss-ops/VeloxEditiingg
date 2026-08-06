// Package metrics / collector_http.go
//
// HTTP control-plane route usage counter. Every HTTP request served by the
// master control plane is classified by API surface and counted so operators
// can decide when a legacy route can be removed (Phase 6 API-surface
// unification): a legacy surface with sustained usage must stay; a quiet one
// can be retired.
//
// The middleware lives in the router bootstrap (cmd/server) and forwards
// (surface, routeTemplate) pairs to Collector.RecordHTTPRoute via the
// HTTPRouteUsageSink contract below. The route label is the gin route
// TEMPLATE (e.g. "/api/v1/admin/workers/:worker_id"), never the concrete
// path with a worker_id, so cardinality stays bounded by the route table.
package metrics

import "strings"

// Route surface taxonomy (Phase 6 API-surface unification).
//
// The canonical division of the control plane is:
//
//	agent — worker-authenticated traffic under /api/v1/agent/*
//	admin — operator worker surface under /api/v1/admin/workers/*
//	fleet — aggregated snapshots / metrics / dashboards under /api/v1/fleet/*
//
// Everything still mounted under a pre-canonical path is classified legacy
// so its usage is visible before removal. `other` covers the remaining
// control-plane routes (jobs, script, video, instaedit, ansible, health...).
const (
	RouteSurfaceAgent  = "agent"
	RouteSurfaceAdmin  = "admin"
	RouteSurfaceFleet  = "fleet"
	RouteSurfaceLegacy = "legacy"
	RouteSurfaceOther  = "other"
)

// ClassifyRouteSurface maps a gin route TEMPLATE (FullPath) to its canonical
// surface. Legacy routes are the ones the Phase-6 migration retires once
// their usage counter is quiet: the old /api/v1/workers diagnostic surface
// (list/get + per-worker read endpoints), the /api/v1/worker-assets agent
// path, and the /worker admin group. Unknown templates classify as other.
func ClassifyRouteSurface(route string) string {
	switch {
	case strings.HasPrefix(route, "/api/v1/agent/"):
		return RouteSurfaceAgent
	case strings.HasPrefix(route, "/api/v1/fleet/"):
		return RouteSurfaceFleet
	// The pre-canonical fleet aggregates that were mounted under the admin
	// namespace stay legacy: the fleet-wide snapshot
	// /api/v1/admin/workers/metrics (distinct from the per-worker
	// /api/v1/admin/workers/:worker_id/metrics, which IS admin) and the
	// /api/v1/admin/alerts/* fleet-wide alert ledger. Both must be counted
	// as legacy so their removal window is measurable. Order matters: these
	// exact/prefix cases must run BEFORE the generic /api/v1/admin/workers
	// prefix match below.
	case route == "/api/v1/admin/workers/metrics",
		strings.HasPrefix(route, "/api/v1/admin/alerts/"):
		return RouteSurfaceLegacy
	case strings.HasPrefix(route, "/api/v1/admin/workers"):
		return RouteSurfaceAdmin
	case strings.HasPrefix(route, "/api/v1/workers"),
		strings.HasPrefix(route, "/api/v1/worker-assets"),
		strings.HasPrefix(route, "/worker/"):
		return RouteSurfaceLegacy
	default:
		return RouteSurfaceOther
	}
}

// HTTPRouteUsageSink is the contract the router middleware depends on for
// forwarding per-request route classifications onto the Prometheus registry.
// Defined here (consumed-by-router) following the same pattern as
// WorkerResourceSink / PlacementRejectionSink.
type HTTPRouteUsageSink interface {
	// RecordHTTPRoute counts one HTTP request served on the given API
	// surface for the given route template. surface is one of the
	// RouteSurface* constants; route is the gin FullPath template.
	RecordHTTPRoute(surface, route string)
}

// Compile-time guard: *Collector implements HTTPRouteUsageSink.
var _ HTTPRouteUsageSink = (*Collector)(nil)

// RecordHTTPRoute increments velox_master_http_route_requests_total for a
// single request classified by (surface, route template). Empty surface or
// route is a programming error in the router middleware and is dropped
// loudly-free: an empty label would expose an invalid series.
func (c *Collector) RecordHTTPRoute(surface, route string) {
	if surface == "" || route == "" {
		return
	}
	c.httpRouteRequests.Inc([]string{surface, route}, 1)
}

// initHTTPFamilies creates the HTTP route usage counter. Called once from
// NewCollector at boot.
func (c *Collector) initHTTPFamilies() {
	c.httpRouteRequests = NewCounterFamily(
		"velox_master_http_route_requests_total",
		"HTTP requests served by the master control plane, classified by route template and API surface (agent|admin|fleet|legacy|other)",
		[]string{"surface", "route"},
	)
}

// httpFamilies returns the HTTP route-usage subset registered by
// NewCollector via allFamilies.
func (c *Collector) httpFamilies() []*Family {
	return []*Family{c.httpRouteRequests}
}
