// Package grpcserver / handler_config.go
//
// HandlerConfig and dependency setters / typed accessors for the
// WorkerControl handler. Extracted from handler.go to keep the core
// types file focused.
package grpcserver

import (
	"velox-server/internal/ingest"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/registry"
)

// HandlerConfig holds configuration for the gRPC handler.
type HandlerConfig struct {
	// PushMode enables Phase 5+ behaviour: send JobOffer directly and
	// let workers respond with JobAccepted.
	PushMode bool
	// AllowInsecure is dev-only: allow insecure gRPC connections
	// (VELOX_GRPC_ALLOW_INSECURE_DEV).
	AllowInsecure bool
	// AllowedWorkers is a P0 comma-separated worker ID allowlist
	// (VELOX_ALLOWED_WORKERS).
	AllowedWorkers string
}

// SetIngestionSvc installs the canonical TaskReportIngestionService so
// handleTaskResult can delegate to it. Bootstrap calls this immediately
// after NewHandler to wire the audit closure. Setting nil clears the
// reference (useful for tests that swap services mid-flight).
func (h *Handler) SetIngestionSvc(svc *ingest.TaskReportIngestionService) {
	h.ingestionSvc = svc
}

// SetResourceSink installs the WorkerResourceSink used by handleHeartbeat
// (Scorecard v1 / F2). Bootstrap wires metrics.NewCollector here; tests
// inject a recording stub. NIL-safe — handlers without a metrics surface
// still persist the typed heartbeat via registry.Heartbeat() but skip
// the Prometheus projection.
func (h *Handler) SetResourceSink(sink velmetrics.WorkerResourceSink) {
	h.resourceSink = sink
}

// SetPlacementRejectionSink installs the PlacementRejectionSink used by
// the placement pipeline (recordPlacementRejections + handleUnsupportedExecutorRejection).
// Bootstrap wires metrics.NewCollector here; tests inject a recording stub.
// NIL-safe — handlers without a metrics surface still log rejections but
// skip the Prometheus projection.
func (h *Handler) SetPlacementRejectionSink(sink velmetrics.PlacementRejectionSink) {
	h.placementRejectionSink = sink
}

// SetCapabilityRegistry installs the readiness registry that gates the
// on-the-wire "artifact.commit.v1" dispatch path. Bootstrap wires the
// canonical registry.NewCapabilityRegistry() (with coordinator + spool +
// transport probes) here; tests can inject a focused registry to verify
// the fail-closed semantic in handler_artifacts_test.go.
//
// NIL-safe — a Handler constructed without the registry (legacy test
// paths, partial-wiring bootstrap variants) skips the gate entirely.
func (h *Handler) SetCapabilityRegistry(r *registry.CapabilityRegistry) {
	h.capabilityRegistry = r
}
