// Package grpcserver / handler_config.go
//
// HandlerConfig and dependency setters / typed accessors for the
// WorkerControl handler. Extracted from handler.go to keep the core
// types file focused.
package grpcserver

import (
	"strings"

	"velox-server/internal/artifacts"
	"velox-server/internal/completion"
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

// SetAssetDownloadProgressSink installs the latest-state persistence sink.
// Nil restores the Handler's dbStore fallback when one is configured.
func (h *Handler) SetAssetDownloadProgressSink(sink AssetDownloadProgressSink) {
	h.assetDownloadProgressSink = sink
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

// SetCompletionProtocol wires the durable typed artifact publication bridge.
// The bridge is optional for legacy/lightweight handler tests, but bootstrap
// must supply all four dependencies when typed worker capabilities are on.
func (h *Handler) SetCompletionProtocol(coord completion.Coordinator, store completion.UploadProtocolStore, chunked *artifacts.ChunkedUploadService, masterURL string) {
	h.completionCoord = coord
	h.completionStore = store
	h.chunkedUploadSvc = chunked
	h.masterURL = strings.TrimRight(strings.TrimSpace(masterURL), "/")
}

// SetPlacementPin installs the worker_id pin (VELOX_PLACEMENT_PIN_WORKER_ID)
// used by the placement matcher to deterministically restrict dispatch
// to a single worker. Empty string (or whitespace-only) clears the pin
// and restores the stateless matcher. Bootstrap wires
// cfg.Workers.PlacementPinWorkerID here so the matcher emits
// RejectPlacementPinExcluded for every non-pinned worker when the
// operator sets the env var; tests/worker-cert/smoke_one.sh relies on
// this for per-worker deterministic certification without having to
// drain the rest of the eligible pool at the operator level.
func (h *Handler) SetPlacementPin(workerID string) {
	h.placementMatcher.SetPin(strings.TrimSpace(workerID))
}
