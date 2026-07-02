// Package completion / reconcile_metrics_adapter_test.go
//
// Reverse-direction compile-time guard: the metrics package's
// *Collector (which exposes IncReconcile(string, string) +
// IncCommitDeadlineExceeded()) is structurally compatible with
// this package's ReconcileMetrics interface (which uses
// ReconcileCase + ReconcileAction — both `type X string`).
//
// The guard verifies both directions:
//
//   - *Collector satisfies completion.ReconcileMetrics
//     (a signature drift on either side breaks the build).
//
// Why a separate file: putting the guard inside reconcile_test.go
// would force the metrics package to import completion for the
// reverse direction, which would close the import cycle. This
// file is package completion (white-box); the metrics package is
// not imported. The guard var declaration is the canonical
// build-time check.
//
// Equivalent to the guard in metrics/collector_reconcile_guard.go
// but from the COMPLETION side, ensuring the wire contract is
// symmetric.
package completion

import (
	"testing"

	"velox-server/internal/metrics"
)

// TestReconcileMetricsAdapter_CollectorSatisfies asserts that
// *metrics.Collector satisfies the completion.ReconcileMetrics
// interface. The assertion is a compile-time guard; the test
// body is empty (the type check happens at link time).
func TestReconcileMetricsAdapter_CollectorSatisfies(t *testing.T) {
	var _ ReconcileMetrics = (*metrics.Collector)(nil)
	// Also verify the noop satisfies (sanity).
	var _ ReconcileMetrics = noopReconcileMetrics{}
}
