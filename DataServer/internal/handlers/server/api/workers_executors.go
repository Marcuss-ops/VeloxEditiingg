package api

import (
	"strings"

	workersreg "velox-server/internal/workers"
	"velox-shared/controltransport"
)

// extractExecutors projects the canonical registry for the HTTP response.
// Legacy map decoding is deliberately performed by workers.Registry at the
// persistence/heartbeat boundary, not by this API package.
func extractExecutors(source interface{}) []ExecutorEntry {
	registry, ok := source.(controltransport.ExecutorRegistry)
	if !ok {
		// Compatibility is confined to this API boundary for callers still
		// holding a decoded legacy capabilities map. Production WorkerInfo
		// projections always pass ExecutorRegistry.
		legacy, err := controltransport.ExecutorRegistryFromLegacy(source)
		if err != nil {
			return nil
		}
		registry = legacy
	}
	all := registry.All()
	if len(all) == 0 {
		return nil
	}
	out := make([]ExecutorEntry, 0, len(all))
	for _, capability := range all {
		out = append(out, ExecutorEntry{ID: capability.ID, Version: int32(capability.Version)})
	}
	return out
}

// workerAdvertisesExecutor is true iff the typed registry contains an
// executor whose ID matches `want`. The version tail (after "@") is
// ignored for the operator filter; exact version matching remains the
// placement master's responsibility.
//
// Returns false on empty Capabilities or absent "executors" key.
func workerAdvertisesExecutor(w workersreg.WorkerInfo, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return true
	}
	wantID := want
	if at := strings.Index(want, "@"); at >= 0 {
		wantID = want[:at]
	}
	return w.ExecutorCapabilities.HasID(wantID)
}
