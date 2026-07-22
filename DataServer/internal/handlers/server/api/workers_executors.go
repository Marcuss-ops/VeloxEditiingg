package api

import (
	"strings"

	workersreg "velox-server/internal/workers"
)

// extractExecutors pulls the canonical executor list from the worker's
// capabilities map. Supports both the proto-structured form
// ("executors": [{"id":"...","version":1}]) and the flat-map form.
func extractExecutors(caps map[string]interface{}) []ExecutorEntry {
	if caps == nil {
		return nil
	}
	// Proto-structured form: {"executors": [{"id":"...","version":1}]}
	if raw, ok := caps["executors"]; ok {
		switch list := raw.(type) {
		case []interface{}:
			out := make([]ExecutorEntry, 0, len(list))
			for _, item := range list {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				id, _ := m["id"].(string)
				if id == "" {
					continue
				}
				var ver int32
				if v, ok := toFloat64(m["version"]); ok {
					ver = int32(v)
				}
				out = append(out, ExecutorEntry{ID: id, Version: ver})
			}
			return out
		}
	}
	return nil
}

// workerAdvertisesExecutor is true iff `infos` Capabilities["executors"]
// contains an entry whose id matches `want`. The version tail (after
// "@") is ignored — operators want to filter by capability regardless
// of which version is currently running, and the dispatch master uses
// the same logic when ranking.
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
	if w.Capabilities == nil {
		return false
	}
	raw, ok := w.Capabilities["executors"]
	if !ok {
		return false
	}
	switch list := raw.(type) {
	case []interface{}:
		for _, item := range list {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			id, _ := m["id"].(string)
			if id == wantID {
				return true
			}
		}
	case []map[string]interface{}:
		for _, m := range list {
			id, _ := m["id"].(string)
			if id == wantID {
				return true
			}
		}
	}
	return false
}
