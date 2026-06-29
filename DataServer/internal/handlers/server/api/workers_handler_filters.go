// Package api — RW-PROD-005 filter parser + applier.
//
// GET /api/v1/workers accepts optional query params:
//   - ?class=             (matches WorkerClass)
//   - ?status=            (must be one of CONNECTED|STALE|DISCONNECTED|DRAINING)
//   - ?rollout_group=     (matches RolloutGroup)
//   - ?needs_executor=    (executor id like "scene.composite.v1@1" — match the
//     executor list in Capabilities)
//
// Filters are validated at the boundary (400 Bad Request on parse failure)
// so the rest of the handler can rely on a typed Filters struct. The applier
// runs over an already-sanitized WorkerInfo slice; it never returns a
// cross-class worker when the class filter is active (DB-level filter — A7
// — runs in the SQL query before the applier even sees the row).
//
// The parser deliberately tolerates whitespace + lower/upper case for
// status so a "curl ?status=connected" does not 400; class and rollout_group
// match exact (case-sensitive) since those are operator-assigned.
package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

const (
	FilterStatusConnected    = "CONNECTED"
	FilterStatusStale        = "STALE"
	FilterStatusDisconnected = "DISCONNECTED"
	FilterStatusDraining     = "DRAINING"
)

// Filters is the typed result of ParseFilters. Empty fields mean
// "no filter on this dimension" (the corresponding GET param was
// absent).
//
// Encoding matches the lowercase class / rollout_group carried on
// the worker_info table; status is one of the four CONNECTED|STALE|
// DISCONNECTED|DRAINING enum strings (case-insensitive accepted,
// canonical case emitted).
type Filters struct {
	Class         string
	Status        string
	RolloutGroup  string
	NeedsExecutor string
}

// IsZero returns true iff every filter field is empty — i.e. the
// caller did not pass any GET param. Handler uses this to skip the
// applier entirely when the request is the unfiltered list.
func (f Filters) IsZero() bool {
	return f.Class == "" && f.Status == "" && f.RolloutGroup == "" && f.NeedsExecutor == ""
}

// ParseFilters parses the worker GET filter query params out of an
// incoming gin.Context. Returns (Filters, nil) on success, or writes
// the 400 Bad Request directly and returns a zero Filters + error
// when a param value is invalid.
func ParseFilters(c *gin.Context) (Filters, error) {
	var f Filters
	if v := strings.TrimSpace(c.Query("class")); v != "" {
		// Allow exact match (case-sensitive). Whitelist of canonical
		// classes is NOT enforced here so a future costmodel.descriptor
		// addition (e.g. "fpga") doesn't break the parser; the
		// applier filters by exact match anyway.
		f.Class = v
	}
	if v := strings.TrimSpace(c.Query("status")); v != "" {
		canonical := strings.ToUpper(v)
		switch canonical {
		case FilterStatusConnected, FilterStatusStale, FilterStatusDisconnected, FilterStatusDraining:
			f.Status = canonical
		default:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid ?status= — must be one of CONNECTED | STALE | DISCONNECTED | DRAINING (case-insensitive)",
				"got":   v,
			})
			return Filters{}, errFilterInvalid
		}
	}
	if v := strings.TrimSpace(c.Query("rollout_group")); v != "" {
		f.RolloutGroup = v
	}
	if v := strings.TrimSpace(c.Query("needs_executor")); v != "" {
		// Canonical pattern: <id>@<version>. We accept either with or
		// without the "@<version>" tail (operators sometimes forget).
		f.NeedsExecutor = v
	}
	return f, nil
}

// errFilterInvalid is a sentinel so the caller can detect a parse
// failure happens-after-response (gin.JSON was already emitted on the
// 400 path) without comparing strings.
var errFilterInvalid = filterParseError{}

type filterParseError struct{}

func (filterParseError) Error() string { return "filter parse failed; 400 already emitted" }

// ApplyFilters returns the subset of `infos` matching the typed
// filters. Empty filters return the input unchanged.
//
// Implementation note: the in-memory applier is the defense layer
// above the SQL WHERE filter (A7) — the SQL filter is the source of
// truth, the applier catches mislabelled rows that might have leaked
// from a future bug before reaching operators. Running both reduces
// the bug surface to a single line; running neither would let a
// regression in the SQL pass through silently.
func ApplyFilters(infos []workersreg.WorkerInfo, f Filters) []workersreg.WorkerInfo {
	if f.IsZero() {
		return infos
	}
	out := infos[:0:0] // never mutate caller slice
	for _, w := range infos {
		if f.Class != "" && w.Class != f.Class {
			continue
		}
		if f.Status != "" && w.Status != f.Status {
			continue
		}
		if f.RolloutGroup != "" && w.RolloutGroup != f.RolloutGroup {
			continue
		}
		if f.NeedsExecutor != "" {
			if !workerAdvertisesExecutor(w, f.NeedsExecutor) {
				continue
			}
		}
		out = append(out, w)
	}
	return out
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
