package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

// errFilterInvalid is a sentinel so the caller can detect a parse
// failure happens-after-response (gin.JSON was already emitted on the
// 400 path) without comparing strings.
var errFilterInvalid = filterParseError{}

type filterParseError struct{}

func (filterParseError) Error() string { return "filter parse failed; 400 already emitted" }

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
