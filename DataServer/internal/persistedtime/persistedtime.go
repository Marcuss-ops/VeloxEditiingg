package persistedtime

import (
	"fmt"
	"time"
)

// Package persistedtime is the single parser for timestamps persisted by
// Velox writers in their three historical layouts:
//
//   - RFC3339Nano  (modern writers: time.Now().UTC().Format(time.RFC3339Nano))
//   - RFC3339      (seconds-precision writers: nowRFC3339-style helpers)
//   - "2006-01-02 15:04:05" (bare SQLite datetime('now') output)
//
// It exists because the same ladder was previously hand-copied into
// smokerunstore, store and artifactsstore ("local copy so this leaf stays
// free of internal/store"); three identical copies is exactly how semantic
// drift starts — one copy gains a layout, the others silently disagree on
// what a persisted timestamp is. This leaf has no internal dependencies by
// design, so every store-family package can import it without coupling.

// persistedLayouts is the canonical parse ladder, ordered fastest/most
// specific first (RFC3339Nano also parses seconds-precision RFC3339 input,
// so the second entry is a safety net rather than a common path).
var persistedLayouts = []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"}

// Parse parses a persisted timestamp from any of the canonical layouts and
// returns the parsed time. The error names the DB column via field so the
// caller's wrapping is operator-actionable.
func Parse(value, field string) (time.Time, error) {
	for _, layout := range persistedLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid persisted timestamp for %s: %q", field, value)
}
