// Package dbutil — small ergonomic helpers that bridge Go map[string]interface{}
// shapes (returned by database/sql Scan) and typed Go values.
//
// The previous code-base had three near-identical helpers living in
// different packages (queue.getIntField, queue.asIntFromMap added later,
// grpcserver.readMapInt added during the gRPC worker handler rewrite).
// They drifted on string handling and produced subtle behaviour gaps when
// the same field was read from a SQLite row (typed int) versus a JSON blob
// (string integer).
//
// IntFromMap is the canonical replacement and lives here so any future
// schema/migrations code can rely on a single, well-tested conversion
// without re-implementing it.
package dbutil

import (
	"encoding/json"
)

// IntFromMap extracts an integer field from a generic map. Returns 0 when
// the key is missing or the value cannot be interpreted as an integer.
//
// Accepted input shapes (in order):
//   - int / int64 / float64 (typical database/sql Scan results for INTEGER
//     columns: float64 for the textual protocol, int64 for native drivers);
//   - string: parsed as JSON integer so values landed via JSON blob
//     (e.g., raw_json in jobs) convert cleanly. Empty strings return 0.
func IntFromMap(m map[string]interface{}, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case float32:
		return int(n)
	case float64:
		return int(n)
	case string:
		// JSON-decoded payloads sometimes carry integers as strings
		// ("retry_count": "3"). Parse through json.Unmarshal for the
		// common case rather than rolling a hand-written int parser.
		if n == "" {
			return 0
		}
		var out int
		if err := json.Unmarshal([]byte(n), &out); err == nil {
			return out
		}
		return 0
	}
	return 0
}
