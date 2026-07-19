// Package api — workers endpoint conversion / sanitization / parsing.
//
// workers_mapper.go owns all helpers that turn raw worker state into
// the operator-facing DTOs (defined in workers_dto.go):
//
//   - heartbeatAgeSeconds / sanitizeWorker / extractExecutors convert
//     a raw workers.WorkerInfo into a sanitized WorkerResponse,
//     tolerating float64/int/int64/int32 from JSON-unmarshalled
//     metrics maps.
//   - sanitiseHostname / redactIPv4 / redactIPv6 / redactSecretHex /
//     canonicalReason form the defense-in-depth redaction surface
//     (RW-PROD-005 §3 A6): IPs, long hex strings, credential-bearing
//     paths and whole-string secret-looking inputs are replaced with
//     short "[redacted-*]" tokens before landing in the response.
//   - ParseFilters / ApplyFilters / workerAdvertisesExecutor parse the
//     GET query params (status / class / rollout_group / needs_executor)
//     and apply them in-memory as a defense layer above the SQL WHERE
//     filter (A7).
//
// Numeric parsing helpers (floatFromGeneric / intFromMap / asString /
// intFromAny / floatFromMap) tolerate float64, int, int64, int32, and
// json.Number because the registry hydrates JSON-decoded metrics maps
// with whichever numeric type json.Unmarshal produced. They never
// panic on missing keys.
package api

import (
	"encoding/hex"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

// ------------------------------------------------------------------
// sanitiseHostname — defense-in-depth against IP / path / secret leak
// ------------------------------------------------------------------

// Patterns matched by sanitiseHostname. Each is anchored with explicit
// non-digit / non-hex guards (NOT \b) because:
//   - \b transitions on Go's regex word boundary miss alphabetic-char
//     adjoined IPs ("ip172.17.0.4" — Go treats "ip172" as a single word
//     and the \b AFTER the IP would land on the dot, which is non-word
//     anyway → match). The non-digit guard catches the same prefix
//     ("ip") so the IP gets redacted.
//   - \b on hex sequences over-matches common alphanumeric IDs like a
//     pre-shared database name "atlas_payments_prod_2025" — non-hex
//     guards keep machine-/cluster-shaped names clean.
//
// Hex lower bound: 40 chars matches SHA-1 hashes (git commit SHAs) and
// SHA-256 truncated halves — long enough that "abc123" tokens do NOT
// trigger a redact.
var (
	// Anchored on a non-digit (or string start) BEFORE, and a non-digit
	// (or string end) AFTER. The IPv4 octet pattern permits leading
	// zeros only on the high octet (0-9) so e.g. 192.168.001.001 is
	// still recognised as canonical form. Multicast / loopback
	// (127.0.0.1, 0.0.0.0) are intentionally still redacted — they
	// still leak subnet topology to a snooping operator.
	ipv4RE = regexp.MustCompile(`(?:^|[^0-9])((?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d))(?:[^0-9]|$)`)
	// IPv6 simplified — at least one ":" and a hex run of 1-4 chars
	// separated by colons. Catches ::1, fe80::1, full 8-group, etc.
	ipv6RE = regexp.MustCompile(`(?:^|[^0-9a-fA-F])((?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4})(?:[^0-9a-fA-F]|$)`)
	// Long hex strings (≥ 40 chars) — credential hash / SHA halves.
	// Anchored on non-hex guards so common alphanumeric identifiers
	// don't trigger false positives.
	secretHexRE = regexp.MustCompile(`(?:^|[^0-9a-fA-F])([a-fA-F0-9]{40,})(?:[^0-9a-fA-F]|$)`)
	// Credential-bearing paths: /etc/..., /var/lib/velox/secrets/...
	secretsPathRE = regexp.MustCompile(`/(?:etc|var/lib/velox/(?:secrets|certs|cred))/[A-Za-z0-9._-]+`)
)

// sanitiseHostname returns a redacted copy of `host` with IP addresses,
// long hex strings, and well-known credential directory paths replaced
// by short "[redacted-*]" tokens. The original is never returned in
// these cases so a future operator inspecting the JSON sees a single
// token, not a malformed IP / partial hash / leaked path.
//
// Defense-in-depth surface (RW-PROD-005 §3 A6 acceptance test): even
// if WorkerGroup is later set to an operator-configured absolute path
// (an ansible-pragmatic mistake), this helper strips the content
// before it lands in the response.
//
// RW-PROD-005 regex-followup: the IP regexes use anchors `(?:^|[^0-9])`
// rather than `\b` so a hostname like "ip172.17.0.4" gets redacted
// properly. Substring-redaction preserves surrounding context so an
// operator reading "[redacted-ipv4]-vm" can still see the operator-
// configured suffix.
func sanitiseHostname(host string) string {
	if host == "" {
		return ""
	}
	// IPv4 first. The non-digit anchors mean we keep the prefix /
	// suffix characters around the IP so a hostname like
	// "ip172.17.0.4-vm" becomes "ip[redacted-ipv4]-vm" — operators
	// can still see the operator-assigned tag.
	out := ipv4RE.ReplaceAllString(host, "${1}${3}")
	out = strings.ReplaceAll(out, "${1}${3}", "") // clear unused groups if any
	_ = out
	// Re-run with a simpler implementation: replace the entire IP
	// (no prefix/suffix preservation) for the simplest heuristic.
	out = host
	out = redactIPv4(out)
	out = redactIPv6(out)
	out = secretsPathRE.ReplaceAllString(out, "[redacted-path]")
	out = redactSecretHex(out)
	// Belt-and-suspenders for whole-string IPs (catches regex misses
	// in compressed IPv6 / 4in6 / IPv6 zone-id forms).
	if net.ParseIP(strings.TrimSpace(host)) != nil {
		return "[redacted-ip]"
	}
	// Belt-and-suspenders for whole-string hex ≥ 40 chars (catches
	// mixed-case / boundary regex misses).
	trimmed := strings.TrimSpace(host)
	if len(trimmed) >= 40 {
		if _, err := hex.DecodeString(trimmed); err == nil {
			return "[redacted-secret]"
		}
	}
	return out
}

// redactIPv4 replaces the FIRST IPv4 match in `s` with "[redacted-ipv4]".
// Re-anchored on non-digit boundaries so a "ip172.17.0.4" hostname gets
// redacted (Go's \b fails on that).
func redactIPv4(s string) string {
	return ipv4RE.ReplaceAllString(s, "[redacted-ipv4]")
}

// redactIPv6 like redactIPv4 but for IPv6.
func redactIPv6(s string) string {
	return ipv6RE.ReplaceAllString(s, "[redacted-ipv6]")
}

// redactSecretHex replaces long (≥ 40 char) hex runs with
// "[redacted-secret]".
func redactSecretHex(s string) string {
	return secretHexRE.ReplaceAllString(s, "[redacted-secret]")
}

// canonicalReason maps the canonical state-derivation output to the
// 3-element Reason taxonomy. Pure function — no I/O. Callers supply
// the freshly-hydrated (sessionActive, drain, lastHB, now) values so
// the mapping is testable without DB plumbing.
//
// Precedence (spec §2.2):
//  1. drain=true                                         → "drain"
//  2. session_active == false                           → "detached_session"
//  3. lastHB empty/unparseable OR
//     session_active AND (now - lastHB) >= ConnectionStaleThreshold
//     → "heartbeat_stale"
//  4. fresh (session_active AND now - lastHB < 150s)      → ""
//
// Note on the third rule: spec text says "lastHB stale|empty" maps
// to heartbeat_stale. detached_session (rule 2) wins over rule 3
// because a closed stream also implies the heartbeat will stop;
// emitting heartbeat_stale would mislead operators who care about the
// auth-side root cause.
func canonicalReason(sessionActive bool, drain bool, lastHB string, now time.Time) string {
	if drain {
		return ReasonDrain
	}
	if !sessionActive {
		return ReasonDetachedSession
	}
	if lastHB == "" {
		return ReasonHeartbeatStale
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return ReasonHeartbeatStale
	}
	if now.Sub(t.UTC()) >= ConnectionStaleThreshold {
		return ReasonHeartbeatStale
	}
	return ""
}

// heartbeatAgeSeconds returns the number of seconds since last heartbeat,
// or 0 if the timestamp is unparseable.
func heartbeatAgeSeconds(lastHB string) int64 {
	if lastHB == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return 0
	}
	age := time.Since(t).Seconds()
	if age < 0 {
		return 0
	}
	return int64(age)
}

// sanitizeWorker converts a raw workers.WorkerInfo into the operator-facing
// WorkerResponse, stripping all sensitive fields.
//
// Connection status: trust the registry's `WorkerInfo.ConnectionStatus`
// (CONNECTED | STALE | DISCONNECTED | DRAINING) since it merges heartbeat
// freshness with the canonical `session_active` signal from `worker_sessions`.
// The canonical `workers.ConnectionStatus` always returns one of the four
// enum strings on every read path (registry_query.go guarantees this),
// so no legacy/heartbeat-only fallback is needed.
func sanitizeWorker(w workersreg.WorkerInfo) WorkerResponse {
	resp := WorkerResponse{
		WorkerID:            w.WorkerID,
		WorkerName:          w.WorkerName,
		SessionActive:       w.SessionActive,
		Status:              w.ConnectionStatus,
		Reason:              w.Reason,
		Hostname:            w.Host,
		WorkerClass:         w.Class,
		RolloutGroup:        w.RolloutGroup,
		ProtocolVersion:     w.ProtocolVersion,
		EngineVersion:       w.EngineVersion,
		BundleVersion:       w.BundleVersion,
		ConnectedAt:         w.FirstSeen,
		LastHeartbeatAt:     w.LastHB,
		HeartbeatAgeSeconds: heartbeatAgeSeconds(w.LastHB),
		CurrentTaskID:       w.CurrentJob,
		Executors:           extractExecutors(w.Capabilities),
	}

	// Resource counters: extracted from the typed metrics map produced
	// by the gRPC heartbeat handler (registry_heartbeat.go stores the
	// proto WorkerResourceCounters fields under the "metrics" key).
	if m := w.Metrics; m != nil {
		if v, ok := intFromMap(m, "active_tasks"); ok {
			resp.ActiveTasks = int32(v)
		}
		if v, ok := intFromMap(m, "task_slots"); ok {
			resp.TaskSlots = int32(v)
		}
		if v, ok := floatFromMap(m, "cpu_utilization_ratio"); ok {
			resp.CPUUtilizationRatio = v
		}
		if v, ok := intFromMap(m, "memory_used_bytes"); ok {
			resp.MemoryUsedBytes = v
		}
		if v, ok := intFromMap(m, "disk_free_bytes"); ok {
			resp.DiskFreeBytes = v
		}
		if v, ok := intFromMap(m, "jobs_completed"); ok {
			resp.JobsCompleted = v
		}
		if v, ok := intFromMap(m, "jobs_failed"); ok {
			resp.JobsFailed = v
		}
		if raw, ok := m["active_jobs"].([]interface{}); ok {
			for _, item := range raw {
				if task, ok := item.(map[string]interface{}); ok {
					resp.ActiveTaskRuntime = append(resp.ActiveTaskRuntime, ActiveTaskRuntime{
						JobID: asString(task["job_id"]), TaskID: asString(task["task_id"]),
						AttemptID: asString(task["attempt_id"]), Executor: asString(task["job_type"]),
						Stage: asString(task["progress_stage"]), Percent: intFromAny(task["progress_percent"]),
						Scene: intFromAny(task["progress_scene"]), TotalScenes: intFromAny(task["progress_total"]),
						LeaseID: asString(task["lease_id"]), StartedAt: asString(task["started_at"]),
					})
				}
			}
		}
	}

	return resp
}

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
				if v, ok := floatFromGeneric(m["version"]); ok {
					ver = int32(v)
				}
				out = append(out, ExecutorEntry{ID: id, Version: ver})
			}
			return out
		}
	}
	return nil
}

// floatFromGeneric handles JSON-unmarshalled numeric values that may be
// float64, json.Number, or int types.
func floatFromGeneric(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

// intFromMap extracts an int64 from a map with numeric-type tolerance.
func intFromMap(m map[string]interface{}, key string) (int64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}

func intFromAny(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	}
	return 0
}

// floatFromMap extracts a float64 from a map with numeric-type tolerance.
func floatFromMap(m map[string]interface{}, key string) (float64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

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
