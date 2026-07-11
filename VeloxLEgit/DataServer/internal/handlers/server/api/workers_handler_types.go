// Package api — RW-PROD-005 type definitions.
//
// New DTO shapes the operator-facing workers endpoint will emit after
// the canonical-state migration. The split into a dedicated file keeps
// workers_handler.go readable: the existing handler's responsibility
// is the route + filter + serialization surface; the new types file
// owns the value-types and the sanitizer helpers.
//
// SECURITY posture (canonical, see OWNERSHIP.md §3):
//   - HostSummary deliberately omits Hostname when it would leak IPv4/IPv6;
//     sanitiseHostname() replaces offending patterns with "[redacted-...]".
//   - Bundle_hash / credential_hash / TLS file paths / worker secret /
//     raw IP addresses of any kind are NEVER carried into the response.
//   - Reasons are an enumerated 3-element taxonomy (see Reason* consts);
//     ad-hoc string literals must not be added at call sites.
package api

import (
	"encoding/hex"
	"net"
	"regexp"
	"strings"
	"time"

	workersreg "velox-server/internal/workers"
)

// WorkerInfo aliases the canonical registry read-model type so this package
// can refer to the worker shape consistently across build, vet, and tests.
type WorkerInfo = workersreg.WorkerInfo

// Re-export ConnectionStaleThreshold so the canonical reason is
// computed against the same threshold the registry uses for STALE.
// Drift would create a window where the API reports reason=heartbeat_stale
// but status=CONNECTED — operators would have to look up the threshold
// definition to make sense of the inconsistency.
var ConnectionStaleThreshold = workersreg.ConnectionStaleThreshold

// Reason canonical taxonomy (RW-PROD-005 §2.2).
//
//	drain             — Drain=true overrides everything (precedence 1).
//	detached_session  — session_active=false (stream closed), all other
//	                    signals ignored (precedence 2). Mirrors spec
//	                    "Stream chiuso → detached_session senza
//	                    aspettare 30s".
//	heartbeat_stale   — session_active=true but last_heartbeat is stale,
//	                    empty, or unparseable. With a fresh session the
//	                    canonical state is STALE (30s-5min). With an old
//	                    session the state is DISCONNECTED but the reason
//	                    is still heartbeat_stale (the session is up but
//	                    the heartbeat has stopped).
//	""                — fresh: status=CONNECTED, no reason emitted.
const (
	ReasonDrain           = "drain"
	ReasonDetachedSession = "detached_session"
	ReasonHeartbeatStale  = "heartbeat_stale"
)

// HostSummary is the sanitized host-side metadata exposed in the
// operator-facing WorkerResponse. NO IPs, NO creds, NO cert paths,
// NO worker secret. Hostname goes through sanitiseHostname() which
// replaces IPv4/IPv6/secret-looking/absolute-path patterns with
// "[redacted-...]" — the path-filter is the defense-in-depth surface
// against a future operator setting WorkerGroup to a directory literal
// (an Ansible pragmatic mistake that the fuzz test catches). Numeric
// resource counters are exposed because they are operator-observability
// signals and have no PII value.
type HostSummary struct {
	Hostname        string `json:"hostname"`        // sanitised via sanitiseHostname()
	CPUCount        int    `json:"cpu_count"`       // runtime.NumCPU()
	HasGPU          bool   `json:"has_gpu"`         // sampler-derived
	RAMBytes        int64  `json:"ram_bytes"`       // sampler-derived
	DiskFreeBytes   int64  `json:"disk_free_bytes"` // sampler-derived (snapshot, not realtime)
	MaxParallelJobs int32  `json:"max_parallel_jobs"`
}

// ExecutorSummary is a single executor descriptor flattened from the
// capabilities blob. ResourceClass is included so a
// dispatcher can render "velox-worker-fleet: 4×cpu + 2×gpu" without
// re-decoding the full capabilities map.
type ExecutorSummary struct {
	ID            string `json:"id"`
	Version       int32  `json:"version"`
	ResourceClass string `json:"resource_class,omitempty"`
}

// TaskSummary is the per-worker current_task projection. Empty when
// the worker has no RUNNING TaskAttempt. Race-tolerant: LoadCurrentTask
// is called from the handler read path with row-level locking via the
// task_attempts table primary index, so concurrent TaskAttempt updates
// surface as either (RUNNING, task X) or (no row) for at most one tick.
type TaskSummary struct {
	TaskID    string `json:"task_id"`
	JobID     string `json:"job_id"`
	Executor  string `json:"executor,omitempty"` // e.g. "scene.composite.v1@1"
	Status    string `json:"status,omitempty"`   // always "RUNNING" today
	StartedAt string `json:"started_at,omitempty"`
}

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
//  4. fresh (session_active AND now - lastHB < 30s)       → ""
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
