package api

import (
	"encoding/hex"
	"net"
	"regexp"
	"strings"
)

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
	out := redactIPv4(host)
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
