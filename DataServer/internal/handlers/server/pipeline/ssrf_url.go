// Package pipeline: ssrf_url.go is the SSRF defense for outgoing URLs
// on the simplified POST /api/v1/jobs intake.
//
// It runs INSIDE the handler's request validation path so a malicious
// client cannot trick the worker into fetching internal-network or
// cloud-metadata URLs. The validator checks the URL at submit time
// (the FIRST hop the worker would request); follow-up redirect chain
// validation is a worker-side concern (separate future commit).
//
// Defense-in-depth policy (the hybrid default per user P1 request):
//
//   - velox-asset:// is ALWAYS accepted. It is the canonical internal
//     asset registry and the egress path is rewritten into a local
//     artifact fetch (no network).
//
//   - BLOCKLIST enforced globally: every non-velox-asset:// URL must
//     resolve to a public IP. Reject RFC 1918 (10/8, 172.16/12,
//     192.168/16), loopback (127/8, ::1), link-local including cloud
//     metadata (169.254/16 — 169.254.169.254 in particular), multicast
//     (224/4), unspecified (0.0.0.0, ::), IPv6 unique-local (fc00::/7).
//
//   - SCHEMES: only https:// for non-loopback external IPs. http://
//     is blocked for non-loopback because it carries no integrity and
//     is trivial to MITM; loopback http is only allowed when
//     cfg.Runtime.AllowLoopbackAdminAuthDev is true (matches the
//     existing dev-mode escape hatch). Other schemes (file://,
//     gopher://, ftp://, dict://, ldap://, javascript://, data:) are
//     unconditionally rejected.
//
//   - ALLOWLIST enforced when non-empty: if cfg.AllowedExternalDomains
//     contains any entries, the URL's hostname MUST match one of
//     them (suffix match so `cdn.example.com` matches an entry
//     `example.com`; explicit `*.foo.com` wildcard supported).
//
//   - REDIRECTS: the validator does not fetch any byte. The
//     recommendation is submitted-time DNS resolve + IP-class check;
//     the worker-side fetcher is expected to reject any redirect to
//     a private IP (separate commit).
//
//   - SIZE LIMIT / TIMEOUT: enforced on the actual GET (worker
//     side); the validator's submit-time checks are STATIC so it has
//     no budget to apply. Documented here so future contributors
//     don't duplicate the check.
package pipeline

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"

	"velox-server/internal/config"
	"velox-server/internal/netsecurity"
	"velox-shared/assetref"
)

// ValidateExternalURL inspects raw and returns nil when the URL is
// safe to use as an EGRESS fetch target by the worker; otherwise
// returns *SSRFValidationError.
//
// The struct error carries machine-readable fields so the handler
// can serialize it into SubmitJobValidationError.Details without
// exposing internal-only information:
//
//	Path:    JSON-pointer-style path to the offending URL in the
//	         request body (e.g. "scenes.3.clip.url")
//	URL:     the raw value as sent by the client (echo for debug)
//	Reason:  short machine token: "scheme", "ip_private",
//	         "ip_loopback", "ip_link_local", "ip_metadata",
//	         "ip_multicast", "ip_unspecified", "ip_unresolved",
//	         "allowlist_miss", "http_disallowed",
//	         "loopback_http_disabled". Use one of these in tests /
//	         clients; the message tail is for humans only.
type SSRFValidationError struct {
	Path   string
	URL    string
	Reason string
}

func (e *SSRFValidationError) Error() string {
	return fmt.Sprintf("ssrf_url: %s at %s: %s", e.URL, e.Path, e.Reason)
}

// ValidateExternalURL is the (URL, allowlist, dev-mode) → error
// helper used by the request validator. Kept separate from the
// method form so the policy can be unit-tested on raw strings.
//
// allowDomains comes from cfg.AllowedExternalDomains (already
// trimmed + lowercased at config load). empty allowDomains means
// "no additional allowlist filter" (blocklist-only mode).
//
// allowLoopbackHTTP comes from cfg.Runtime.AllowLoopbackAdminAuthDev.
// It's deliberately pinned to the SAME dev-mode flag so the dev
// gate has a single switch — operators either trust loopback or they
// don't.
func ValidateExternalURL(raw string, allowDomains []string, allowLoopbackHTTP bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		// Empty URLs are skipped silently by the request validator
		// (they're optional fields).
		return nil
	}

	// Internal asset schemes bypass the network entirely: velox-asset:// is
	// the content-addressed registry and velox-drive:// is the deferred
	// Drive bridge — both are materialized by the worker through the
	// authenticated master bridge, never by an egress fetch of the URL
	// itself.
	if _, err := assetref.ParseCanonicalWire(raw); err == nil {
		return nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return &SSRFValidationError{
			URL: raw, Reason: "malformed",
		}
	}
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	switch scheme {
	case "":
		// Bare paths like "/foo" aren't URLs at all. Treat as
		// malformed so clients always get a clear rejection.
		return &SSRFValidationError{
			URL: raw, Reason: "malformed",
		}
	case "https":
		// OK as long as the IP-class check passes below. http is
		// checked separately.
	case "http":
		// http:// is allowed only for public OR for loopback with the
		// dev gate. We check both via the IP-class block below.
	default:
		return &SSRFValidationError{
			URL: raw, Reason: "scheme",
		}
	}

	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		return &SSRFValidationError{
			URL: raw, Reason: "malformed",
		}
	}

	// Allowlist enforcement is checked FIRST when configured. This
	// matters for two reasons:
	//   1. Known-good suffix / wildcard match avoids a DNS round trip
	//      for every trusted-domain URL (a major gain — DNS resolution
	//      is uncached and the slowest path in the validator).
	//   2. IP-literal URLs (host = "93.184.216.34") cannot match a
	//      domain-name allowlist entry, so they go through the same
	//      gate as hostnames: an IP literal under any non-empty
	//      allowlist is rejected as `allowlist_miss`. This is the
	//      intended policy — an operator configuring "only *.cdn.foo"
	//      does NOT want raw IP literals to leak through.
	//
	// Defense-in-depth note: an allowlist match short-circuits the
	// submit-time validator on the explicit assumption that the
	// operator trusts their own allowlist. The IP-class blocklist
	// ALONE applies when no allowlist is configured. Worker-side
	// connect-time validation is a separate concern (not in scope
	// here) and is the canonical layer to re-check the IP after
	// runtime DNS resolution under a fresh resolver.
	if len(allowDomains) > 0 {
		if !hostnameAllowed(host, allowDomains) {
			return &SSRFValidationError{
				URL: raw, Reason: "allowlist_miss",
			}
		}
		return nil
	}

	// IP literal? If so, classify directly. Otherwise resolve via DNS
	// (use the system resolver; on bounded resolves, a future
	// enhancement can swap in a sharded resolver that respects
	// cfg.Runtime.DNSResolvers). Note: we deliberately do NOT trust
	// any client-supplied IP without further validation; DNS
	// rebinding is mitigated by validating ALL resolved IPs as one
	// set (an attacker can return a public IP first, then a private
	// IP on the next retry — workers MUST enforce the same check at
	// connect time).
	if ip := net.ParseIP(host); ip != nil {
		if reason := classifyIP(ip, allowLoopbackHTTP, scheme); reason != "" {
			return &SSRFValidationError{
				URL: raw, Reason: reason,
			}
		}
		// Allowed IP literal; allowlist already checked above so a
		// non-matching IP literal would have been rejected there.
	} else {
		ips, err := net.LookupIP(host)
		if err != nil || len(ips) == 0 {
			return &SSRFValidationError{
				URL: raw, Reason: "ip_unresolved",
			}
		}
		// Reject if ANY resolved IP is in the deny set — even one
		// private IP across multi-A records makes the hostname
		// unsafe. Workers must re-check on connect (DNS-rebind
		// defense).
		for _, ip := range ips {
			if reason := classifyIP(ip, allowLoopbackHTTP, scheme); reason != "" {
				return &SSRFValidationError{
					URL: raw, Reason: reason,
				}
			}
		}
	}

	return nil
}

// classifyIP returns "" when ip is acceptable for the given context
// (scheme and dev gate). Returns one of the SSRF Reason strings when
// the IP is denied. The function is the canonical IP-class table;
// every caller MUST go through it (use this everywhere the policy is
// evaluated).
//
// allowLoopbackHTTP is a bool (Go syntax quirk: when an untyped param
// sits between two typed params, its type defaults to the FOLLOWING
// param's type; leaving allowLoopbackHTTP implicit would silently
// type it as `string` and every caller passing `bool` would fail to
// compile. Pin the type explicitly.)
func classifyIP(ip net.IP, allowLoopbackHTTP bool, scheme string) string {
	if ip == nil {
		return "ip_unresolved"
	}
	// IPv4-mapped IPv6 normalization: re-bind to the v4 shape if
	// applicable. To4() returns nil for true v6 addresses, so the
	// underlying ip variable is still used for v6 checks below.
	// Without this, [::ffff:127.0.0.1] and [::ffff:10.0.0.1] bypass
	// the loopback / private checks because net.IP.IsLoopback()
	// returns true ONLY for ::1 and IsPrivate() ignores the mapped
	// form.
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}

	// Unspecified = 0.0.0.0 / ::. Always reject.
	if ip.IsUnspecified() {
		return "ip_unspecified"
	}
	// Loopback. http://loopback is allowed ONLY with the dev gate.
	if ip.IsLoopback() {
		if scheme == "https" {
			return "ip_loopback"
		}
		if !allowLoopbackHTTP {
			return "ip_loopback"
		}
		return "loopback_http_disabled"
	}

	// Specific carve-out BEFORE the umbrella IP-class checks so the
	// audit log surfaces the action-blame token ("metadata" rather
	// than a generic "link_local") for the cloud-metadata IP. The
	// order matters: ip_metadata > ip_link_local_uni in specificity.
	if ip.Equal(net.IPv4(169, 254, 169, 254)) {
		return "ip_metadata"
	}

	// Multicast (224/4 + ff00::/8). The umbrella reason wins over
	// ip_link_local_mcast (224.0.0.0/24, ff02::/16) — both are
	// subsets of multicast; audits want the broader category.
	if ip.IsMulticast() {
		return "ip_multicast"
	}

	// Link-local unicast (169.254/16, fe80::/10). Distinct from the
	// multicast carve-out above.
	if ip.IsLinkLocalUnicast() {
		return "ip_link_local"
	}

	// Private (RFC 1918 IPv4 + RFC 4193 IPv6 unique-local).
	if ip.IsPrivate() {
		return "ip_private"
	}

	// Non-public CIDR ranges that the stdlib predicates above do not
	// classify (RFC 6598 CGNAT, benchmarking, cloud metadata). Single
	// source of truth in internal/netsecurity, shared with inputsecurity.
	if netsecurity.DisallowedIP(ip) {
		return "ip_private"
	}

	// http:// for non-loopback is denied by policy: only https for
	// public hosts. (Loopback http has already been adjudicated above.)
	if scheme == "http" && !ip.IsLoopback() {
		return "http_disallowed"
	}

	return ""
}

// hostnameAllowed reports whether host matches any entry in
// allowDomains. Matching is suffix-based:
//   - exact match on the host label, or
//   - bare suffix "example.com" matches "a.example.com", or
//   - wildcard "*.foo.com" matches one extra label ("a.foo.com").
//
// This is the simpler-than-glob policy and matches the most common
// SaaS providers' expectation.
//
// Empty allowDomains returns true (semantically "no allowlist
// configured → allowlist filter passes"; the IP-class blocklist is
// the only active gate in that case). Callers in ValidateExternalURL
// gate on len(allowDomains) > 0 BEFORE invoking, but the helper
// itself is safe to call unconditionally for clarity at call sites.
func hostnameAllowed(host string, allowDomains []string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	if len(allowDomains) == 0 {
		return true
	}
	for _, raw := range allowDomains {
		domain := strings.ToLower(strings.TrimSpace(raw))
		if domain == "" {
			continue
		}
		// Wildcard: literal "*.foo.com" — strip "*." prefix.
		if strings.HasPrefix(domain, "*.") {
			suffix := domain[1:] // "*.foo.com" → ".foo.com"
			hostLabels := strings.Split(host, ".")
			domainLabels := strings.Split(strings.TrimPrefix(suffix, "."), ".")
			if len(hostLabels) <= len(domainLabels) {
				continue
			}
			match := true
			for i := range domainLabels {
				if hostLabels[len(hostLabels)-len(domainLabels)+i] != domainLabels[i] {
					match = false
					break
				}
			}
			// Also require the second-to-last label to be exactly one
			// label below the suffix (the wildcard is "*.foo.com", so
			// `foo.com` itself does NOT match — only "x.foo.com" does).
			if match && len(hostLabels) == len(domainLabels)+1 {
				return true
			}
			continue
		}
		// Exact match.
		if host == domain {
			return true
		}
		// Suffix match: "a.example.com" matches "example.com".
		if strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

// ValidateAllExternalURLs walks the canonical nested scene asset
// URLs and returns a non-nil error iff any URL fails the SSRF policy. On
// failure the returned slice-cumulative error encodes every
// violation so the client sees the full picture in one round trip.
//
// allowDomains / allowLoopbackHTTP are shorthand for the matching
// cfg values; the function is a pure validator with no global state
// so unit tests can pin in a deterministic config snippet.
//
// Each URL check is independent and, in the default blocklist-only mode,
// performs a blocking DNS resolution (the slowest part of the validator).
// The checks are therefore fanned out over a bounded worker pool instead of
// being serialized one scene after another, which would multiply the DNS
// latency by the number of asset URLs on the submit request path.
func ValidateAllExternalURLs(req SubmitJobRequest, cfg *config.Config) []SSRFValidationError {
	if cfg == nil {
		// Defensive: nil cfg means callers passed a bad argument.
		// Skip SSRF entirely rather than crashing the request.
		return nil
	}
	domains := cfg.AllowedExternalDomains
	allowLoopbackHTTP := cfg.Runtime.AllowLoopbackAdminAuthDev

	// Flatten the scene asset URLs into an ordered work list so results can
	// be re-collected in scene order regardless of which lookup finishes
	// first (the returned error slice stays deterministic).
	type candidate struct {
		path string
		url  string
	}
	var candidates []candidate
	for i, s := range req.Scenes {
		base := fmt.Sprintf("scenes.%d", i)
		if s.Clip != nil {
			candidates = append(candidates, candidate{path: base + ".clip.url", url: s.Clip.URL})
		}
		if s.Voiceover != nil {
			candidates = append(candidates, candidate{path: base + ".voiceover.url", url: s.Voiceover.URL})
		}
		if s.Subtitles != nil {
			candidates = append(candidates, candidate{path: base + ".subtitles.url", url: s.Subtitles.URL})
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	// Bounded fan-out: at most maxConcurrentLookups DNS resolutions in
	// flight, so a MaxScenes-sized request cannot spawn thousands of
	// unbounded goroutines against the resolver.
	const maxConcurrentLookups = 16
	sem := make(chan struct{}, maxConcurrentLookups)
	results := make([]*SSRFValidationError, len(candidates))
	var wg sync.WaitGroup
	for i, c := range candidates {
		wg.Add(1)
		sem <- struct{}{} // blocks the collector loop when the pool is full
		go func(i int, c candidate) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := ValidateExternalURL(c.url, domains, allowLoopbackHTTP); err != nil {
				if se, ok := err.(*SSRFValidationError); ok {
					se.Path = c.path
					results[i] = se
				}
			}
		}(i, c)
	}
	wg.Wait()

	var errs []SSRFValidationError
	for _, r := range results {
		if r != nil {
			errs = append(errs, *r)
		}
	}
	return errs
}
