// Package netsecurity is the single source of truth for network-address
// classification: which IPs must never be used as an egress/ingress fetch
// target (loopback, private, link-local, multicast, unspecified, and the
// explicit non-public CIDR ranges).
//
// It exists because the same disallow-list was previously duplicated in two
// places — internal/inputsecurity (disallowedIP) and
// internal/handlers/server/pipeline (classifyIP) — and had already drifted
// (one blocked RFC 6598 CGNAT + benchmarking ranges, the other did not; one
// normalized IPv4-mapped IPv6 before the CIDR match, the other did not).
// Both consumers now call DisallowedIP so the policy can only change in one
// place.
package netsecurity

import "net"

// disallowedCIDRs are non-public ranges that the stdlib IP-class predicates
// do not cover (carrier-grade NAT, IETF benchmarking, link-local, and
// cloud-metadata). Sorted by prefix length descending is not required —
// net.IPNet.Contains handles overlapping ranges — but each entry is a
// /-suffixed CIDR so ParseCIDR failures are impossible unless the literal
// itself is edited incorrectly.
var disallowedCIDRs = []string{
	"100.100.100.200/32", // Alibaba cloud metadata endpoint
	"192.0.0.0/24",       // IETF protocol assignments / benchmarking
	"198.18.0.0/15",      // benchmarking (RFC 2544)
	"100.64.0.0/10",      // RFC 6598 carrier-grade NAT (shared address space)
	"169.254.0.0/16",     // IPv4 link-local (superset of 169.254.169.254)
}

// disallowedNetworks is the pre-parsed projection of disallowedCIDRs.
// Parsed exactly once at init so DisallowedIP never re-runs net.ParseCIDR
// (allocation + parse) on the SSRF/dial hot path. The string slice remains
// the single human-readable source; the parsed slice cannot drift because
// it is derived from it at package init.
var disallowedNetworks = func() []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(disallowedCIDRs))
	for _, cidr := range disallowedCIDRs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			// A literal edit typo must fail at init, never silently weaken
			// the blocklist at runtime.
			panic("netsecurity: invalid disallowed CIDR " + cidr + ": " + err.Error())
		}
		nets = append(nets, network)
	}
	return nets
}()

// DisallowedIP reports whether ip must not be used as a fetch target.
//
// It first normalizes IPv4-mapped IPv6 (e.g. [::ffff:10.0.0.1]) to its
// 4-byte form so that both the stdlib predicates and the CIDR blocklist see
// the same address shape. Without this, net.ParseCIDR("10.0.0.0/8").Contains
// returns false for a 16-byte mapped address and the block is silently
// bypassed.
//
// A nil ip is treated as disallowed (fail closed): callers must never allow
// a nil address through.
func DisallowedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}

	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
		return true
	}

	for _, network := range disallowedNetworks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
