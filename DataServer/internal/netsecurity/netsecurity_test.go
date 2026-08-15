package netsecurity

import (
	"net"
	"testing"
)

// TestDisallowedIP pins the unified non-public blocklist so a future edit
// to the stdlib predicates or the CIDR list cannot silently weaken the
// policy shared by inputsecurity and the pipeline SSRF validator.
func TestDisallowedIP(t *testing.T) {
	cases := []struct {
		name    string
		ip      string
		blocked bool
	}{
		// Nil is fail-closed.
		{"nil", "", true},

		// stdlib IP classes.
		{"unspecified_v4", "0.0.0.0", true},
		{"unspecified_v6", "::", true},
		{"loopback_v4", "127.0.0.1", true},
		{"loopback_v6", "::1", true},
		{"private_10", "10.0.0.1", true},
		{"private_192_168", "192.168.1.1", true},
		{"private_172_16", "172.16.0.1", true},
		{"private_v6_ula", "fc00::1", true},
		{"link_local_v4", "169.254.10.20", true},
		{"link_local_v6", "fe80::1", true},
		{"multicast_v4", "224.0.0.1", true},
		{"multicast_v6", "ff02::1", true},

		// Explicit non-public CIDR ranges (not stdlib-classified).
		{"cgnat", "100.64.0.1", true},
		{"benchmark_192_0", "192.0.0.1", true},
		{"benchmark_198_18", "198.18.0.1", true},
		{"aws_metadata", "169.254.169.254", true},
		{"alibaba_metadata", "100.100.100.200", true},

		// IPv4-mapped IPv6 must be normalized before the CIDR match so a
		// mapped private/CGNAT address cannot bypass the blocklist.
		{"mapped_private", "::ffff:10.0.0.1", true},
		{"mapped_cgnat", "::ffff:100.64.0.1", true},

		// Public destinations must remain allowed.
		{"public_1", "93.184.216.34", false},
		{"public_2", "8.8.8.8", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ip net.IP
			if tc.ip != "" {
				ip = net.ParseIP(tc.ip)
				if ip == nil {
					t.Fatalf("net.ParseIP(%q) returned nil", tc.ip)
				}
			}
			if got := DisallowedIP(ip); got != tc.blocked {
				t.Errorf("DisallowedIP(%s) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}
