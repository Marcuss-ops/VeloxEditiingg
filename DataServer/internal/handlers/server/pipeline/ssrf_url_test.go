// Package pipeline: ssrf_url_test.go is the unit-test surface for the
// SSRF URL validator. Each test exercises ONE specific deny reason from
// the hybrid blocklist+allowlist policy so a regression names the rule
// in the failure message rather than just "TestValidateExternalURL_X
// failed".
//
// The tests do NOT touch the network: they pin the validator's
// "hostname resolves to IP X via net.LookupIP" assumption with a
// no-network `host` argument by always passing IP literals where DNS
// resolution would matter, OR using the suffix/label form (matching
// `hostnameAllowed` exactly) when the policy is destination-agnostic.
package pipeline

import (
	"strings"
	"testing"
)

// TestValidateExternalURL_HappyPaths asserts the allow-list of
// trusted URLs passes regardless of allowDomains content.
func TestValidateExternalURL_HappyPaths(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"velox_asset_prefix", "velox-asset://voiceovers/introduction.mp3"},
		{"velox_asset_with_path_and_query",
			"velox-asset://voiceovers/foo/bar.mp3?rev=42"},
		{"https_public_ip_literal", "https://93.184.216.34/resource.mp4"},
		{"https_public_hostname",
			// We don't resolve DNS here (no network) — but the hostname
			// goes through `hostnameAllowed` when allowDomains is set.
			// With empty allowDomains, the IP-class block applies only
			// after DNS resolves. Pass an int literal to skip DNS.
			// (This case uses IP literal; DNS-resolved hostname tests
			// are in TestValidateExternalURL_HostnameAllowedMatch.)
			"https://93.184.216.34/path?q=1"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateExternalURL(tc.url, nil, false); err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

// TestValidateExternalURL_RejectsPrivateIPs runs every IP-class in
// the blocklist through ValidateExternalURL.
func TestValidateExternalURL_RejectsPrivateIPs(t *testing.T) {
	cases := []struct {
		name       string
		url        string
		wantReason string
	}{
		{"loopback_ipv4", "https://127.0.0.1/x", "ip_loopback"},
		{"loopback_ipv6", "https://[::1]/x", "ip_loopback"},
		{"aws_metadata", "https://169.254.169.254/latest/meta-data/", "ip_metadata"},
		{"link_local_random", "https://169.254.10.20/x", "ip_link_local"},
		{"private_10", "https://10.0.0.1/x", "ip_private"},
		{"private_172", "https://172.16.5.5/x", "ip_private"},
		{"private_192", "https://192.168.1.1/x", "ip_private"},
		{"ipv6_unique_local", "https://[fc00::1]/x", "ip_private"},
		{"multicast", "https://224.0.0.1/x", "ip_multicast"},
		{"unspecified_ipv4", "https://0.0.0.0/x", "ip_unspecified"},
		{"unspecified_ipv6", "https://[::]/x", "ip_unspecified"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExternalURL(tc.url, nil, false)
			if err == nil {
				t.Fatalf("want error reason=%q, got nil", tc.wantReason)
			}
			se, ok := err.(*SSRFValidationError)
			if !ok {
				t.Fatalf("want *SSRFValidationError, got %T", err)
			}
			if se.Reason != tc.wantReason {
				t.Fatalf("want reason=%q, got %q (full=%v)", tc.wantReason, se.Reason, se)
			}
		})
	}
}

// TestValidateExternalURL_RejectsSchemes covers scheme denial.
func TestValidateExternalURL_RejectsSchemes(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"file", "file:///etc/passwd"},
		{"gopher", "gopher://example.com/_"},
		{"ftp", "ftp://example.com/x"},
		{"dict", "dict://example.com/x"},
		{"ldap", "ldap://example.com/x"},
		{"javascript", "javascript://alert(1)"},
		{"data", "data:text/plain;base64,SGVsbG8="},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExternalURL(tc.url, nil, false)
			if err == nil {
				t.Fatalf("want error, got nil for %q", tc.url)
			}
			se, ok := err.(*SSRFValidationError)
			if !ok {
				t.Fatalf("want *SSRFValidationError, got %T", err)
			}
			if se.Reason != "scheme" {
				t.Fatalf("want reason=scheme, got %q", se.Reason)
			}
		})
	}
}

// TestValidateExternalURL_HTTPOnlyOnLoopback covers the http rule.
func TestValidateExternalURL_HTTPOnlyOnLoopback(t *testing.T) {
	// http://public → http_disallowed.
	if err := ValidateExternalURL("http://93.184.216.34/x", nil, false); err == nil {
		t.Fatal("want error, got nil for http://public")
	} else if se, ok := err.(*SSRFValidationError); !ok || se.Reason != "http_disallowed" {
		t.Fatalf("want http_disallowed, got %v", err)
	}

	// http://loopback without dev gate → ip_loopback.
	if err := ValidateExternalURL("http://127.0.0.1/x", nil, false); err == nil {
		t.Fatal("want error, got nil for http://loopback")
	} else if se, ok := err.(*SSRFValidationError); !ok || se.Reason != "ip_loopback" {
		t.Fatalf("want ip_loopback, got %v", err)
	}

	// http://loopback WITH dev gate → still rejected (dev gate does
	// not relax SSRF — the user spec was explicit on this).
	if err := ValidateExternalURL("http://127.0.0.1/x", nil, true); err == nil {
		t.Fatal("want error, got nil for http://loopback with dev gate")
	}
}

// TestValidateExternalURL_AllowlistRequired covers the hybrid
// behavior: when allowDomains is non-empty, only matching hosts are
// accepted (blocklist still applies in addition).
func TestValidateExternalURL_AllowlistRequired(t *testing.T) {
	allow := []string{"cdn.example.com", "*.trusted.io"}

	cases := []struct {
		name     string
		url      string
		wantPass bool
	}{
		// IP literal: bypasses hostname check but still hits IP-class
		// blocklist when private. The public IP literal passes the
		// IP blocklist BUT fails the allowlist because the host-label
		// doesn't match.
		{"public_ip_literal_blocked_by_allowlist",
			"https://93.184.216.34/x", false},
		// Suffix match
		{"exact_match", "https://cdn.example.com/x", true},
		{"suffix_match", "https://us-east.cdn.example.com/x", true},
		// ".trusted.io" wildcard pattern.
		{"wildcard_match", "https://api.trusted.io/x", true},
		{"wildcard_bare_match", "https://trusted.io/x", false},
		// Disallowed hostname.
		{"not_allowed", "https://attacker.example/x", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExternalURL(tc.url, allow, false)
			if tc.wantPass {
				if err != nil {
					t.Fatalf("want pass, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want reject, got nil for %q", tc.url)
			}
			se, _ := err.(*SSRFValidationError)
			if se == nil {
				t.Fatalf("want *SSRFValidationError, got %T", err)
			}
			// The reject reason could be either allowlist_miss or
			// ip_xxx depending on which guard fires first. Both are
			// valid rejections — only nil is wrong.
			if se.Reason == "" {
				t.Fatalf("want non-empty reason, got %v", se)
			}
		})
	}
}

// TestValidateExternalURL_MalformedURLs covers parse failures.
func TestValidateExternalURL_MalformedURLs(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"not-a-url",
		":noscheme",
		"://noscheme",
	}
	for _, raw := range cases {
		// Empty / whitespace-only URLs are NOT errors; the validator
		// returns nil and the request validator decides whether to
		// require the field.
		if strings.TrimSpace(raw) == "" {
			if err := ValidateExternalURL(raw, nil, false); err != nil {
				t.Fatalf("want nil for empty URL, got %v", err)
			}
			continue
		}
		t.Run("malformed_"+raw, func(t *testing.T) {
			if err := ValidateExternalURL(raw, nil, false); err == nil {
				t.Fatalf("want error for %q, got nil", raw)
			}
		})
	}
}

// TestHostnameAllowed_Policy covers all suffix / wildcard branches
// independently of the IP-class blocklist. We never go to network:
// every input is a literal label.
func TestHostnameAllowed_Policy(t *testing.T) {
	cases := []struct {
		host   string
		allow  []string
		wantOK bool
	}{
		// Empty / blank inputs.
		{"", []string{"foo.com"}, false},
		{"foo.com", nil, true}, // no allowlist = pass
		{"foo.com", []string{}, true},

		// Exact.
		{"foo.com", []string{"foo.com"}, true},
		{"bar.com", []string{"foo.com"}, false},

		// Suffix.
		{"a.foo.com", []string{"foo.com"}, true},
		{"foo.com", []string{"foo.com"}, true},
		{"foo.com.example.com", []string{"example.com"}, true},

		// Wildcard.
		{"a.foo.com", []string{"*.foo.com"}, true},
		{"b.a.foo.com", []string{"*.foo.com"}, false}, // too deep
		{"foo.com", []string{"*.foo.com"}, false},      // bare domain
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.host+"_vs_"+strings.Join(tc.allow, ","), func(t *testing.T) {
			got := hostnameAllowed(tc.host, tc.allow)
			if got != tc.wantOK {
				t.Fatalf("hostnameAllowed(%q, %v) = %v, want %v", tc.host, tc.allow, got, tc.wantOK)
			}
		})
	}
}
