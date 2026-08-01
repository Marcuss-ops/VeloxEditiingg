package inputsecurity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Policy is the security policy applied to every acquired input.
type Policy struct {
	MaxBytes              int64
	MaxRedirects          int
	DNSLookupTimeout      time.Duration
	ConnectTimeout        time.Duration
	ResponseHeaderTimeout time.Duration
	TransferTimeout       time.Duration
	ProbeTimeout          time.Duration
	TempDir               string
	QuarantineDir         string
	AllowedRoots          []string
	Resolver              IPResolver
	Transport             http.RoundTripper
	Metrics               *Metrics

	// AllowPrivateNetworks exists solely for hermetic unit tests that rewrite
	// a public provider hostname to httptest.Server. Production construction
	// never enables it.
	AllowPrivateNetworks bool
}

type IPResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

func DefaultPolicy() Policy {
	return Policy{
		MaxBytes:              256 * 1024 * 1024,
		MaxRedirects:          5,
		DNSLookupTimeout:      5 * time.Second,
		ConnectTimeout:        10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		TransferTimeout:       5 * time.Minute,
		ProbeTimeout:          20 * time.Second,
		Resolver:              net.DefaultResolver,
		Metrics:               NewMetrics(),
	}
}

func (p Policy) withDefaults() Policy {
	d := DefaultPolicy()
	if p.MaxBytes <= 0 {
		p.MaxBytes = d.MaxBytes
	}
	if p.MaxRedirects <= 0 {
		p.MaxRedirects = d.MaxRedirects
	}
	if p.DNSLookupTimeout <= 0 {
		p.DNSLookupTimeout = d.DNSLookupTimeout
	}
	if p.ConnectTimeout <= 0 {
		p.ConnectTimeout = d.ConnectTimeout
	}
	if p.ResponseHeaderTimeout <= 0 {
		p.ResponseHeaderTimeout = d.ResponseHeaderTimeout
	}
	if p.TransferTimeout <= 0 {
		p.TransferTimeout = d.TransferTimeout
	}
	if p.ProbeTimeout <= 0 {
		p.ProbeTimeout = d.ProbeTimeout
	}
	if p.Resolver == nil {
		p.Resolver = d.Resolver
	}
	if p.Metrics == nil {
		p.Metrics = NewMetrics()
	}
	return p
}

func (p Policy) validateURL(ctx context.Context, raw string, redirect bool) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" {
		return nil, newError(KindUnknown, ErrInvalidURL, "URL must contain a scheme and host and cannot contain userinfo", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, newError(KindUnknown, ErrUnsupportedScheme, "only http and https are accepted", nil)
	}
	if u.Host == "" || u.User != nil {
		return nil, newError(KindUnknown, ErrInvalidURL, "URL must contain a host and cannot contain userinfo", nil)
	}
	if strings.TrimSpace(u.Hostname()) == "" {
		return nil, newError(KindUnknown, ErrInvalidURL, "URL host is empty", nil)
	}
	if !p.AllowPrivateNetworks {
		if err := p.validateHost(ctx, u.Hostname(), redirect); err != nil {
			return nil, err
		}
	}
	return u, nil
}

func (p Policy) validateHost(ctx context.Context, host string, redirect bool) error {
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		if disallowedIP(ip) {
			code := ErrPrivateNetwork
			if redirect {
				code = ErrDNSRebinding
			}
			return newError(KindUnknown, code, "host resolves to a non-public network", nil)
		}
		return nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, p.DNSLookupTimeout)
	defer cancel()
	addresses, err := p.Resolver.LookupIPAddr(lookupCtx, host)
	if err != nil {
		if lookupCtx.Err() != nil {
			return newError(KindUnknown, ErrDownloadTimeout, "DNS lookup timed out", lookupCtx.Err())
		}
		return newError(KindUnknown, ErrInvalidURL, "DNS lookup failed", err)
	}
	if len(addresses) == 0 {
		return newError(KindUnknown, ErrInvalidURL, "DNS returned no addresses", nil)
	}
	for _, address := range addresses {
		if disallowedIP(address.IP) {
			code := ErrPrivateNetwork
			if redirect {
				code = ErrDNSRebinding
			}
			return newError(KindUnknown, code, "host resolves to a non-public network", nil)
		}
	}
	return nil
}

func disallowedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	// Carrier-grade NAT, benchmarking and the common cloud metadata ranges
	// are not safe destinations even when a platform library does not classify
	// them as private.
	blocked := []struct {
		network string
		bits    int
	}{
		{"100.64.0.0", 10}, {"192.0.0.0", 24}, {"198.18.0.0", 15},
		{"169.254.0.0", 16}, {"100.100.100.200", 32},
	}
	for _, entry := range blocked {
		_, network, err := net.ParseCIDR(fmt.Sprintf("%s/%d", entry.network, entry.bits))
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

func (p Policy) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if p.AllowPrivateNetworks {
		dialer := net.Dialer{Timeout: p.ConnectTimeout}
		return dialer.DialContext(ctx, network, address)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, newError(KindUnknown, ErrInvalidURL, "invalid network address", err)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, p.DNSLookupTimeout)
	addresses, err := p.Resolver.LookupIPAddr(lookupCtx, host)
	cancel()
	if err != nil {
		if lookupCtx.Err() != nil {
			return nil, newError(KindUnknown, ErrDownloadTimeout, "DNS lookup timed out", lookupCtx.Err())
		}
		return nil, newError(KindUnknown, ErrInvalidURL, "DNS lookup failed", err)
	}
	dialer := net.Dialer{Timeout: p.ConnectTimeout}
	var lastErr error
	for _, candidate := range addresses {
		if disallowedIP(candidate.IP) {
			return nil, newError(KindUnknown, ErrDNSRebinding, "DNS answer is not public", nil)
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	return nil, lastErr
}

func generatedName(prefix, suffix string) string {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return prefix + suffix
	}
	return prefix + "-" + hex.EncodeToString(raw[:]) + suffix
}

func (p Policy) quarantine(path string, kind Kind, code ErrorCode, reason string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if strings.TrimSpace(p.QuarantineDir) == "" {
		return os.Remove(path)
	}
	if err := os.MkdirAll(p.QuarantineDir, 0o700); err != nil {
		return newError(kind, ErrQuarantineFailed, "cannot create quarantine directory", err)
	}
	destination := filepath.Join(p.QuarantineDir, generatedName(string(kind)+"-", ".quarantine"))
	if err := os.Rename(path, destination); err != nil {
		return newError(kind, ErrQuarantineFailed, "cannot move suspicious file to quarantine", err)
	}
	metadata := map[string]any{"kind": kind, "error_code": code, "reason": reason, "path": filepath.Base(destination)}
	data, _ := json.Marshal(metadata)
	if err := os.WriteFile(destination+".json", data, 0o600); err != nil {
		return newError(kind, ErrQuarantineFailed, "cannot write quarantine metadata", err)
	}
	p.Metrics.ObserveQuarantined(kind, code)
	return nil
}
