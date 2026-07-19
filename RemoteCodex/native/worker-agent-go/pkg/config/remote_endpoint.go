package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidateRemoteMasterEndpoint is the production/staging self-execution gate.
// A worker must not use a master endpoint that resolves to itself: that
// configuration makes a two-host test indistinguishable from a local render.
// Development keeps the historical same-host loopback path available.
func ValidateRemoteMasterEndpoint(c *WorkerConfig) error {
	if c == nil {
		return fmt.Errorf("%w: config is nil", ErrInvalidConfig)
	}
	env := strings.ToLower(strings.TrimSpace(c.Environment))
	if env == "" || env == "dev" {
		return nil
	}

	for name, raw := range map[string]string{
		"master_url":       c.MasterURL,
		"control_grpc_url": c.ControlGRPCURL,
	} {
		host, err := endpointHost(raw, name == "master_url")
		if err != nil {
			return err
		}
		if isLocalEndpoint(host) {
			return fmt.Errorf("%w: SELF_MASTER_ENDPOINT_FORBIDDEN: %s=%q resolves to this worker; configure a remote master", ErrInvalidConfig, name, raw)
		}
	}
	return nil
}

func endpointHost(raw string, isURL bool) (string, error) {
	value := strings.TrimSpace(raw)
	if isURL {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" {
			return "", fmt.Errorf("%w: invalid master_url %q", ErrInvalidConfig, raw)
		}
		return strings.TrimSuffix(strings.ToLower(u.Hostname()), "."), nil
	}
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		// Permit a host without a port for operators/tests using a resolver
		// default, while still rejecting malformed empty values.
		host = value
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "" {
		return "", fmt.Errorf("%w: invalid control_grpc_url %q", ErrInvalidConfig, raw)
	}
	return strings.TrimSuffix(host, "."), nil
}

func isLocalEndpoint(host string) bool {
	if host == "localhost" || host == "ip6-localhost" || host == "localhost.localdomain" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || localInterfaceIPs()[ip.String()]
	}
	for _, ip := range lookupIPs(host) {
		if ip.IsLoopback() || localInterfaceIPs()[ip.String()] {
			return true
		}
	}
	return false
}

func lookupIPs(host string) []net.IP {
	ips, _ := net.LookupIP(host)
	return ips
}

func localInterfaceIPs() map[string]bool {
	result := make(map[string]bool)
	interfaces, _ := net.Interfaces()
	for _, iface := range interfaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			if ip, _, err := net.ParseCIDR(addr.String()); err == nil {
				result[ip.String()] = true
			}
		}
	}
	return result
}
