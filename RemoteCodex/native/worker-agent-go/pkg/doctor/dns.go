package doctor

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"velox-worker-agent/pkg/config"
)

// DNSReachabilityValidator checks that the master hostname resolves and
// the TCP port is reachable via dial within a 5s timeout.
// RW-PROD-002 section 2 item 4.
type DNSReachabilityValidator struct{}

func (v *DNSReachabilityValidator) ID() string { return "dns.reachability" }

func (v *DNSReachabilityValidator) Run(ctx context.Context, cfg *config.WorkerConfig) Result {
	target := cfg.ControlGRPCURL
	if target == "" {
		target = cfg.MasterURL
	}
	if target == "" {
		return fail("dns.reachability",
			"no control_grpc_url or master_url configured",
			"set control_grpc_url in worker_config.json to the gRPC endpoint")
	}

	// Strip scheme prefix if present using strings.HasPrefix which is
	// safer than manual slicing.
	hostPort := target
	if strings.HasPrefix(hostPort, "https://") {
		hostPort = strings.TrimPrefix(hostPort, "https://")
	} else if strings.HasPrefix(hostPort, "http://") {
		hostPort = strings.TrimPrefix(hostPort, "http://")
	}
	// Strip any trailing path.
	if idx := strings.IndexByte(hostPort, '/'); idx >= 0 {
		hostPort = hostPort[:idx]
	}

	// host:port format expected; if no port, append default gRPC port.
	if _, _, err := net.SplitHostPort(hostPort); err != nil {
		hostPort = net.JoinHostPort(hostPort, "8443")
	}

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(dialCtx, "tcp", hostPort)
	if err != nil {
		return fail("dns.reachability",
			fmt.Sprintf("cannot reach %s: %v", hostPort, err),
			"verify the master is running, the address is correct, and firewall rules allow the connection")
	}
	_ = conn.Close()
	return pass("dns.reachability", fmt.Sprintf("TCP dial to %s succeeded", hostPort))
}
