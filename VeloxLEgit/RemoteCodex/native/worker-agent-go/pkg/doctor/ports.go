package doctor

import (
	"context"
	"fmt"
	"net"

	"velox-worker-agent/pkg/config"
)

// PortsValidator checks that the health and Prometheus ports are free.
// It briefly binds to each port and closes immediately.
// RW-PROD-002 §2 item 7.
type PortsValidator struct{}

func (v *PortsValidator) ID() string { return "ports" }

func (v *PortsValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	var failures []string

	// Check health port.
	if cfg.HealthPort > 0 {
		addr := fmt.Sprintf(":%d", cfg.HealthPort)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			failures = append(failures, fmt.Sprintf("health_port %d: %v", cfg.HealthPort, err))
		} else {
			_ = ln.Close()
		}
	}

	// Check Prometheus port.
	if cfg.PrometheusPort > 0 {
		addr := fmt.Sprintf(":%d", cfg.PrometheusPort)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			failures = append(failures, fmt.Sprintf("prometheus_port %d: %v", cfg.PrometheusPort, err))
		} else {
			_ = ln.Close()
		}
	}

	if len(failures) > 0 {
		detail := ""
		for i, f := range failures {
			if i > 0 {
				detail += "; "
			}
			detail += f
		}
		return fail("ports", detail, "free the conflicting ports or change health_port/prometheus_port in config")
	}

	return pass("ports", fmt.Sprintf("health_port=%d prometheus_port=%d free", cfg.HealthPort, cfg.PrometheusPort))
}
