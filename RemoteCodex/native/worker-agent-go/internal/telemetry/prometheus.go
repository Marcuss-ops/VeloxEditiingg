// Package telemetry provides Prometheus metrics collection for the worker agent.
package telemetry

import (
	"fmt"
	"net"
	"net/http"
)

// NewPrometheusMetrics creates a new Prometheus metrics collector by composing
// the metric-family registries. Metric definitions and export ownership live
// beside their family methods; this function only wires them together.
func NewPrometheusMetrics() *PrometheusMetrics {
	metrics := &PrometheusMetrics{}
	initPrometheusJobFamily(metrics)
	initPrometheusCacheFamily(metrics)
	initPrometheusWorkerFamily(metrics)
	initPrometheusArtifactFamily(metrics)
	initPrometheusAttemptFamily(metrics)
	return metrics
}

// ExportPrometheus returns metrics in the historical Prometheus text format.
func (m *PrometheusMetrics) ExportPrometheus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return exportPrometheusJobFamily(m) +
		exportPrometheusCacheFamily(m) +
		exportPrometheusWorkerFamily(m) +
		exportPrometheusArtifactFamily(m) +
		exportPrometheusAttemptFamily(m)
}

// Global Prometheus metrics instance.
var globalPrometheus = NewPrometheusMetrics()

// GetPrometheusMetrics returns the global Prometheus metrics instance.
func GetPrometheusMetrics() *PrometheusMetrics {
	return globalPrometheus
}

// StartPrometheusServer starts an HTTP server for Prometheus metrics scraping.
func StartPrometheusServer(port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid Prometheus port %d", port)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprint(w, globalPrometheus.ExportPrometheus())
	})

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listen Prometheus port %d: %w", port, err)
	}
	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Prometheus server error: %v\n", err)
		}
	}()
	return nil
}
