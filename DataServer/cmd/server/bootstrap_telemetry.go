package main

// bootstrap_telemetry.go wires the Prometheus metrics registry + collector
// and attaches operational telemetry to every component that needs it.
// Split out of bootstrap_composition.go so the orchestrator stays focused
// on dependency order.

import (
	"velox-server/internal/config"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/logging"
	"velox-shared/compatibility"
)

// wireMetricsTelemetry creates the metrics registry + collector and
// attaches operational telemetry to every component that needs it.
func wireMetricsTelemetry(
	p *persistenceDeps,
	a *assetDeps,
	m *moduleDeps,
) (*velmetrics.Registry, *velmetrics.Collector) {
	metricsRegistry := velmetrics.NewRegistry()
	metricsCollector := velmetrics.NewCollector(metricsRegistry)
	if p != nil && p.SQLite != nil {
		p.SQLite.SetDBTelemetry(metricsCollector.OperationalTelemetry())
	}
	if a != nil && a.CompletionSQLiteStore != nil {
		a.CompletionSQLiteStore.SetDBRetryObserver(metricsCollector.OperationalTelemetry())
	}
	if m != nil && m.DeliveryRunner != nil {
		m.DeliveryRunner.WithTelemetry(metricsCollector.OperationalTelemetry())
		m.DeliveryRunner.WithLogger(logging.NewLogger("deliveries.runner"))
	}
	if m != nil && m.ForwardingRunner != nil {
		m.ForwardingRunner.WithTelemetry(metricsCollector.ForwardingTelemetry())
		m.ForwardingRunner.WithLogger(logging.NewLogger("forwarding.runner"))
		m.ForwardingRunner.WithIntakeSourceRecorder(velmetrics.NewIntakeSourceSink())
	}
	if m != nil && m.RemoteEngineClient != nil {
		m.RemoteEngineClient.WithMetrics(metricsCollector.RemoteEngineTelemetry())
		m.RemoteEngineClient.WithLogger(logging.NewLogger("remoteengine"))
	}
	if m != nil && m.AssetService != nil {
		for _, family := range velmetrics.NewInputSecurityFamilies(m.AssetService.SecurityMetrics()) {
			if family != nil {
				metricsRegistry.Register(family)
			}
		}
		for _, family := range velmetrics.NewAssetMediaMetadataFamilies(m.AssetService.MediaMetadataMetrics()) {
			if family != nil {
				metricsRegistry.Register(family)
			}
		}
	}
	return metricsRegistry, metricsCollector
}

// wireCompatibilityMode sets the global alias compatibility mode.
func wireCompatibilityMode(cfg *config.Config, mc *velmetrics.Collector) {
	compatibility.SetAliasReadObserver(mc.NewCompatibilityAliasObserver())
	compatibility.SetAliasRejectedObserver(mc.NewCompatibilityAliasRejectionObserver())
	if cfg.Compatibility.Mode == "strict" {
		compatibility.SetMode(compatibility.ModeStrict)
	} else {
		compatibility.SetMode(compatibility.ModeCompat)
	}
}
