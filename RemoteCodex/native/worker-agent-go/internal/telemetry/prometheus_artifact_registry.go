package telemetry

func initPrometheusArtifactFamily(m *PrometheusMetrics) {
	m.artifactTmpfsReservedBytes = &GaugeVec{Name: "velox_artifact_tmpfs_reserved_bytes", Help: "RAM bytes currently reserved for tmpfs artifact staging", values: map[string]float64{"total": 0}}
	m.artifactTmpfsSpillTotal = &CounterVec{Name: "velox_artifact_tmpfs_spill_total", Help: "Artifacts spilled from tmpfs to durable NVMe", values: map[string]float64{"total": 0}}
	m.artifactTmpfsSpillBytes = &CounterVec{Name: "velox_artifact_tmpfs_spill_bytes_total", Help: "Bytes spilled from tmpfs to durable NVMe", values: map[string]float64{"total": 0}}
	m.artifactNvmeFallback = &CounterVec{Name: "velox_artifact_nvme_fallback_total", Help: "Artifact staging placements that fell back to NVMe by reason", Label: "reason", values: make(map[string]float64)}
}

func exportPrometheusArtifactFamily(m *PrometheusMetrics) string {
	return m.artifactTmpfsReservedBytes.export() + m.artifactTmpfsSpillTotal.export() + m.artifactTmpfsSpillBytes.export() + m.artifactNvmeFallback.export()
}
