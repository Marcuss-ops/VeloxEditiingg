package telemetry

func initPrometheusAttemptFamily(m *PrometheusMetrics) {
	m.attemptCPUTimeMs = &CounterVec{Name: "velox_attempt_cpu_time_ms_total", Help: "Accumulated CPU time observed across completed attempts (ms)", values: map[string]float64{"total": 0}}
	m.attemptPeakRSSBytes = &GaugeVec{Name: "velox_attempt_peak_rss_bytes", Help: "Peak resident set size observed for the latest completed attempt (bytes)", values: map[string]float64{"total": 0}}
	m.attemptIOBytes = &CounterVec{Name: "velox_attempt_io_bytes_total", Help: "Observed attempt I/O bytes by direction", Label: "direction", values: map[string]float64{"disk_read": 0, "disk_write": 0, "network_rx": 0, "network_tx": 0}}
	m.attemptFrames = &CounterVec{Name: "velox_attempt_frames_total", Help: "Observed media frames by fact kind across completed attempts", Label: "kind", values: map[string]float64{"decoded": 0, "composited": 0, "encoded": 0, "media_in": 0, "media_out": 0}}
	m.attemptProcesses = &CounterVec{Name: "velox_attempt_processes_total", Help: "Observed process lifecycle counts by kind across completed attempts", Label: "kind", values: map[string]float64{"engine_spawn": 0, "engine_external_spawn": 0, "ffmpeg_spawn": 0, "ffprobe_spawn": 0, "shell_spawn": 0, "curl_spawn": 0}}
}

func exportPrometheusAttemptFamily(m *PrometheusMetrics) string {
	return m.attemptCPUTimeMs.export() + m.attemptPeakRSSBytes.export() + m.attemptIOBytes.export() + m.attemptFrames.export() + m.attemptProcesses.export()
}
