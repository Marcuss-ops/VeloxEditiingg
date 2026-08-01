// Package metrics / collector_render.go
//
// Render-ratio + task-phase timing families, sliced out of collector.go
// so the Collector struct definition stays focused on registration.
// Recorded by RecordAttempt (renderSpeed) in collector_attempts.go.
package metrics

// initRenderFamilies creates the per-project render-speed ratio gauge
// and the per-phase task timing histogram. Called once from
// NewCollector at boot.
func (c *Collector) initRenderFamilies() {
	c.renderSpeed = NewGaugeFamily(
		"velox_project_render_speed_ratio",
		"Ratio of media duration to wall clock time (>1 means faster than realtime)",
		[]string{"executor_id", "worker_class"},
	)

	c.phaseDurations = NewHistogramFamily(
		"velox_task_phase_duration_seconds",
		"Per-phase duration in seconds for a canonical rendering phase",
		[]string{"executor_id", "executor_version", "worker_class", "phase", "status"},
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800},
	)
}

// renderFamilies returns the render/phase subset registered by
// NewCollector via allFamilies.
func (c *Collector) renderFamilies() []*Family {
	return []*Family{
		c.renderSpeed,
		c.phaseDurations,
	}
}
