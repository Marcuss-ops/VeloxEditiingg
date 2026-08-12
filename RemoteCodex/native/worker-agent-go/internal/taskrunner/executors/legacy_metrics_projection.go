package executors

// legacy_metrics_projection.go is the one-way compatibility boundary for
// executor metrics. Executors retain this view only because older task-result
// consumers still accept a dotted map; canonical facts live in RawMetrics and
// canonical derived values come from pkg/performance.
//
// Keeping the map behind this type is intentional: a new producer cannot
// accidentally make the legacy map authoritative by writing map entries
// directly in its execution path. The final Map call is the only hand-off to
// ExecutionResult.Metrics.

type legacyMetricsProjection struct {
	values map[string]interface{}
}

func newLegacyMetricsProjection() *legacyMetricsProjection {
	return &legacyMetricsProjection{values: make(map[string]interface{})}
}

func (p *legacyMetricsProjection) Set(key string, value interface{}) {
	if p == nil || key == "" {
		return
	}
	if p.values == nil {
		p.values = make(map[string]interface{})
	}
	p.values[key] = value
}

func (p *legacyMetricsProjection) Map() map[string]interface{} {
	if p == nil || len(p.values) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(p.values))
	for key, value := range p.values {
		out[key] = value
	}
	return out
}
