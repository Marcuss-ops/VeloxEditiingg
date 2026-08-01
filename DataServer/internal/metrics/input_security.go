package metrics

import "velox-server/internal/inputsecurity"

// NewInputSecurityFamilies adapts the bounded input-security counters to the
// master Prometheus registry without making the security package depend on
// this package.
func NewInputSecurityFamilies(source *inputsecurity.Metrics) []*Family {
	if source == nil {
		return nil
	}
	rejections := NewCounterFamily(
		"input.security_rejections_total",
		"Rejected input acquisitions by role and canonical error code",
		[]string{"kind", "error_code"},
	)
	bytes := NewCounterFamily(
		"input.security_rejected_bytes_total",
		"Bytes rejected while acquiring inputs",
		[]string{"kind", "error_code"},
	)
	quarantined := NewCounterFamily(
		"input.security_quarantined_total",
		"Suspicious input files moved to quarantine",
		[]string{"kind", "error_code"},
	)
	source.AddObserver(func(kind inputsecurity.Kind, code inputsecurity.ErrorCode, rejectedBytes uint64, isQuarantine bool) {
		labels := []string{string(kind), string(code)}
		if isQuarantine {
			quarantined.Inc(labels, 1)
			return
		}
		rejections.Inc(labels, 1)
		if rejectedBytes > 0 {
			bytes.Inc(labels, rejectedBytes)
		}
	})
	return []*Family{rejections, bytes, quarantined}
}
