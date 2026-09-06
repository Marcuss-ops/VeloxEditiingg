// Package metrics / ingest_coercion.go
//
// A2-1 follow-up: TaskResult status-coercion counter.
//
// IngestTaskResult defensively coerces any wire status other than
// succeeded/failed/cancelled to "failed" (the IngestCommand.Status
// contract). Before this counter existed, that coercion was invisible:
// a worker enum drift (e.g. "SUCCEEDED" casing, or a brand-new status
// value) silently converted task outcomes into failures. The counter
// makes the coercion measurable so the strict-rejection decision
// (refuse the report instead of coercing) can be made from data after
// an observation window.
//
// Label discipline (per metrics.go header):
//   - Only "status" is exposed as a label. The value is the RAW wire
//     status, which is worker-controlled. To keep cardinality bounded
//     the value is sanitized: length-capped and reduced to a fixed
//     character class; anything that does not survive the sanitizer
//     collapses into the "malformed" series. High-cardinality values
//     (task_id, attempt_id) belong in the structured log emitted at
//     the coercion site, NOT here.
package metrics

import "strings"

// ingestStatusCoerced backs velox_ingest_status_coerced_total.
var ingestStatusCoerced = NewCounterFamily(
	"velox_ingest_status_coerced_total",
	"TaskResult wire statuses coerced to 'failed' by ingestion (unknown status values).",
	[]string{"status"},
)

// coercedStatusSanitizer caps label values and restricts them to a safe
// character class so a hostile/buggy worker cannot mint unbounded label
// series. Matches the master's task status vocabulary conventions
// (lowercase ascii letters and underscore, e.g. "succeeded", "in_progress").
const coercedStatusMaxLen = 32

func coerceStatusLabel(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || len(trimmed) > coercedStatusMaxLen {
		return "malformed"
	}
	var b strings.Builder
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			return "malformed"
		}
	}
	return b.String()
}

// RecordStatusCoercion increments the coercion counter for the given raw
// wire status. Package-level like the intake families so the ingestion
// path can record without threading a Collector dependency (the family
// is registered once per registry in NewCollector via
// packageIngestFamilies).
func RecordStatusCoercion(rawStatus string) {
	ingestStatusCoerced.Inc([]string{coerceStatusLabel(rawStatus)}, 1)
}

// packageIngestCoercionFamilies returns the package-level coercion family
// for registration in NewCollector.
func packageIngestCoercionFamilies() []*Family {
	return []*Family{ingestStatusCoerced}
}
