// Package metrics / creator_intake.go
//
// Creator-intake adoption counter: tracks accepted creator payloads by
// intake path so operators can monitor migration from the async
// CreatorForwardingRunner (legacy) to the new synchronous HTTP push
// endpoint /api/v1/creator/jobs (POST creator_push).
//
// Label discipline (per internal/metrics/catalog.go header):
//   - Only "path" is exposed as a label. The two valid values are
//     "creator_push" (the new HTTP push endpoint) and
//     "creator_forwarder" (the async CreatorForwardingRunner).
//   - High-cardinality labels (source_provider, source_job_id, job_id)
//     are FORBIDDEN here — they would explode the time-series count.
//   - source_provider etc. belong in structured logs (see
//     DataServer/internal/handlers/server/pipeline/creator_push.go),
//     NOT in metric labels.
package metrics

// pipelineCreatorIntakeAccepted is the typed CounterFamily backing the
// catalog entry `pipeline.creator_intake_accepted_total`. Label set is
// bounded to {"path"} so the time-series cardinality is fixed at 2.
var pipelineCreatorIntakeAccepted = NewCounterFamily(
	"pipeline_creator_intake_accepted_total",
	"Total number of creator payloads accepted, split by intake path (push vs forwarder).",
	[]string{"path"},
)

// CreatorIntakeSink is the production implementation of
// pipeline.CreatorIntakeSink. It records every accepted payload by
// intake path so operators can compute the push/forwarder adoption
// ratio over time.
//
// The struct is empty: all state lives in the package-level
// pipelineCreatorIntakeAccepted family. Safe to share across goroutines
// (the family's Inc is internally synchronized).
type CreatorIntakeSink struct{}

// NewCreatorIntakeSink returns a sink that records accepted creator
// payloads. The returned value is a thin handle; the underlying
// counter is package-level.
func NewCreatorIntakeSink() *CreatorIntakeSink { return &CreatorIntakeSink{} }

// IncAccepted increments the counter for the given intake path. The
// path MUST be one of the bounded values "creator_push" or
// "creator_forwarder"; unknown values are still recorded (as a new
// series) so a future regression that introduces a third path is
// visible in dashboards, but the catalog's documented label set is
// the contract.
func (CreatorIntakeSink) IncAccepted(path string) {
	pipelineCreatorIntakeAccepted.Inc([]string{path}, 1)
}
