// Package metrics / creator_intake.go
//
// Creator-intake adoption counter: tracks accepted creator payloads by
// intake path so operators can monitor migration from the async
// CreatorForwardingRunner (legacy) to the new synchronous HTTP push
// endpoint /api/v1/creator/jobs (POST creator_push).
//
// Label discipline (per internal/metrics/catalog.go header):
//   - Only "path" is exposed as a label. The two valid values
//     are "creator_push" (the HTTP endpoint /api/v1/creator/jobs) and
//     "creator_forwarder" (the async CreatorForwardingRunner).
//     The "remote_engine_legacy" label was retired when the legacy
//     /api/remote/pipeline sync-forward endpoint was removed from
//     main; see docs/CREATOR-PUSH.md §Removal.
//   - High-cardinality labels (source_provider, source_job_id, job_id)
//     are FORBIDDEN here — they would explode the time-series count.
//   - source_provider etc. belong in structured logs (see
//     DataServer/internal/handlers/server/pipeline/creator_push.go
//     and DataServer/internal/handlers/server/pipeline/forwarding.go),
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

// pipelineIntakeSourceAccepted is the typed CounterFamily backing the
// catalog entry `pipeline.intake_source_accepted_total`. Label set is
// bounded to {"intake_source"} with the closed vocabulary from
// creatorflow (canonical, creator, instaedit, batch, script_generate,
// script_kind, pipeline_run, calendar). It measures alias usage across
// EVERY job-creation surface so an endpoint can be deprecated only when
// its series is provably zero over the chosen window.
var pipelineIntakeSourceAccepted = NewCounterFamily(
	"pipeline_intake_source_accepted_total",
	"Total number of jobs accepted by the master, split by intake source (the producer surface that submitted the job).",
	[]string{"intake_source"},
)

// IntakeSourceSink is the production implementation of
// creatorflow.IntakeSourceRecorder. It records every accepted submission
// by intake source so operators can measure alias usage before
// deprecating/removing a legacy endpoint.
//
// The struct is empty: all state lives in the package-level
// pipelineIntakeSourceAccepted family. Safe to share across goroutines.
type IntakeSourceSink struct{}

// NewIntakeSourceSink returns a sink that records accepted submissions by
// intake source. The returned value is a thin handle; the underlying
// counter is package-level.
func NewIntakeSourceSink() *IntakeSourceSink { return &IntakeSourceSink{} }

// IncAccepted increments the intake-source counter for the given source.
// The source MUST be one of the bounded creatorflow values; unknown values
// are still recorded (as a new series) so a future regression that
// introduces a new surface is visible in dashboards.
func (IntakeSourceSink) IncAccepted(source string) {
	pipelineIntakeSourceAccepted.Inc([]string{source}, 1)
}

// RecordIntakeSource increments the intake-source counter for the given
// source. It is the direct package-level convenience used by producers
// that do not route through the canonical submitter (e.g. the script
// direct-enqueue boundary).
func RecordIntakeSource(source string) {
	pipelineIntakeSourceAccepted.Inc([]string{source}, 1)
}

// packageIntakeFamilies returns the package-level intake families that
// must be registered on every Collector so the /metrics endpoint exposes
// them. Both families (creator-intake path + intake-source) are
// package-level singletons shared across Collector instances; they are
// registered once per registry in NewCollector.
func packageIntakeFamilies() []*Family {
	return []*Family{
		pipelineCreatorIntakeAccepted,
		pipelineIntakeSourceAccepted,
	}
}

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
