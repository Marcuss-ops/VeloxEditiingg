// Package metrics / legacy_body_shape.go
//
// Legacy-body-shape warning counter: tracks POST /api/v1/jobs
// submissions that arrive with the legacy compatibility body shape
// (top-level voiceover_paths[] / scenes[N].clip_link /
// subtitle_tracks[]) WITHOUT manifest_ref. The submission is still
// accepted — the counter is the operator-visible signal that
// migration to the manifest_ref contract is overdue, NOT a gate
// that rejects the request.
//
// This is the metrics-side counterpart to the
// `client_kind=pipelinegen_pre_manifest_ref` log line that
// job_submit.go::NormalizeExternalJobSubmission emits when the same
// condition fires. The metric exists so dashboards can compute the
// migration-rate over time (the legacy-shape rate is monotonically
// trending toward zero as PipelineGen migrates) without grepping
// logs.
//
// Label discipline (per internal/metrics/catalog.go header):
//   - Only "client_kind" is exposed as a label. The single
//     documented value today is "pipelinegen_pre_manifest_ref" —
//     the canonical PipelineGen pre-migration client identity.
//     Future values are additive (a new bounded enum entry), never
//     replacing this one.
//   - High-cardinality labels (idempotency_key, job_id) are
//     FORBIDDEN here — they would explode the time-series count.
//     idempotency_key etc. belong in structured logs
//     (job_submit.go::pipelineLog), NOT in metric labels.
package metrics

// pipelineLegacyBodyShape is the typed CounterFamily backing the
// catalog entry `pipeline.legacy_body_shape_total`. Label set is
// bounded to {"client_kind"} so the time-series cardinality is fixed
// at the documented enum size.
var pipelineLegacyBodyShape = NewCounterFamily(
	"pipeline_legacy_body_shape_total",
	"Total number of POST /api/v1/jobs submissions that arrived with the legacy compatibility body shape without manifest_ref. Non-blocking warning — the submission is still accepted.",
	[]string{"client_kind"},
)

// LegacyBodySink is the production interface for emitting
// legacy-body-shape warnings. The canonical implementation lives in
// NewLegacyBodySink; tests typically pass a recording mock so they
// can assert the exact (client_kind) tuples that fired without
// relying on a Prometheus collector.
//
// The interface is intentionally separate from CreatorIntakeSink
// (the post-launch adoption counter) so a future refactor can wire
// or unwire either independently. The two counters have different
// label sets and different operational meaning (creator_intake
// counts accepted payloads by intake path; legacy_body_shape counts
// compat-shape arrivals by client_kind), and conflating them under
// one IncAccepted-style method would lock the label cardinality.
type LegacyBodySink interface {
	// IncLegacyBody increments the counter for the given client_kind.
	// client_kind MUST be one of the bounded documented values
	// (today: "pipelinegen_pre_manifest_ref"). Unknown values are
	// still recorded as a new series so a future regression that
	// introduces a typo'd enum value is visible in dashboards,
	// but the catalog's documented label set is the contract.
	IncLegacyBody(client_kind string)
}

// LegacyBodySinkImpl is the production implementation of
// LegacyBodySink. The struct is empty: all state lives in the
// package-level pipelineLegacyBodyShape family. Safe to share
// across goroutines (the family's Inc is internally synchronized).
type LegacyBodySinkImpl struct{}

// NewLegacyBodySink returns a sink that records legacy-body-shape
// warnings. The returned value is a thin handle; the underlying
// counter is package-level.
func NewLegacyBodySink() *LegacyBodySinkImpl { return &LegacyBodySinkImpl{} }

// IncLegacyBody increments the counter for the given client_kind.
// See LegacyBodySink interface for the label-cardinality contract.
func (LegacyBodySinkImpl) IncLegacyBody(client_kind string) {
	pipelineLegacyBodyShape.Inc([]string{client_kind}, 1)
}