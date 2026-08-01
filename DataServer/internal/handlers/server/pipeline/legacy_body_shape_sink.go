package pipeline

// LegacyBodySinkClientKindPreManifestRef is the bounded enum value
// for the `client_kind` label on `pipeline.legacy_body_shape_total`.
// Marks POST /api/v1/jobs submissions whose body shape matches the
// pre-manifest_ref compat envelope (top-level voiceover_paths[] /
// scenes[N].clip_link / subtitle_tracks[]) without supplying a
// manifest_ref. The submission is still accepted (the warning is
// non-blocking); the counter is the operator-visible signal that
// migration to manifest_ref is overdue.
//
// Future client_kind values (e.g. "internal_legacy_test_harness")
// are additive and MUST be added to this constant set BEFORE the
// production path emits them, so the catalog + the constant + the
// runtime cannot drift.
const LegacyBodySinkClientKindPreManifestRef = "pipelinegen_pre_manifest_ref"

// LegacyBodySink records legacy-body-shape warnings on POST
// /api/v1/jobs. The canonical production implementation lives in
// velox-server/internal/metrics (NewLegacyBodySink). Nil values are
// treated as a noop sink (safe default for tests and for callers
// that have not yet wired the metric). The interface is intentionally
// separate from CreatorIntakeSink because the two counters have
// different label sets and different operational meaning (creator
// intake tracks accepted payloads by intake path; legacy body shape
// tracks compat-shape arrivals by client_kind).
type LegacyBodySink interface {
	// IncLegacyBody increments the counter for the given client_kind.
	// client_kind MUST be one of the bounded documented values
	// (today: LegacyBodySinkClientKindPreManifestRef =
	// "pipelinegen_pre_manifest_ref"). Unknown values are still
	// recorded as a new series so a future regression that
	// introduces a typo'd enum value is visible in dashboards, but
	// the catalog's documented label set is the contract.
	IncLegacyBody(client_kind string)
}

// noopLegacyBodySink is the safe default when no sink is wired.
// The handler falls back to it so a missing wiring never panics and
// never silently drops a metric event.
type noopLegacyBodySink struct{}

func (noopLegacyBodySink) IncLegacyBody(string) {}
