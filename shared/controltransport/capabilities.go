// Package controltransport / capabilities.go
//
// Capability strings advertised by workers in the WorkerHello handshake
// and consulted by the master at dispatch time. All constants live here
// so both sides of the wire define the namespace in exactly one place;
// any new capability MUST be added here and re-emitted by the worker
// side (RemoteCodex/.../internal/worker/worker.go) before any master
// code can rely on it.
//
// Capability negotiation mechanism today
// --------------------------------------
//
// Hello.capabilities (worker_control.proto) is typed as
// `google.protobuf.Struct`, an opaque JSON-shaped map. The wire format
// therefore tolerates ANY key string, so adding capabilities does NOT
// require proto regeneration — workers can advertise new capability
// strings as soon as this Go file publishes them. The master side
// reads structpb.NewStruct() output of Hello.capabilities and consults
// CapabilityArtifactCommitV1 / CapabilityExecutorHybridV1 at dispatch
// time.
//
// Adding a new capability
// -----------------------
//
//   1. Append the constant below — kebab-case <<version-suffixed>>
//      form ("<area>.<version>", e.g. "artifact.commit.v1") so old
//      workers can advertise deprecated versions naturally without
//      tripping the upgrade path.
//   2. Bump HasCapability() to include any new ALL_CAPS alias only if
//      you want it to be implicit-on (almost never — explicit
//      advertising is the rule).
//   3. Add a unit test in capabilities_test.go asserting the constant
//      round-trips through Struct → JSON → Struct without distortion.
//
// The capability namespace is forward-only: a master MUST accept any
// string it does not understand and log a single verbose-WARN line per
// unknown capability. Removing a capability is a hard cutover (drain
// the fleet, see docs/completion-protocol.md §Phase 6).
//
// Design note on proto §1.4 vs impl
// ---------------------------------
// completion-protocol.md §1.4 says "edit worker_control.proto to add
// the new capability string, then regenerate". The implementation
// here deliberately diverges: the existing `Hello.capabilities = 8`
// field is a `google.protobuf.Struct` (opaque JSON-shaped map) that
// already tolerates arbitrary key strings. We advertise capability
// strings (e.g. "artifact.commit.v1") as Struct keys, so adding new
// caps does NOT require proto regeneration — old workers can carry
// new strings without rebuilding WHILE the capability rides in the
// (opaque) `Hello.capabilities = 8` Struct field. The proto is
// therefore unchanged in Phase 1.4; gen-proto.sh is run for
// verification only (SHA-256 of worker_control.pb.go stays stable).
// If a future protocol revision needs to constrain the capability
// shape (forbid arbitrary Struct keys, require legacy clients to drop
// unknown caps), migrate to a dedicated
// `repeated string capability_versions = N` field on Hello and
// rebind the typed strings here in lockstep with the regen.
package controltransport

// Well-known worker capability strings (Fase 1.4 of the Artifact
// Commit Protocol, see docs/completion-protocol.md). Keep kebab-case
// for wire readability; keep the literal value stable across releases
// since legacy workers carry this string verbatim in their worker_config.
const (
	// CapabilityArtifactCommitV1 — the worker speaks the Artifact
	// Commit Protocol (Fase 1+ of docs/completion-protocol.md):
	// publishes TaskOutputDeclared, consumes ArtifactUploadPlan,
	// uploads via the transport registry, gates cleanup on
	// TaskCommitAck. Masters consult this capability at dispatch to
	// decide whether the Task requires required_outputs (Phase 2
	// routes required_outputs-bearing Tasks only to v1+ workers).
	CapabilityArtifactCommitV1 = "artifact.commit.v1"

	// CapabilityExecutorHybridV1 — the worker ships the hybrid
	// executor (audio_url relaxation + explicit pipeline_id
	// resolution, see Phase 2 follow-ups). Pre-hybrid workers
	// keep emitting the legacy audio_url-as-spool-key contract;
	// hybrid-aware masters preferentially route to workers that
	// advertise this so resolving pipeline IDs goes through
	// ResolvePipelineID() instead of the wire-provided ID.
	CapabilityExecutorHybridV1 = "executor.hybrid.v1"

	// CapabilityTaskOutputDeclaredV1 — the worker emits the typed
	// TaskOutputDeclared message (Fase 3.3 of
	// docs/completion-protocol.md) carrying a repeated
	// OutputManifest list per Attempt. Pre-v1 workers publish the
	// legacy ArtifactUploaded (field 18) instead. Masters route
	// the typed path only when this capability is advertised.
	CapabilityTaskOutputDeclaredV1 = "task.output.declared.v1"

	// CapabilityArtifactUploadPlanV1 — the worker consumes the
	// typed ArtifactUploadPlan message (Fase 3.4) with a
	// per-manifest UploadTarget list, commit_token bearer, and
	// per-target upload_url / chunk_size / expires_at_unix. The
	// master only emits this message to workers that advertise
	// the capability; otherwise it falls back to the v0 plain
	// ArtifactUploaded shape.
	CapabilityArtifactUploadPlanV1 = "artifact.upload.plan.v1"

	// CapabilityArtifactUploadCompletedV1 — the worker emits the
	// typed ArtifactUploadCompleted message (Fase 3.5) reporting
	// uploaded_bytes + worker_sha256 for the master's verification
	// pass. Pre-v1 workers fall back to the legacy ArtifactUploaded
	// upload_status="completed" payload.
	CapabilityArtifactUploadCompletedV1 = "artifact.upload.completed.v1"

	// CapabilityTaskCommitAckV1 — the worker consumes the typed
	// TaskCommitAck message (Fase 3.6) signaling the attempt is
	// durably committed (jobs.status='SUCCEEDED',
	// attempt_commits.status='COMMITTED'). On receipt the worker
	// transitions the local spool row to COMMITTED and may unlink
	// the on-disk file. Without this capability the worker falls
	// back to a polling reconciliation against
	// attempt_commits.status.
	CapabilityTaskCommitAckV1 = "task.commit.ack.v1"
)

// AllCapabilities is the canonical closed-set of capabilities the
// master TODAY recognises. Unknown strings are silently accepted and
// logged; recognised strings are inspected against the dispatch policy.
// Kept as a slice (not a map) so the iteration order is deterministic
// across builds — important for log readability and snapshot diffing.
var AllCapabilities = []string{
	CapabilityArtifactCommitV1,
	CapabilityExecutorHybridV1,
	CapabilityTaskOutputDeclaredV1,
	CapabilityArtifactUploadPlanV1,
	CapabilityArtifactUploadCompletedV1,
	CapabilityTaskCommitAckV1,
}

// IsKnownCapability reports whether the given string is one of the
// recognised capabilities (above). Unknown strings return false: this
// is the forward-only capability negotiation contract — new workers
// may emit new strings and the master MUST accept and pass them
// through without rejecting the worker. The master-side logging of
// unknown capabilities is the caller's responsibility (typically the
// handshake handler at session start); IsKnownCapability itself is a
// pure predicate.
func IsKnownCapability(s string) bool {
	for _, c := range AllCapabilities {
		if c == s {
			return true
		}
	}
	return false
}
