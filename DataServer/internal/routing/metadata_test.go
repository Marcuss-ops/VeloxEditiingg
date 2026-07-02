// metadata_test.go — focused unit tests for the canonical map-mutation
// contracts in the routing package.
//
// This is the FIRST test file in the routing package. The surface
// under test:
//
//   - (k ForwardingKey).InjectIntoPayload    — symmetric write-side
//     helper for callers that hold ONLY a ForwardingKey value.
//   - (m InternalRoutingMetadata).InjectIntoPayload — all-fields
//     helper for callers that hold the full routing bundle.
//
// The CANONICAL invariant locked here across BOTH methods:
//
//   (1) nil-target is a clean no-op (no panic).
//   (2) zero-value fields are NEVER written (an empty ForwardingKey
//       does not produce an "" entry under KeyForwardingKey; a
//       zero-value InternalRoutingMetadata does not produce entries
//       in any of the four routing keys).
//   (3) The ForwardingKey variant is STRICTLY a subset of the
//       InternalRoutingMetadata variant — it writes ONLY KeyForwardingKey
//       and MUST NOT introduce KeyPipelineID/KeyExecutorID/KeyExecutorVersion.
//
// Sub-tests are organized by the contract dimension they lock:

//
// ── §1 nil-target ────────────────────────────────────────────────
//
// ── §2 empty-receiver ────────────────────────────────────────────
//
// ── §3 happy-path ────────────────────────────────────────────────
//
// ── §4 overwrite-existing-key + preserve-other-keys ───────────────
//
// ── §5 symmetric-guarantees ──────────────────────────────────────
//
// Maps in Go are not safe for concurrent write; the InjectIntoPayload
// method does NOT synchronize on the caller's behalf. Tests in this
// file therefore run single-goroutine and DO NOT include race tests.
// The contract is "the caller MUST serialize concurrent injection";

package routing

import "testing"

// ── §1. nil-target guard ──────────────────────────────────────────

// TestForwardingKey_InjectIntoPayload_NilTarget asserts that passing
// nil as the target map is a clean no-op (must not panic). The test
// harness would already convert an unrecovered panic into a FAIL —
// this test simply makes the no-panic behavior explicit at the file
// level so a future refactor cannot regress it silently.
//
// Mirror of InternalRoutingMetadata.InjectIntoPayload's nil-target
// guard (locked in §5 below).
func TestForwardingKey_InjectIntoPayload_NilTarget(t *testing.T) {
	ForwardingKey("remote_engine:creator-forward-1:scene.composite.v1").InjectIntoPayload(nil)
}

// ── §2. empty-receiver guard ──────────────────────────────────────

// TestForwardingKey_InjectIntoPayload_EmptyKey asserts that a
// zero-value ForwardingKey("") does NOT inject an empty entry under
// KeyForwardingKey. This locks the contract that an empty key is
// treated as "no forwarding key" (not as an explicit empty string)
// so a typo'd value does not silently overwrite a pre-existing
// routing entry.
func TestForwardingKey_InjectIntoPayload_EmptyKey(t *testing.T) {
	target := map[string]interface{}{KeyForwardingKey: "old_value"}
	ForwardingKey("").InjectIntoPayload(target)
	if got := target[KeyForwardingKey]; got != "old_value" {
		t.Errorf("empty key overwrote existing value: got %v, want \"old_value\"", got)
	}
}

// ── §3. happy-path ────────────────────────────────────────────────

// TestForwardingKey_InjectIntoPayload_HappyPath asserts the canonical
// write of a non-empty ForwardingKey into a target map. Verified via
// the FromPayload round-trip: write-then-read yields the same key.
// The round-trip assertion catches both directions of the contract —
// that InjectIntoPayload writes the right value AND that FromPayload
// reads it back without losing info.
func TestForwardingKey_InjectIntoPayload_HappyPath(t *testing.T) {
	k := ForwardingKey("remote_engine:creator-forward-1:scene.composite.v1")
	target := map[string]interface{}{}
	k.InjectIntoPayload(target)
	if got := FromPayload(target).ForwardingKey; got != k {
		t.Errorf("FromPayload(target).ForwardingKey: got %q, want %q", got, k)
	}
}

// TestForwardingKey_InjectIntoPayload_FormatForwardingKey_RoundTrip
// locks the canonical producer/consumer chain used by creatorflow:
// FormatForwardingKey(provider, sourceJobID, executorID) builds the
// key → InjectIntoPayload writes it → target[KeyForwardingKey] is
// the canonical colon-joined string. The first half is the wire
// format; the second half is what the enqueuer reads.
func TestForwardingKey_InjectIntoPayload_FormatForwardingKey_RoundTrip(t *testing.T) {
	k := FormatForwardingKey("remote_engine", "creator-forward-1", "scene.composite.v1")
	target := map[string]interface{}{}
	k.InjectIntoPayload(target)
	if got, want := target[KeyForwardingKey], string(k); got != want {
		t.Errorf("%s: got %v, want %q", KeyForwardingKey, got, want)
	}
}

// ── §4. overwrite-existing-key + preserve-other-keys ──────────────

// TestForwardingKey_InjectIntoPayload_UpdatesTarget asserts that a
// ForwardingKey injection OVERWRITES any pre-existing value under
// KeyForwardingKey AND leaves every non-KeyForwardingKey entry
// untouched. Combines §4's overwrite + preserve cases into one
// observation surface so the assertion reads as "the canonical
// 1-key update on a mixed-target map produces the expected
// (key=updated, all others=preserved) tuple".
func TestForwardingKey_InjectIntoPayload_UpdatesTarget(t *testing.T) {
	target := map[string]interface{}{
		KeyForwardingKey: "old_value",
		KeyPipelineID:    "pipeline-abc",
		KeyExecutorID:    "executor-x",
		"scene_count":    12,
	}
	newKey := ForwardingKey("new_provider:new_job:new_executor")
	newKey.InjectIntoPayload(target)

	if got, want := target[KeyForwardingKey], string(newKey); got != want {
		t.Errorf("overwrite %s: got %v, want %q", KeyForwardingKey, got, want)
	}
	if v := target[KeyPipelineID]; v != "pipeline-abc" {
		t.Errorf("preserved %s: got %v, want \"pipeline-abc\"", KeyPipelineID, v)
	}
	if v := target[KeyExecutorID]; v != "executor-x" {
		t.Errorf("preserved %s: got %v, want \"executor-x\"", KeyExecutorID, v)
	}
	if v := target["scene_count"]; v != 12 {
		t.Errorf("preserved scene_count: got %v, want 12", v)
	}
}

// ── §5. symmetric-guarantees with InternalRoutingMetadata.InjectIntoPayload ──

// TestInternalRoutingMetadata_InjectIntoPayload_NilTarget mirrors §1
// on the InternalRoutingMetadata variant. Both methods share the
// nil-target guard so the parallel no-op behaviour holds across
// both routing write entry points.
func TestInternalRoutingMetadata_InjectIntoPayload_NilTarget(t *testing.T) {
	m := InternalRoutingMetadata{
		ForwardingKey: ForwardingKey("p:j:e"),
	}
	m.InjectIntoPayload(nil)
}

// TestInternalRoutingMetadata_InjectIntoPayload_EmptyFields_NoWrites
// mirrors §2 across ALL FOUR fields: a zero-value InternalRoutingMetadata
// must NOT inject any zero-value entries under any of the four
// routing keys. This locks the symmetric "empty-fields-no-overwrite"
// pattern across both write entry points AND matches §2's contract
// for the ForwardingKey-only variant.
func TestInternalRoutingMetadata_InjectIntoPayload_EmptyFields_NoWrites(t *testing.T) {
	target := map[string]interface{}{
		KeyForwardingKey:   "old_fwd",
		KeyPipelineID:      "old_pipeline",
		KeyExecutorID:      "old_executor",
		KeyExecutorVersion: 99,
	}
	InternalRoutingMetadata{}.InjectIntoPayload(target)
	if got := target[KeyForwardingKey]; got != "old_fwd" {
		t.Errorf("ForwardingKey empty overwrote: got %v, want \"old_fwd\"", got)
	}
	if got := target[KeyPipelineID]; got != "old_pipeline" {
		t.Errorf("PipelineID empty overwrote: got %v, want \"old_pipeline\"", got)
	}
	if got := target[KeyExecutorID]; got != "old_executor" {
		t.Errorf("ExecutorID empty overwrote: got %v, want \"old_executor\"", got)
	}
	if got := target[KeyExecutorVersion]; got != 99 {
		t.Errorf("ExecutorVersion=0 overwrote: got %v, want 99", got)
	}
}

// TestForwardingKey_InjectIntoPayload_IsSubsetOf_InternalRoutingMetadata_InjectIntoPayload
// locks the most important boundary: the ForwardingKey variant writes
// ONLY the forwarding key. PipelineID, Executor.ID, Executor.Version
// MUST remain untouched. If a future PR tries to merge the two
// methods (e.g., adds `Pipeline(p)` chaining on the ForwardingKey
// variant) this test will fail loudly.
//
// The assertion is "exactly one key was written, AND that key is
// KeyForwardingKey" — strict subset.
func TestForwardingKey_InjectIntoPayload_IsSubsetOf_InternalRoutingMetadata_InjectIntoPayload(t *testing.T) {
	target := map[string]interface{}{}
	k := ForwardingKey("p:j:e")
	k.InjectIntoPayload(target)

	if len(target) != 1 {
		t.Errorf("ForwardingKey variant wrote %d keys, want 1: target=%v", len(target), target)
	}
	if _, present := target[KeyPipelineID]; present {
		t.Errorf("ForwardingKey variant must NOT write %s", KeyPipelineID)
	}
	if _, present := target[KeyExecutorID]; present {
		t.Errorf("ForwardingKey variant must NOT write %s", KeyExecutorID)
	}
	if _, present := target[KeyExecutorVersion]; present {
		t.Errorf("ForwardingKey variant must NOT write %s", KeyExecutorVersion)
	}
	if got := target[KeyForwardingKey]; got != string(k) {
		t.Errorf("ForwardingKey variant missing %s: got %v, want %q", KeyForwardingKey, got, string(k))
	}
}
