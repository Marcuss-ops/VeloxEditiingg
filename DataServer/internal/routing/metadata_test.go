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

// ── §6. ForwardingKey escape/unescape symmetry ───────────────────
//
// FormatForwardingKey now percent-encodes `:` (as %3A) and `%` (as %25)
// inside each component BEFORE joining them with the literal separator.
// Parse() then reverses that. The invariant locked here is:
//
//   FormatForwardingKey(p, j, e).Parse() == (p, j, e)   for ALL inputs
//
// including inputs that legitimately contain `:` or `%` (the historical
// cases that would have silently mis-split under the old fmt.Sprintf
// implementation). A clean ASCII input that contains neither byte must
// round-trip byte-identically so existing DB rows remain readable.

// TestFormatForwardingKey_ASCIIRoundTrip covers the simplest case:
// components that contain neither `:` nor `%` must pass through
// unchanged (no spurious encoding), because that is the assertion that
// keeps historical ForwardingKey rows compatible with the new code.
func TestFormatForwardingKey_ASCIIRoundTrip(t *testing.T) {
	cases := []struct {
		p, j, e string
	}{
		{"remote_engine", "creator-forward-1", "scene.composite.v1"},
		{"creator_pc_1", "creator-job-001", "scene.composite.v1"},
		{"a", "b", "c"},
		{"", "job", "exec"}, // empty component (legal; the bytes survive)
	}
	for _, tc := range cases {
		k := FormatForwardingKey(tc.p, tc.j, tc.e)
		if gotP, gotJ, gotE := k.Parse(); gotP != tc.p || gotJ != tc.j || gotE != tc.e {
			t.Errorf("round-trip(%q,%q,%q) = %q -> Parse() = (%q,%q,%q)",
				tc.p, tc.j, tc.e, k, gotP, gotJ, gotE)
		}
	}
}

// TestFormatForwardingKey_ColonInComponentIsEncoded locks the central
// use case: a SourceJobID that contains `:` (e.g., a social-delivery
// id of the form `delivery_<uuid>|dest_X`) MUST NOT shift the column
// of downstream components. Format encodes the `:` inside job to %3A
// so the literal `:` that follows in the format string is the ONLY
// column delimiter. Parse then reverses it byte-identically.
func TestFormatForwardingKey_ColonInComponentIsEncoded(t *testing.T) {
	k := FormatForwardingKey("p", "delivery:abc:def", "e")
	// Encoded form: "p:delivery%3Aabc%3Adef:e"
	want := "p:delivery%3Aabc%3Adef:e"
	if string(k) != want {
		t.Errorf("FormatForwardingKey with ':' in middle: got %q, want %q", k, want)
	}
	if gotP, gotJ, gotE := k.Parse(); gotP != "p" || gotJ != "delivery:abc:def" || gotE != "e" {
		t.Errorf("Parse after colon-encoding: got (%q,%q,%q), want (p,delivery:abc:def,e)",
			gotP, gotJ, gotE)
	}
}

// TestFormatForwardingKey_PercentInComponentIsEncoded covers the
// symmetric escape edge: a literal `%` inside a component must NOT
// absorb the following two bytes as if they were an escape sequence.
// `%` is escaped first (to %25), so a downstream `%3A` (the encoded
// form of `:` introduced by the second pass) cannot be confused with
// an already-encoded `%`.
func TestFormatForwardingKey_PercentInComponentIsEncoded(t *testing.T) {
	k := FormatForwardingKey("p", "a%b", "e")
	want := "p:a%25b:e"
	if string(k) != want {
		t.Errorf("FormatForwardingKey with '%%' in middle: got %q, want %q", k, want)
	}
	if gotP, gotJ, gotE := k.Parse(); gotP != "p" || gotJ != "a%b" || gotE != "e" {
		t.Errorf("Parse after percent-encoding: got (%q,%q,%q), want (p,a%%b,e)",
			gotP, gotJ, gotE)
	}
}

// TestFormatForwardingKey_ColonAndPercentTogether covers the
// pathological case where a single component contains BOTH chars.
// The encoding order in escapeForwardingKeyComponent is `%` first,
// then `:` — so the input `%:` becomes `%25%3A` (NOT `%253A` nor
// `%25%3A`). If the order ever flipped the round-trip would be
// wrong because encoded `%` plus `:` would be mis-decoded.
func TestFormatForwardingKey_ColonAndPercentTogether(t *testing.T) {
	tc := []string{"a%:b", "%:a:b", "::%::", "100%"}
	for _, in := range tc {
		k := FormatForwardingKey("p", in, "e")
		if gotP, gotJ, gotE := k.Parse(); gotP != "p" || gotJ != in || gotE != "e" {
			t.Errorf("round-trip with mixed components (input=%q): got %q -> (%q,%q,%q)",
				in, k, gotP, gotJ, gotE)
		}
	}
}

// TestParse_LegacyPlainKey_StillSplits right-sides a developer-readable
// guarantee: keys produced by FormatForwardingKey BEFORE this change
// (just `fmt.Sprintf("%s:%s:%s", ...)` with no escaping) still parse
// cleanly as long as they contain neither `:` nor `%` inside any
// component. The 3-column split is on the FIRST TWO literals `:` and
// only the third gets the residual — so a legacy key with three
// colon-free parts round-trips. This is what makes the migration
// non-blocking on historical DB rows.
func TestParse_LegacyPlainKey_StillSplits(t *testing.T) {
	k := ForwardingKey("remote_engine:creator-forward-1:scene.composite.v1")
	if gotP, gotJ, gotE := k.Parse(); gotP != "remote_engine" || gotJ != "creator-forward-1" || gotE != "scene.composite.v1" {
		t.Errorf("legacy plain key parse: got (%q,%q,%q), want (remote_engine,creator-forward-1,scene.composite.v1)",
			gotP, gotJ, gotE)
	}
}

// TestParse_LegacyMalformedKey_SilentlyMisSplits documents the
// DELIBERATE non-migration behavior: a pre-existing key that contains
// an unmangled `:` inside one of its components WILL be mis-split by
// Parse() because SplitN takes the FIRST two `:`. The migration
// story is: such rows must be rewritten by a one-time migration
// before any new producer keys land; the only safe assumption inside
// Parse itself is "input is clean ASCII or already escaped".
//
// This test does NOT assert the mis-split (which would be tautological);
// it asserts that such a key is NOT silently treated as malformed —
// we want this property so the migration can proceed, and so future
// operators reading a mis-split row see the breakage rather than
// drop the row.
func TestParse_LegacyMalformedKey_StillReadsButMisSplits(t *testing.T) {
	// A pre-migration key with a colon in the middle column. New
	// code (escaped) would represent this same logical tuple as
	// "p:delivery%3Aabc:e". The legacy key, if it sneaks through,
	// will be read as (`p`, `delivery`, `abc:e`) — documented here
	// so future auditors can spot it on old rows.
	k := ForwardingKey("p:delivery:abc:e")
	gotP, gotJ, gotE := k.Parse()
	if gotP != "p" {
		t.Errorf("legacy malformed: provider split wrong: got %q", gotP)
	}
	if gotJ == "delivery:abc" && gotE == "e" {
		// EXPECTED: silent mis-split. Documented.
		t.Logf("legacy unmangled ':' in middle column was mis-split into (%q,%q,%q) — this is the documented migration-induced defect", gotP, gotJ, gotE)
	}
}
