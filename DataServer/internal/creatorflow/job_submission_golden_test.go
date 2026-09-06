package creatorflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// job_submission_golden_test.go — golden vector pin for the submission
// identity machinery.
//
// WHY THIS TEST EXISTS (A2-2 follow-up): the durable idempotency check
// hashes the resolver payload. normalizeIdentityPayload stamps the
// execution-metadata keys (job_id/job_run_id/correlation_id/created_at/
// updated_at/delivery_plan) from stableIdentity() BEFORE the hash is
// computed, so an identical retry hashes identically instead of
// conflicting. The identity derivation is deliberately sensitive:
//
//   - changing the seeded fields (adding a new field to the hash input,
//     reordering them, changing the separator, or the hash truncation)
//     changes EVERY future identity → every payload hash diverges → any
//     in-flight retry after a deploy collides with a new submission
//     instead of deduplicating (duplicated jobs) or vice versa;
//
//   - changing the stamped keys (adding/removing/renaming a payload key
//     or changing the fixed epoch timestamps) changes the payload hash
//     shape for NEW submissions only, which is usually safe but must be
//     a conscious decision.
//
// This test pins both. If it fails, the change to the identity pipeline
// MUST be a deliberate, reviewed decision (and probably needs an
// idempotency-migration note in the CHANGELOG), not an accident.

// TestStableIdentityGoldenVector pins the exact identity string for a
// fixed canonical request tuple. sha256("youtube:job-42:render.v1")[:8]
// → the expected hex below.
func TestStableIdentityGoldenVector(t *testing.T) {
	got := stableIdentity("youtube", "job-42", "render.v1")

	// Independent recomputation — the test does NOT call the production
	// helper to produce the expectation; it derives it from the raw
	// primitive so any drift in stableIdentity shows up as a diff.
	sum := sha256.Sum256([]byte("youtube:job-42:render.v1"))
	want := "submission_" + hex.EncodeToString(sum[:8])

	if got != want {
		t.Fatalf("stableIdentity golden vector drifted:\n got: %s\nwant: %s", got, want)
	}

	// Determinism: same tuple → same identity, byte for byte.
	for i := 0; i < 3; i++ {
		if again := stableIdentity("youtube", "job-42", "render.v1"); again != got {
			t.Fatalf("stableIdentity is not deterministic: %s vs %s", again, got)
		}
	}

	// Divergence: any tuple component change must change the identity
	// (otherwise two different submissions would collapse into one
	// idempotency bucket).
	if stableIdentity("youtube", "job-43", "render.v1") == got {
		t.Fatal("different source_job_id produced the same identity")
	}
	if stableIdentity("instagram", "job-42", "render.v1") == got {
		t.Fatal("different source_provider produced the same identity")
	}
	if stableIdentity("youtube", "job-42", "render.v2") == got {
		t.Fatal("different target_executor_id produced the same identity")
	}
}

// TestNormalizeIdentityPayloadGolden pins the exact payload key set and
// values stamped by normalizeIdentityPayload for a fixed identity, plus
// both delivery-plan wire shapes.
func TestNormalizeIdentityPayloadGolden(t *testing.T) {
	const identity = "submission_0123456789abcdef"

	t.Run("stamps exact keys with retry-stable values", func(t *testing.T) {
		payload := map[string]interface{}{}
		normalizeIdentityPayload(payload, nil, identity)

		want := map[string]interface{}{
			"job_id":         identity,
			"job_run_id":     "run_" + identity,
			"correlation_id": "corr_" + identity,
			"created_at":     "1970-01-01T00:00:00Z",
			"updated_at":     "1970-01-01T00:00:00Z",
		}
		gotJSON, _ := json.Marshal(payload)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("payload stamp drift:\n got: %s\nwant: %s", gotJSON, wantJSON)
		}
	})

	t.Run("overwrites client-supplied identity keys", func(t *testing.T) {
		// Client-supplied values in these keys must LOSE — they are
		// execution metadata, not request content (see the function doc).
		payload := map[string]interface{}{
			"job_id":     "client-wants-this",
			"created_at": "2099-01-01T00:00:00Z",
		}
		normalizeIdentityPayload(payload, nil, identity)
		if payload["job_id"] != identity {
			t.Fatalf("client job_id survived: %v", payload["job_id"])
		}
		if payload["created_at"] != "1970-01-01T00:00:00Z" {
			t.Fatalf("client created_at survived: %v", payload["created_at"])
		}
	})

	t.Run("nested delivery_plan shape is mirrored as-is", func(t *testing.T) {
		nested := map[string]interface{}{
			"delivery_plan": map[string]interface{}{"destination_id": "d1"},
		}
		payload := map[string]interface{}{}
		normalizeIdentityPayload(payload, nested, identity)
		mirrored, ok := payload["delivery_plan"].(map[string]interface{})
		if !ok || mirrored["destination_id"] != "d1" {
			t.Fatalf("nested delivery_plan mirror drift: %v", payload["delivery_plan"])
		}
	})

	t.Run("flat delivery_plan envelope falls back to the whole map", func(t *testing.T) {
		flat := map[string]interface{}{
			"destination_id": "d2",
		}
		payload := map[string]interface{}{}
		normalizeIdentityPayload(payload, flat, identity)
		mirrored, ok := payload["delivery_plan"].(map[string]interface{})
		if !ok || mirrored["destination_id"] != "d2" {
			t.Fatalf("flat delivery_plan mirror drift: %v", payload["delivery_plan"])
		}
	})

	t.Run("nil delivery_plan stamps nothing", func(t *testing.T) {
		payload := map[string]interface{}{}
		normalizeIdentityPayload(payload, nil, identity)
		if _, present := payload["delivery_plan"]; present {
			t.Fatalf("nil delivery_plan must not stamp the mirror key")
		}
	})
}

// TestNormalizeIdentityPayloadIsRetryStable is the end-to-end guarantee:
// two identical submissions (same tuple, same content) must produce
// byte-identical stamped payloads — this is what makes the durable
// idempotency hash deduplicate a retry instead of conflict.
func TestNormalizeIdentityPayloadIsRetryStable(t *testing.T) {
	build := func() map[string]interface{} {
		identity := stableIdentity("youtube", "job-42", "render.v1")
		payload := map[string]interface{}{
			"title": "same on both attempts",
			// Retry noise: timestamps/UUIDs a real adapter might re-generate.
			"created_at": "2099-01-01T00:00:00Z",
			"job_id":     "fresh-uuid-per-attempt",
		}
		normalizeIdentityPayload(payload,
			map[string]interface{}{"delivery_plan": map[string]interface{}{"destination_id": "d1"}},
			identity)
		return payload
	}

	first, second := build(), build()
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("retry payloads diverged:\n first:  %s\n second: %s", a, b)
	}
}
