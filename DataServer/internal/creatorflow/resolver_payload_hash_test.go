package creatorflow

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// TestPayloadHashIdempotency exercises the [P0 #3] payload-hash
// contract on checkIdempotencyFastPath:
//
//   - same key + IDENTICAL payload -> (ResolveOutput, nil) — caller
//     returns the cached hit; response body carries "created":false
//     via buildIdempotentResolveResponse.
//   - same key + DIFFERENT payload -> (nil, ErrIdempotencyKeyReused)
//     — caller turns into HTTP 409 idempotency_key_reused; no new
//     Job is created.
//   - no forwarding row cache (resolver proceeds to INSERT path) ->
//     (nil, nil) — caller does the INSERT/UPSERT round-trip.
//
// Pre-055 rows whose payload_sha256 is the empty default cleanly
// skip the hash check (legacy idempotency semantics preserved).
// The corresponding sub-test is included so regressions in the
// migration window cannot silently start hard-failing duplicate
// webhooks as conflicts.
func TestPayloadHashIdempotency(t *testing.T) {
	const (
		job1          = "job-from-hash"
		provider      = "remote_engine"
		sourceJobID   = "src-key-1"
		targetExec    = "scene.composite.v1"
		storedForward = "cf-stored"
	)

	// Two payloads with identical structure but different content.
	// json.Marshal over `map[string]interface{}` sorts keys
	// deterministically, so identical logical payloads produce the
	// same SHA across runs; mutated payloads produce a different
	// SHA.
	payloadA := map[string]interface{}{
		"scenes":     []interface{}{"Opening", "Closing"},
		"video_name": "Version A",
		"total_secs": 10.0,
	}
	payloadB := map[string]interface{}{
		"scenes":     []interface{}{"Opening", "MIDDLE", "Closing"},
		"video_name": "Version B",
		"total_secs": 18.0,
	}

	_, shaA := resolverMarshalPayload(payloadA)
	_, shaB := resolverMarshalPayload(payloadB)

	if shaA == "" {
		t.Fatalf("precondition broken: resolverMarshalPayload returned empty sha for payloadA")
	}
	if shaA == shaB {
		t.Fatalf("precondition broken: payloadA and payloadB should produce DIFFERENT SHAs")
	}

	t.Run("same key + identical payload -> idempotent hit (created:false)", func(t *testing.T) {
		repo := &fakeForwardingRepo{
			bySource: map[string]*store.CreatorForwarding{
				keyForSource(provider, sourceJobID, targetExec): {
					ForwardingID:  storedForward,
					PayloadSHA256: shaA,
				},
			},
		}
		r := &Resolver{
			jobLookup: &fakeJobLookup{jobs: map[string]*jobs.Job{
				job1: {ID: job1, Status: jobs.StatusPending},
			}},
			forwardRepo: repo,
		}
		req := ResolveRequest{SourceProvider: provider, SourceJobID: sourceJobID}
		out, err := r.checkIdempotencyFastPath(
			context.Background(), req, job1, targetExec, payloadA,
		)
		if err != nil {
			t.Fatalf("expected nil err on hash match, got %v", err)
		}
		if out == nil {
			t.Fatalf("expected non-nil ResolveOutput, got nil")
		}
		if out.JobID != job1 {
			t.Fatalf("want JobID=%s, got %s", job1, out.JobID)
		}
		if out.ForwardingID != storedForward {
			t.Fatalf("want ForwardingID=%s, got %s", storedForward, out.ForwardingID)
		}
		if out.Response["created"] != false {
			t.Fatalf("want created=false on idempotent hit, got %v", out.Response["created"])
		}
		// EnsureForwarded was called (the post-hash repair path).
		if len(repo.ensureForwardedCalls) != 1 {
			t.Fatalf("expected one EnsureForwarded call, got %d: %v",
				len(repo.ensureForwardedCalls), repo.ensureForwardedCalls)
		}
	})

	t.Run("same key + different payload -> ErrIdempotencyKeyReused (409)", func(t *testing.T) {
		repo := &fakeForwardingRepo{
			bySource: map[string]*store.CreatorForwarding{
				keyForSource(provider, sourceJobID, targetExec): {
					ForwardingID:  storedForward,
					PayloadSHA256: shaA, // stores Version A
				},
			},
		}
		r := &Resolver{
			jobLookup: &fakeJobLookup{jobs: map[string]*jobs.Job{
				job1: {ID: job1, Status: jobs.StatusPending},
			}},
			forwardRepo: repo,
		}
		req := ResolveRequest{SourceProvider: provider, SourceJobID: sourceJobID}
		out, err := r.checkIdempotencyFastPath(
			context.Background(), req, job1, targetExec, payloadB, // sends Version B
		)
		if !errors.Is(err, ErrIdempotencyKeyReused) {
			t.Fatalf("expected ErrIdempotencyKeyReused, got %v", err)
		}
		if out != nil {
			t.Fatalf("expected nil out on conflict, got %+v", out)
		}
		// EnsureForwarded was NOT called — the 409 short-circuits.
		if len(repo.ensureForwardedCalls) != 0 {
			t.Fatalf("EnsureForwarded should NOT be called on hash conflict, got %v",
				repo.ensureForwardedCalls)
		}
	})

	t.Run("no forwarding row + no job -> caller proceeds to INSERT", func(t *testing.T) {
		// Empty fakes: no job cached, no forwarding row.
		r := &Resolver{
			jobLookup:   &fakeJobLookup{},
			forwardRepo: &fakeForwardingRepo{},
		}
		req := ResolveRequest{SourceProvider: provider, SourceJobID: "fresh-key"}
		out, err := r.checkIdempotencyFastPath(
			context.Background(), req, "job-fresh-id", targetExec, map[string]interface{}{"fresh": true},
		)
		if err != nil {
			t.Fatalf("expected nil err (no hit → caller INSERTs), got %v", err)
		}
		if out != nil {
			t.Fatalf("expected nil out (no hit → caller INSERTs), got %+v", out)
		}
	})

	t.Run("pre-055 row with empty payload_sha256 -> skip hash check, idempotent hit", func(t *testing.T) {
		// Confirms legacy idempotency: rows whose payload_sha256 is
		// the empty default (pre-migration-055 forwarding rows) MUST
		// NOT hard-fail to 409 on hash mismatch. Reposted webhook
		// deliveries for legacy rows must continue to return the
		// idempotent hit.
		repo := &fakeForwardingRepo{
			bySource: map[string]*store.CreatorForwarding{
				keyForSource(provider, sourceJobID, targetExec): {
					ForwardingID:  storedForward,
					PayloadSHA256: "", // legacy empty default
				},
			},
		}
		r := &Resolver{
			jobLookup: &fakeJobLookup{jobs: map[string]*jobs.Job{
				job1: {ID: job1, Status: jobs.StatusPending},
			}},
			forwardRepo: repo,
		}
		req := ResolveRequest{SourceProvider: provider, SourceJobID: sourceJobID}
		// Even though payloadB differs from the (empty) stored hash,
		// the empty-SHA bypass means the function still returns the
		// idempotent hit — preserving legacy semantics during the
		// migration window.
		out, err := r.checkIdempotencyFastPath(
			context.Background(), req, job1, targetExec, payloadB,
		)
		if err != nil {
			t.Fatalf("pre-055 empty-SHA legacy bypass tripped the hash check: err=%v", err)
		}
		if out == nil || out.JobID != job1 {
			t.Fatalf("want idempotent hit on legacy empty-SHA row, got %+v / err=%v", out, err)
		}
	})
}
