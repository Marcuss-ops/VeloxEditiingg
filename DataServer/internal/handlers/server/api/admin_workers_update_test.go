// Package api — Step 6/15 fleet-operator mutation handler tests.
//
// Coverage map:
//
//	Drain:
//	  TestDrainWorker_NilHandler             — nil publisher/reg → 503
//	  TestDrainWorker_EmptyWorkerID          — trim-empty → 400
//	  TestDrainWorker_WorkerNotFound         — unknown worker → 404
//	  TestDrainWorker_HappyPath              — Drain=true + audit row + 202
//	  TestDrainWorker_AlreadyDraining        — already drain=true → 409
//	  TestDrainWorker_InFlightConflict       — publisher returns ErrOperationInFlight → 409
//
//	Quarantine:
//	  TestQuarantineWorker_NilHandler        — 503
//	  TestQuarantineWorker_HappyPath         — Quarantined=true + audit row + 202
//	  TestQuarantineWorker_AlreadyQuarantined — already quarantined → 409
//	  TestQuarantineWorker_InFlightConflict  — ErrOperationInFlight → 409
//
//	Resume:
//	  TestResumeWorker_NilHandler            — 503
//	  TestResumeWorker_AlreadyHealthy        — already !Drain && !Quarantined → 409
//	  TestResumeWorker_PreservesDrainUntilSmoke — Drain=true → publish op=resume
//	  TestResumeWorker_PreservesQuarantineUntilSmoke — Quarantined=true → publish op=resume
//	  TestResumeWorker_InFlightConflict      — ErrOperationInFlight → 409
//
//	Update:
//	  TestUpdateWorker_RejectsMissingDigestBeforePublish
//	  TestUpdateWorker_RejectsMalformedDigestBeforePublish
//	  TestUpdateWorker_RejectsInvalidDigestBeforeWorkerLookup
//	  TestUpdateWorker_ValidDigestPublishesOperation
//
//	Defaults:
//	  TestMutationRequest_DefaultsReason     — body omitted → "triggered via admin API"
//
// Tests use stubPublisher (no real SQLite) + workersreg.New(nil)
// (in-memory registry). The mutation handler depends on:
//  1. Registry.GetWorker / SetWorkerDrain / SetWorkerQuarantine
//  2. ControllerPublisher.PublishOperation
//
// Both are swapped for in-process stubs so the test does not
// stand up SQLite or run the migration sweep.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	workersreg "velox-server/internal/workers"
)

func TestUpdateWorker_RejectsInvalidJSONBeforePublish(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/workers/wicket/update", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON -> %d, want 400: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("invalid JSON published %d operations, want 0", len(pub.published))
	}
}

func TestUpdateWorker_RejectsMissingDigestBeforePublish(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		Reason: "missing digest",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing target_digest → %d, want 400: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("missing target_digest published %d operations, want 0", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || info.Drain || info.Quarantined {
		t.Fatalf("invalid update changed worker state: %+v", info)
	}
}

func TestUpdateWorker_RejectsMalformedDigestBeforePublish(t *testing.T) {
	tests := []struct {
		name   string
		digest string
	}{
		{name: "mobile tag", digest: "ghcr.io/o/r:latest"},
		{name: "wrong prefix", digest: "ghcr.io/o/r@sha1:" + strings.Repeat("a", 64)},
		{name: "short sha256", digest: "ghcr.io/o/r@sha256:abc"},
		{name: "uppercase hex", digest: "ghcr.io/o/r@sha256:" + strings.Repeat("A", 64)},
		{name: "wrong registry", digest: "docker.io/o/r@sha256:" + strings.Repeat("a", 64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := newRegisteredRegistry(t, "wicket")
			pub := &stubPublisher{}
			r := updateRoute(newMutationsHandler(reg, pub))

			w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
				TargetDigest: tt.digest,
				Reason:       "malformed digest",
			})
			if w.Code != http.StatusBadRequest {
				t.Fatalf("target_digest=%q → %d, want 400: %s", tt.digest, w.Code, w.Body.String())
			}
			if len(pub.published) != 0 {
				t.Fatalf("target_digest=%q published %d operations, want 0", tt.digest, len(pub.published))
			}
		})
	}
}

func TestUpdateWorker_RejectsInvalidDigestBeforeWorkerLookup(t *testing.T) {
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(workersreg.New(nil), pub))

	w := doPOST(t, r, "/api/v1/admin/workers/ghost/update", MutationRequest{
		TargetDigest: "latest",
		Reason:       "invalid digest ordering",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid digest for unknown worker → %d, want 400: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("invalid digest published %d operations, want 0", len(pub.published))
	}
}

func TestDrainWorker_DropsUnvalidatedTargetDigest(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := drainRoute(newMutationsHandler(reg, pub))

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", MutationRequest{
		TargetDigest: "latest",
		Reason:       "drain only",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("drain with target_digest -> %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 {
		t.Fatalf("published operations=%d, want 1", len(pub.published))
	}
	if got := string(pub.published[0].Payload); got != "{}" {
		t.Fatalf("drain payload=%s, want {} (target_digest is update-only)", got)
	}
}

func TestUpdateWorker_ValidDigestPublishesOperation(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))
	digest := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		TargetDigest: digest,
		Reason:       "valid digest",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("valid target_digest → %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 {
		t.Fatalf("valid target_digest published %d operations, want 1", len(pub.published))
	}
	var payload map[string]string
	if err := json.Unmarshal(pub.published[0].Payload, &payload); err != nil {
		t.Fatalf("operation payload: %v", err)
	}
	if payload["target_digest"] != digest {
		t.Errorf("operation target_digest = %q, want %q", payload["target_digest"], digest)
	}
}

// ─── Drain ─────────────────────────────────────────────────────────────────
