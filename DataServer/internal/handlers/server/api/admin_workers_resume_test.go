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
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

func TestResumeWorker_PreservesDrainUntilSmoke(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerDrain(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed Drain=true: %v", err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume",
		MutationRequest{Reason: "smoke passed at 14:32 UTC"})
	if w.Code != http.StatusAccepted {
		t.Fatalf("resume happy path → %d, want 202: %s", w.Code, w.Body.String())
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Drain || !info.Resuming {
		t.Errorf("resume gate flags: drain=%v resuming=%v, want both true until async smoke gate succeeds", info != nil && info.Drain, info != nil && info.Resuming)
	}
	if len(pub.published) != 1 || pub.published[0].Op != "resume" {
		t.Errorf("audit row: %+v", pub.published)
	}
	if pub.published[0].RequestedBy != "admin" {
		t.Errorf("RequestedBy = %q, want \"admin\"", pub.published[0].RequestedBy)
	}
}

func TestResumeWorker_PreservesQuarantineUntilSmoke(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerQuarantine(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed Quarantined=true: %v", err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("resume happy path → %d, want 202: %s", w.Code, w.Body.String())
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Quarantined || !info.Resuming {
		t.Errorf("resume gate flags: quarantine=%v resuming=%v, want both true until async smoke gate succeeds", info != nil && info.Quarantined, info != nil && info.Resuming)
	}
	if len(pub.published) != 1 || pub.published[0].Op != "resume" {
		t.Errorf("audit row: %+v", pub.published)
	}
	if pub.published[0].RequestedBy != "admin" {
		t.Errorf("RequestedBy = %q, want \"admin\"", pub.published[0].RequestedBy)
	}
}

func TestResumeWorker_ConcurrentAdmissionHasSingleOwner(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerDrain(context.Background(), "wicket", true); err != nil {
		t.Fatal(err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	responses := make(chan int, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil).Code
		}()
	}
	wg.Wait()
	close(responses)

	var accepted, conflicts int
	for code := range responses {
		switch code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("concurrent resume status=%d, want 202 or 409", code)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("concurrent resume outcomes accepted=%d conflicts=%d, want 1/1", accepted, conflicts)
	}
	if len(pub.published) != 1 {
		t.Fatalf("published operations=%d, want exactly one resume operation", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Drain || !info.Resuming {
		t.Fatalf("owned resume gate flags: drain=%v resuming=%v, want both true", info != nil && info.Drain, info != nil && info.Resuming)
	}
}

func TestResumeWorker_GenericPublisherErrorCleansGate(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerDrain(context.Background(), "wicket", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerQuarantine(context.Background(), "wicket", true); err != nil {
		t.Fatal(err)
	}
	pub := &stubPublisher{
		publishFn: func(_ context.Context, _ *store.Operation) error {
			return errors.New("synthetic resume publish failure")
		},
	}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("generic resume publish error -> %d, want 500: %s", w.Code, w.Body.String())
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Drain || !info.Quarantined || info.Resuming || info.ResumeOperationID != "" {
		t.Fatalf("generic publish failure changed resume/exclusion state: info=%+v", info)
	}
}
