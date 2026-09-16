package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
)

// bindConfigBody drives the real request-binding boundary with a JSON body so
// the allowlist and the 0|1 toggle contract are pinned at the HTTP edge.
func bindConfigBody(t *testing.T, kind, body string) (MutationRequest, error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/admin/workers/worker-1/config", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return bindMutationRequest(c, kind)
}

func fmp4Toggle(v int) *int { return &v }

// TestBindMutationRequest_AcceptsFMP4StreamProfileToggle pins the canonical
// rollout path for the fMP4 admission gate: an operator (or fleetctl) can open
// or close the gate on an already-installed worker through the audited config
// operation instead of hand-editing /etc/velox-worker/worker.env.
func TestBindMutationRequest_AcceptsFMP4StreamProfileToggle(t *testing.T) {
	for _, value := range []int{0, 1} {
		req, err := bindConfigBody(t, fleet.OperationKindRestart, `{"fmp4_stream_profile":`+strconv.Itoa(value)+`,"reason":"fmp4 rollout"}`)
		if err != nil {
			t.Fatalf("bind fmp4=%d: %v", value, err)
		}
		if req.FMP4StreamProfile == nil || *req.FMP4StreamProfile != value {
			t.Fatalf("fmp4=%d bound to %v", value, req.FMP4StreamProfile)
		}
		if req.Reason != "fmp4 rollout" {
			t.Fatalf("reason = %q, want operator intent preserved", req.Reason)
		}
	}
}

// TestBindMutationRequest_RejectsFMP4StreamProfileOutsideToggle keeps the knob
// from accepting arbitrary values that the root-owned helper would refuse.
func TestBindMutationRequest_RejectsFMP4StreamProfileOutsideToggle(t *testing.T) {
	_, err := bindConfigBody(t, fleet.OperationKindRestart, `{"fmp4_stream_profile":2}`)
	if err == nil || !strings.Contains(err.Error(), "fmp4_stream_profile must be 0 or 1") {
		t.Fatalf("fmp4=2 error = %v, want toggle rejection", err)
	}
}

// TestBindMutationRequest_ConfigStillRequiresASetting guards the existing
// no-op-refusal: an empty config request must not publish an operation.
func TestBindMutationRequest_ConfigStillRequiresASetting(t *testing.T) {
	_, err := bindConfigBody(t, fleet.OperationKindRestart, `{"reason":"nothing to do"}`)
	if err == nil || !strings.Contains(err.Error(), "worker config requires") {
		t.Fatalf("empty config error = %v, want 'worker config requires'", err)
	}
}

// TestBindMutationRequest_FMP4KnobDoesNotLeakIntoOtherKinds documents that the
// toggle is validated only for the config kind; drain/resume/quarantine bodies
// are untouched and cannot smuggle worker.env mutation.
func TestBindMutationRequest_FMP4KnobDoesNotLeakIntoOtherKinds(t *testing.T) {
	req, err := bindConfigBody(t, fleet.OperationKindDrain, `{"reason":"drain"}`)
	if err != nil {
		t.Fatalf("drain bind: %v", err)
	}
	op := newMutationOperation("worker-1", fleet.OperationKindDrain, req, time.Now(), "op-drain-1")
	var payload map[string]any
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		t.Fatalf("decode drain payload: %v", err)
	}
	if _, present := payload["fmp4_stream_profile"]; present {
		t.Fatalf("drain payload carries fmp4_stream_profile: %s", op.Payload)
	}
}

// TestNewMutationOperation_CarriesFMP4StreamProfileInAuditPayload pins that the
// rollout decision is recorded on the fleet_operations ledger, so the gate
// change is attributable after the fact.
func TestNewMutationOperation_CarriesFMP4StreamProfileInAuditPayload(t *testing.T) {
	req := MutationRequest{Reason: "fmp4 rollout", FMP4StreamProfile: fmp4Toggle(1)}
	op := newMutationOperation("worker-1", fleet.OperationKindRestart, req, time.Now(), "op-config-1")
	var payload map[string]any
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		t.Fatalf("decode config payload: %v", err)
	}
	if got, ok := payload["fmp4_stream_profile"].(float64); !ok || got != 1 {
		t.Fatalf("audit payload fmp4_stream_profile = %v (%s), want 1", payload["fmp4_stream_profile"], op.Payload)
	}
	if op.Op != fleet.OperationKindRestart || op.WorkerID != "worker-1" {
		t.Fatalf("operation identity = %+v", op)
	}
}
