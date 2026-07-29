// Package pipeline — job_submit_e2e_blocked_codes_test.go
//
// §0.3.4 item 4 split (NIT-2) e2e tests for the POST /api/v1/jobs
// pre-flight. The previous 2-state helper collapsed both failure
// buckets into a single `issue:"invalid"` detail; the new 3-state
// helper (
// velox-server/internal/store/store_deliveries.go::BatchDeliveryDestinationsStatus)
// in conjunction with job_submit.go's switch MUST emit distinct
// target_error_code values:
//
//	(b) Velox-side delivery_destinations.enabled = 0 →
//	    target_error_code=BLOCKED_VELOX_DISABLED.
//	(c) destination_id not in delivery_destinations at all →
//	    target_error_code=DESTINATION_NOT_FOUND.
//
// Both scenarios share the SAME handler stack so diagnostic shape
// parity (path / issue / target_error_code / status) is enforced —
// a regression that drops fields on one path but not the other
// trips the test loudly.
package pipeline

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestSubmitJobE2E_PreflightBlockedCodes_DistinctEnvelopes covers
// scenarios (b) and (c) of the §0.3.4 split coverage in one
// function so they share fixtures.
func TestSubmitJobE2E_PreflightBlockedCodes_DistinctEnvelopes(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)

	// Seed a DISABLED delivery_destinations row so scenario (b)
	// has a real, distinct row to point the destination_id at.
	// The default newSubmitJobE2EStack only seeds `drive` (enabled=1).
	if _, err := db.DB().Exec(
		`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at)
		 VALUES ('disabled-dest', 'google_drive', 'Disabled Dest', 0, '{}', datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed disabled-dest: %v", err)
	}

	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	t.Run("b_velox_disabled_envelope", func(t *testing.T) {
		body := validSubmitJobBody("e2e-blocked-disabled-001")
		body.DeliveryPlan[0].DestinationID = "disabled-dest"
		w := postSubmitJob(t, r, body)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("HTTP: want 422 (Velox-side enabled=false); got %d (body=%s)", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp["error"] != "invalid_payload" {
			t.Errorf("error: want invalid_payload; got %v", resp["error"])
		}
		details, ok := resp["details"].([]interface{})
		if !ok || len(details) != 1 {
			t.Fatalf("details: want 1 entry; got %v", resp["details"])
		}
		d0 := details[0].(map[string]interface{})
		if d0["target_error_code"] != BlockedCodeVeloxDisabled {
			t.Errorf("target_error_code: want %q; got %v (Velox-side enabled=false MUST surface as BLOCKED_VELOX_DISABLED, NOT a generic invalid marker — see §0.3.4 item 4 split)",
				BlockedCodeVeloxDisabled, d0["target_error_code"])
		}
		if d0["status"] != "disabled" {
			t.Errorf("status: want disabled; got %v", d0["status"])
		}
		if d0["issue"] != "destination_disabled" {
			t.Errorf("issue: want destination_disabled; got %v", d0["issue"])
		}
		if d0["path"] != "delivery_plan.0.destination_id" {
			t.Errorf("path: want delivery_plan.0.destination_id; got %v", d0["path"])
		}
	})

	t.Run("c_destination_not_found_envelope", func(t *testing.T) {
		body := validSubmitJobBody("e2e-blocked-notfound-002")
		body.DeliveryPlan[0].DestinationID = "never-seeded-id"
		w := postSubmitJob(t, r, body)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("HTTP: want 422 (id not in delivery_destinations); got %d (body=%s)", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp["error"] != "invalid_payload" {
			t.Errorf("error: want invalid_payload; got %v", resp["error"])
		}
		details, ok := resp["details"].([]interface{})
		if !ok || len(details) != 1 {
			t.Fatalf("details: want 1 entry; got %v", resp["details"])
		}
		d0 := details[0].(map[string]interface{})
		if d0["target_error_code"] != "DESTINATION_NOT_FOUND" {
			t.Errorf("target_error_code: want DESTINATION_NOT_FOUND; got %v (DISTINCT from BLOCKED_VELOX_DISABLED — operator dashboards must disambiguate §0.3.4 split)",
				d0["target_error_code"])
		}
		if d0["status"] != "not_found" {
			t.Errorf("status: want not_found; got %v", d0["status"])
		}
		if d0["issue"] != "destination_not_found" {
			t.Errorf("issue: want destination_not_found; got %v", d0["issue"])
		}
		if d0["path"] != "delivery_plan.0.destination_id" {
			t.Errorf("path: want delivery_plan.0.destination_id; got %v", d0["path"])
		}
	})
}
