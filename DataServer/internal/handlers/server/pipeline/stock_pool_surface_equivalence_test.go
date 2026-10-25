package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-shared/contract"
)

func TestSubmitJobBatchAndSinglePreserveSameCompiledPlan(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	makeBody := func(key string) SubmitJobRequest {
		return SubmitJobRequest{IdempotencyKey: key, VideoName: "Compiled plan equivalence", ScriptText: "V2 plan",
			CompiledRenderPlanJSON: planJSON, CompiledRenderPlanSHA256: planSHA,
			DeliveryPlan: []SubmitDeliveryPlanEntry{{DestinationID: "drive"}}}
	}

	single := postSubmitJob(t, r, makeBody("compiled-plan-single"))
	if single.Code != http.StatusAccepted {
		t.Fatalf("single POST: want 202, got %d body=%s", single.Code, single.Body.String())
	}
	singleJobID := jobIDFromResponse(t, single.Body.Bytes())

	raw, err := json.Marshal(SubmitJobBatchRequest{BatchID: "compiled-plan-batch", Items: []SubmitJobRequest{makeBody("compiled-plan-batch-item")}})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/batch", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer m2mJobsAuthFake-token")
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("batch POST: want 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var batchResponse SubmitJobBatchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &batchResponse); err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if len(batchResponse.Items) != 1 || batchResponse.Items[0].Status != "accepted" {
		t.Fatalf("batch item not accepted: %+v", batchResponse.Items)
	}

	for _, jobID := range []string{singleJobID, batchResponse.Items[0].JobID} {
		var specJSON string
		if err := db.DB().QueryRow(`SELECT s.payload_json FROM tasks t JOIN task_specs s ON s.task_id=t.task_id WHERE t.job_id=?`, jobID).Scan(&specJSON); err != nil {
			t.Fatalf("read TaskSpec for %s: %v", jobID, err)
		}
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(specJSON), &payload); err != nil {
			t.Fatalf("decode TaskSpec: %v", err)
		}
		if payload[contract.PayloadKeyCompiledRenderPlanJSON] != planJSON || payload[contract.PayloadKeyCompiledRenderPlanSHA] != planSHA {
			t.Fatalf("job %s did not preserve the same producer-compiled plan", jobID)
		}
	}
}

func jobIDFromResponse(t *testing.T, body []byte) string {
	t.Helper()
	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.JobID == "" {
		t.Fatal("accepted response has no job_id")
	}
	return payload.JobID
}
