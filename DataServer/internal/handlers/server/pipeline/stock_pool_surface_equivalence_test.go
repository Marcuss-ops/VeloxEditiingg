package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// stockPoolRecipe builds a recipe-shaped submission whose scene carries a stock
// POOL (a `stock` ARRAY, the shape the worker shuffles) plus the voiceover the
// enqueue completeness guard requires.
//
// The pool is materialized into SubmitScene.StockAssets, which is an
// INTERNAL-ONLY field (`json:"-"`): it cannot be sent on the wire and is derived
// by NormalizeCanonicalRecipe from spec.scenes[].stock. That is exactly why the
// two intake surfaces can disagree about it.
func stockPoolRecipe(idemKey string) SubmitJobRequest {
	return SubmitJobRequest{
		IdempotencyKey: idemKey,
		VideoName:      "Stock pool equivalence",
		ScriptText:     "Pins stock-pool equivalence across intake surfaces.",
		Spec: map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{
					"text":             "Opening scene",
					"duration_seconds": 3.5,
					"voiceover":        map[string]interface{}{"url": "velox-asset://voiceovers/opening.mp3"},
					"stock":            []interface{}{"velox-asset://stock/a.mp4", "velox-asset://stock/b.mp4"},
				},
			},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{{DestinationID: "drive"}},
	}
}

// TestSubmitJobE2E_BatchAndSingleProjectTheSameStockPool pins equivalence
// between the two intake surfaces at the WORKER PAYLOAD level, and is the
// regression guard for a latent divergence the retired batch HTTP-replay left
// behind.
//
// The retired batch path re-serialized each NORMALIZED item to JSON before
// dispatching it through the single-job handler. scene.stock[] is the
// internal-only canonical pool (`SubmitScene.StockAssets`, `json:"-"`), so the
// round-trip DROPPED it, and the re-normalization inside that handler could not
// restore it: NormalizeCanonicalRecipe only derives scenes from `spec` when the
// request carries no scenes at all, and the round-tripped item already carried
// them. A batch item therefore reached the worker with an EMPTY stock pool while
// the byte-identical single-job request kept both assets.
//
// The batch envelope now hands the normalized item to the intake core unchanged
// (job_submit_core.go), so both surfaces project and persist the same payload.
// This test fails on the old batch path.
func TestSubmitJobE2E_BatchAndSingleProjectTheSameStockPool(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	// Surface 1 — POST /api/v1/jobs.
	single := postSubmitJob(t, r, stockPoolRecipe("stock-pool-single"))
	if single.Code != http.StatusAccepted {
		t.Fatalf("single POST: want 202, got %d body=%s", single.Code, single.Body.String())
	}
	singleJobID := jobIDFromResponse(t, single.Body.Bytes())

	// Surface 2 — POST /api/v1/jobs/batch with the SAME recipe as one item.
	raw, err := json.Marshal(SubmitJobBatchRequest{
		BatchID: "stock-pool-batch",
		Items:   []SubmitJobRequest{stockPoolRecipe("stock-pool-batch")},
	})
	if err != nil {
		t.Fatalf("marshal batch body: %v", err)
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
		t.Fatalf("decode batch response: %v", err)
	}
	if len(batchResponse.Items) != 1 || batchResponse.Items[0].Status != "accepted" {
		t.Fatalf("batch item was not accepted: %+v", batchResponse.Items)
	}
	batchJobID := batchResponse.Items[0].JobID
	if batchJobID == "" {
		t.Fatalf("accepted batch item has no job_id: %+v", batchResponse.Items[0])
	}

	singleStock := persistedSceneStock(t, db, singleJobID)
	batchStock := persistedSceneStock(t, db, batchJobID)
	if len(singleStock) != 2 {
		t.Fatalf("single surface scene.stock[] = %v, want the 2 pooled assets", singleStock)
	}
	if len(batchStock) != 2 {
		t.Fatalf("batch surface scene.stock[] = %v, want the 2 pooled assets — the internal-only stock pool must reach the worker on BOTH surfaces", batchStock)
	}
	if !reflect.DeepEqual(singleStock, batchStock) {
		t.Fatalf("intake-surface drift: single scene.stock[] = %v, batch scene.stock[] = %v", singleStock, batchStock)
	}
}

// jobIDFromResponse reads the canonical job_id out of an accepted response
// envelope.
func jobIDFromResponse(t *testing.T, body []byte) string {
	t.Helper()
	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode job_id: %v", err)
	}
	if payload.JobID == "" {
		t.Fatalf("accepted response carries no job_id: %s", body)
	}
	return payload.JobID
}

// persistedSceneStock returns scenes[0].stock[] from the persisted TaskSpec of a
// job: the worker-visible projection, not the handler's in-memory request.
func persistedSceneStock(t *testing.T, db *store.SQLiteStore, jobID string) []interface{} {
	t.Helper()
	var specJSON string
	if err := db.DB().QueryRow(
		`SELECT s.payload_json
		 FROM tasks t JOIN task_specs s ON s.task_id = t.task_id
		 WHERE t.job_id = ?`, jobID,
	).Scan(&specJSON); err != nil {
		t.Fatalf("read persisted TaskSpec for job %s: %v", jobID, err)
	}
	var spec map[string]interface{}
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		t.Fatalf("decode persisted TaskSpec: %v", err)
	}
	scenes, _ := spec["scenes"].([]interface{})
	if len(scenes) == 0 {
		t.Fatalf("persisted TaskSpec has no scenes: %s", specJSON)
	}
	scene, _ := scenes[0].(map[string]interface{})
	stock, _ := scene["stock"].([]interface{})
	return stock
}
