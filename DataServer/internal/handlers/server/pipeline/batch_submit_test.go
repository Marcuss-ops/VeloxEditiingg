package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func validBatchItem(key string) SubmitJobRequest {
	return SubmitJobRequest{
		IdempotencyKey: key,
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 1}},
		Publications: []SubmitPublication{{
			PublicationID: "publication-" + key,
			OutputRef:     SubmitPublicationOutputRef{ArtifactRole: "final_video"},
			Metadata:      SubmitPublicationMetadata{Title: "Title " + key},
			Destinations:  []SubmitPublicationDestination{{DestinationID: "youtube-main"}},
		}},
	}
}

func TestValidateSubmitJobBatchRequest_EnvelopeRules(t *testing.T) {
	if err, bad := ValidateSubmitJobBatchRequest(SubmitJobBatchRequest{}); !bad || err == nil {
		t.Fatal("empty batch must be rejected")
	}

	for _, test := range []struct {
		name  string
		batch SubmitJobBatchRequest
		issue string
	}{
		{
			name:  "invalid utf8",
			batch: SubmitJobBatchRequest{BatchID: string([]byte{0xff}), Items: []SubmitJobRequest{{IdempotencyKey: "item"}}},
			issue: "invalid_utf8",
		},
		{
			name:  "reserved separator",
			batch: SubmitJobBatchRequest{BatchID: "batch/one", Items: []SubmitJobRequest{{IdempotencyKey: "item"}}},
			issue: "reserved_separator",
		},
		{
			name:  "maximum byte length",
			batch: SubmitJobBatchRequest{BatchID: strings.Repeat("b", MaxSubmitJobBatchIDBytes+1), Items: []SubmitJobRequest{{IdempotencyKey: "item"}}},
			issue: "max_length",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err, bad := ValidateSubmitJobBatchRequest(test.batch)
			if !bad || err == nil {
				t.Fatalf("batch must be rejected: err=%v bad=%v", err, bad)
			}
			for _, detail := range err.Details {
				if detail["path"] == "batch_id" && detail["issue"] == test.issue {
					return
				}
			}
			t.Fatalf("missing batch_id:%s detail: %+v", test.issue, err.Details)
		})
	}

	normalizedBoundary := strings.Repeat("b", MaxSubmitJobBatchIDBytes-2) + "é"
	if err, bad := ValidateSubmitJobBatchRequest(SubmitJobBatchRequest{
		BatchID: normalizedBoundary,
		Items:   []SubmitJobRequest{{IdempotencyKey: "item"}},
	}); bad || err != nil {
		t.Fatalf("valid UTF-8 byte boundary rejected: %v", err)
	}

	tooMany := SubmitJobBatchRequest{BatchID: "batch", Items: make([]SubmitJobRequest, MaxSubmitJobBatchItems+1)}
	batchErr, bad := ValidateSubmitJobBatchRequest(tooMany)
	if !bad || batchErr == nil {
		t.Fatal("oversized batch must be rejected")
	}
	foundMaxItems := false
	for _, detail := range batchErr.Details {
		if detail["path"] == "items" && detail["issue"] == "max_items" {
			foundMaxItems = true
		}
	}
	if !foundMaxItems {
		t.Fatalf("oversized batch details missing max_items: %+v", batchErr.Details)
	}

	valid := SubmitJobBatchRequest{BatchID: "batch", Items: []SubmitJobRequest{{IdempotencyKey: "item"}}}
	if err, bad := ValidateSubmitJobBatchRequest(valid); bad || err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}
}

func TestDecodeStrictJSON_RejectsInvalidUTF8AndTrailingValues(t *testing.T) {
	var destination map[string]any
	if err := decodeStrictJSON(strings.NewReader(`{"ok":true} {"extra":true}`), &destination); err == nil {
		t.Fatal("concatenated JSON values must be rejected")
	}
	if err := decodeStrictJSON(bytes.NewReader([]byte{'{', '"', 'x', '"', ':', 0xff, '}'}), &destination); err == nil {
		t.Fatal("invalid UTF-8 JSON must be rejected")
	}
}

func TestDecodeStrictJSON_RejectsOversizedBody(t *testing.T) {
	var destination map[string]any
	body := strings.NewReader(strings.Repeat("x", MaxStrictJSONBodyBytes+1))
	if err := decodeStrictJSON(body, &destination); err == nil {
		t.Fatal("oversized JSON body must be rejected")
	}
}

func TestSubmitJob_RejectsStrictJSONBoundaryViolations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/v1/jobs", (&Handlers{}).SubmitJob())

	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "invalid utf8",
			body: []byte{'{', '"', 'i', 'd', 'e', 'm', 'p', 'o', 't', 'e', 'n', 'c', 'y', '_', 'k', 'e', 'y', '"', ':', '"', 0xff, '"', '}'},
		},
		{
			name: "trailing value",
			body: []byte(`{"idempotency_key":"item","scenes":[]} {"extra":true}`),
		},
		{
			name: "oversized body",
			body: []byte(strings.Repeat("x", MaxStrictJSONBodyBytes+1)),
		},
		{
			name: "unknown nested metadata override field",
			body: []byte(`{"idempotency_key":"item","scenes":[{"text":"scene","duration_seconds":1}],"publications":[{"publication_id":"publication","output_ref":{"artifact_role":"final_video"},"destinations":[{"destination_id":"youtube","metadata_override":{"title":"title","unknown":"value"}}]}]}`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestSubmitJobBatch_RejectsTrailingJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/v1/jobs/batch", (&Handlers{}).SubmitJobBatch())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/batch", strings.NewReader(`{"batch_id":"b","items":[]} {"extra":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}

func TestSubmitJobBatch_RejectsUnknownEnvelopeField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/v1/jobs/batch", (&Handlers{}).SubmitJobBatch())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/batch", strings.NewReader(`{"batch_id":"b","items":[],"unknown":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "invalid_json" {
		t.Fatalf("error = %v, want invalid_json", body["error"])
	}
}

func TestSubmitJobBatch_ItemWithDeliveryPlanWithoutStoreReturnsControlledFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/v1/jobs/batch", (&Handlers{}).SubmitJobBatch())

	item := validBatchItem("delivery-plan-item")
	item.Publications = nil
	item.DeliveryPlan = []SubmitDeliveryPlanEntry{{DestinationID: "legacy-drive"}}
	payload, err := json.Marshal(SubmitJobBatchRequest{BatchID: "batch-store", Items: []SubmitJobRequest{item}})
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/batch", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}

	var decoded SubmitJobBatchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Items) != 1 || decoded.Items[0].Status != "failed" {
		t.Fatalf("delivery_plan failure was not isolated: %+v", decoded.Items)
	}
	if !strings.Contains(strings.Join(decoded.Items[0].Errors, " "), "store_failure") {
		t.Fatalf("missing store_failure error: %+v", decoded.Items[0].Errors)
	}
}

func TestSubmitJobBatch_IsolatesDuplicateAndInvalidItems(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/v1/jobs/batch", (&Handlers{}).SubmitJobBatch())

	first := validBatchItem("same-key")
	first.Scenes = nil // single-item validation must reject only this item
	second := validBatchItem("same-key")
	third := validBatchItem("different-key")
	third.Scenes = nil
	payload, err := json.Marshal(SubmitJobBatchRequest{
		BatchID: "batch-isolation",
		Items:   []SubmitJobRequest{first, second, third},
	})
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/batch", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}

	var decoded SubmitJobBatchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.BatchID != "batch-isolation" || len(decoded.Items) != 3 {
		t.Fatalf("batch response = %+v", decoded)
	}
	if decoded.Items[0].Status != "rejected" || decoded.Items[2].Status != "rejected" {
		t.Fatalf("invalid items not rejected independently: %+v", decoded.Items)
	}
	if decoded.Items[1].Status != "rejected" || !strings.Contains(strings.Join(decoded.Items[1].Errors, " "), "duplicate_idempotency_key") {
		t.Fatalf("duplicate item was not isolated: %+v", decoded.Items[1])
	}
	if decoded.Items[0].Index != 0 || decoded.Items[1].Index != 1 || decoded.Items[2].Index != 2 {
		t.Fatalf("item indexes not preserved: %+v", decoded.Items)
	}
}
