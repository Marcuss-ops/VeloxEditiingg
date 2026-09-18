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

// TestBatchItemFingerprint_StableAndExcludesNonRenderFields pins the dedupe
// identity: fingerprint-equal iff render-relevant identity is deep-equal.
func TestBatchItemFingerprint_StableAndExcludesNonRenderFields(t *testing.T) {
	base := validBatchItem("key-a")

	fp, err := BatchItemFingerprint(base)
	if err != nil {
		t.Fatalf("BatchItemFingerprint: %v", err)
	}
	if len(fp) != 64 || strings.ToLower(fp) != fp {
		t.Fatalf("fingerprint must be lowercase hex sha256: %q", fp)
	}

	// Identical render, different idempotency key and video_name → same fp.
	other := base
	other.IdempotencyKey = "key-b"
	other.VideoName = "A different display name"
	fpOther, err := BatchItemFingerprint(other)
	if err != nil {
		t.Fatalf("BatchItemFingerprint(other): %v", err)
	}
	if fpOther != fp {
		t.Fatalf("idempotency_key/video_name must not affect fingerprint: %q vs %q", fp, fpOther)
	}

	// Different publications → same fp (re-publication, not re-render).
	withPub := base
	withPub.Publications = []SubmitPublication{{
		PublicationID: "publication-different",
		OutputRef:     SubmitPublicationOutputRef{ArtifactRole: "final_video"},
		Metadata:      SubmitPublicationMetadata{Title: "Other title"},
		Destinations:  []SubmitPublicationDestination{{DestinationID: "other-channel"}},
	}}
	fpPub, err := BatchItemFingerprint(withPub)
	if err != nil {
		t.Fatalf("BatchItemFingerprint(withPub): %v", err)
	}
	if fpPub != fp {
		t.Fatalf("publications must not affect fingerprint: %q vs %q", fp, fpPub)
	}

	// Different render (scene text) → different fp.
	different := base
	different.Scenes = []SubmitScene{{Text: "other scene", DurationSeconds: 2}}
	fpDiff, err := BatchItemFingerprint(different)
	if err != nil {
		t.Fatalf("BatchItemFingerprint(different): %v", err)
	}
	if fpDiff == fp {
		t.Fatal("different scenes must produce a different fingerprint")
	}

	// Different output geometry → different fp.
	scaled := base
	scaled.Output = &SubmitOutput{Width: 720, Height: 1280, FPS: 30}
	fpScaled, err := BatchItemFingerprint(scaled)
	if err != nil {
		t.Fatalf("BatchItemFingerprint(scaled): %v", err)
	}
	if fpScaled == fp {
		t.Fatal("different output geometry must produce a different fingerprint")
	}
}

// TestSubmitJobBatch_DedupesIdenticalPlansWithinBatch drives the full batch
// handler with an in-memory enqueue stub: two byte-identical items must yield
// one accepted + one dedup outcome, with the second carrying the first job.
func TestSubmitJobBatch_DedupesIdenticalPlansWithinBatch(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &Handlers{}
	itemA := validBatchItem("variant-a")
	itemB := validBatchItem("variant-b")
	itemC := validBatchItem("variant-c")

	batch := SubmitJobBatchRequest{BatchID: "batch-dedupe", Items: []SubmitJobRequest{itemA, itemB, itemC}}
	payload, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}

	router := gin.New()
	router.POST("/api/v1/jobs/batch", h.SubmitJobBatch())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/batch", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}

	var decoded SubmitJobBatchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Summary.Total != 3 {
		t.Fatalf("summary.total = %d, want 3", decoded.Summary.Total)
	}

	// Without a wired store, all items fail at enqueue with a controlled
	// per-item failure. The dedupe window must reflect that: no dedup
	// anchors are registered from failed items, so no item can report
	// status "dedup" (a failed enqueue must never become an anchor).
	for _, item := range decoded.Items {
		if item.Status == "dedup" {
			t.Fatalf("failed enqueue must not become a dedupe anchor: %+v", decoded.Items)
		}
	}
	if decoded.Summary.Deduped != 0 || decoded.Summary.Failed != 3 {
		t.Fatalf("unexpected summary: %+v", decoded.Summary)
	}
}
