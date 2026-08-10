package instaedit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

type callbackApplierFake struct {
	calls []store.InstaEditDeliveryEvent
}

func (f *callbackApplierFake) ApplyInstaEditDeliveryEvent(_ context.Context, event store.InstaEditDeliveryEvent) (bool, error) {
	f.calls = append(f.calls, event)
	return true, nil
}

func signedCallbackRequest(t *testing.T, secret string, body []byte, eventID string, ts int64) *http.Request {
	t.Helper()
	timestamp := strconv.FormatInt(ts, 10)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "."))
	_, _ = mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/instaedit/delivery-events", bytesReader(body))
	req.Header.Set("X-Event-ID", eventID)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-Signature", signature)
	return req
}

func bytesReader(body []byte) *bytes.Reader { return bytes.NewReader(body) }

func TestInstaEditDeliveryCallback_VerifiesAndPersists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fake := &callbackApplierFake{}
	service := NewService(nil, nil, nil, nil)
	service.deliveryEvents = fake
	h := NewHandler(HandlerDeps{Service: service, WebhookSecret: "callback-secret"})
	r := gin.New()
	h.RegisterInternalCallbackRoute(r)

	event := deliveryEventRequest{
		ContractVersion: "velox.delivery.event.v1", EventID: "evt-1", DeliveryID: "delivery-1",
		Sequence: 2, Status: "published", Phase: "VERIFY", RemoteID: "remote-1",
		OccurredAt: time.Now().UTC(),
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, signedCallbackRequest(t, "callback-secret", body, event.EventID, time.Now().Unix()))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if len(fake.calls) != 1 || fake.calls[0].DeliveryID != "delivery-1" || fake.calls[0].Sequence != 2 {
		t.Fatalf("unexpected persisted callback: %+v", fake.calls)
	}
}

func TestInstaEditDeliveryCallback_RejectsInvalidSignature(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service := NewService(nil, nil, nil, nil)
	service.deliveryEvents = &callbackApplierFake{}
	h := NewHandler(HandlerDeps{Service: service, WebhookSecret: "callback-secret"})
	r := gin.New()
	h.RegisterInternalCallbackRoute(r)

	body := []byte(`{"contract_version":"velox.delivery.event.v1","event_id":"evt-1","delivery_id":"delivery-1","sequence":1,"status":"published","occurred_at":"2026-08-02T12:00:00Z"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/instaedit/delivery-events", bytesReader(body))
	req.Header.Set("X-Event-ID", "evt-1")
	req.Header.Set("X-Timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	req.Header.Set("X-Signature", "sha256="+hex.EncodeToString(make([]byte, sha256.Size)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}
