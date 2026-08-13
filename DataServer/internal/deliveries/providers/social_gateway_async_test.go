package providers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/socialclient"
)

func TestSocialGatewayProvider_acceptedSubmissionRequiresPublishedReconcile(t *testing.T) {
	const socialDeliveryID = "social-async-regression-1"
	statusPath := "/internal/v1/deliveries/" + socialDeliveryID
	statusReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/deliveries":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"social_delivery_id":"` + socialDeliveryID + `","status":"accepted"}`))
		case r.Method == http.MethodGet && r.URL.Path == statusPath:
			statusReads++
			if statusReads == 1 {
				_, _ = w.Write([]byte(`{"delivery_id":"` + socialDeliveryID + `","publish_status":"processing","thumbnail_status":"pending"}`))
				return
			}
			_, _ = w.Write([]byte(`{"delivery_id":"` + socialDeliveryID + `","publish_status":"PUBLISHED","thumbnail_status":"APPLIED","remote_media_id":"remote-final-1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider := NewSocialGatewayProvider(socialclient.Config{
		BaseURL:         server.URL,
		CallbackBaseURL: server.URL,
	})
	accepted, err := provider.Deliver(context.Background(), sampleArtifact(), sampleDestination(), "delivery-async-regression", "idem-async-regression")
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !accepted.Success || accepted.Status != deliveries.ResultStatusSubmittedToProvider || accepted.RemoteID != socialDeliveryID {
		t.Fatalf("accepted result = %+v", accepted)
	}

	pending, err := provider.Reconcile(context.Background(), "delivery-async-regression", socialDeliveryID)
	if err != nil {
		t.Fatalf("pending Reconcile: %v", err)
	}
	if pending.Success || pending.Status != deliveries.ResultStatusRemoteProcessing || pending.RemoteID != "" {
		t.Fatalf("pending reconciliation result = %+v; accepted operation must not be complete", pending)
	}

	published, err := provider.Reconcile(context.Background(), "delivery-async-regression", socialDeliveryID)
	if err != nil {
		t.Fatalf("published Reconcile: %v", err)
	}
	if !published.Success || published.Status != deliveries.ResultStatusPublished || published.RemoteID != "remote-final-1" {
		t.Fatalf("published reconciliation result = %+v", published)
	}
}
