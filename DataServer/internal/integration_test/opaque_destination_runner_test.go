package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/deliveries"
	"velox-server/internal/deliveries/providers"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

func TestIntegration_DeliveryRunnerForwardsOpaqueDestinationID(t *testing.T) {
	requestBody := make(chan []byte, 1)
	socialRepo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		select {
		case requestBody <- raw:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"social_delivery_id":"runner-opaque-social-1","status":"accepted"}`))
	}))
	defer socialRepo.Close()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "runner-opaque.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	const (
		localDestinationID    = "instaedit_ext-group-a"
		externalDestinationID = "ext-group-a"
		artifactID            = "artifact-runner-opaque"
		deliveryID            = "delivery-runner-opaque"
	)
	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         localDestinationID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalDestinationID,
		Enabled:               true,
		ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":101}`,
	}); err != nil {
		t.Fatalf("InsertDeliveryDestination: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           "job-runner-opaque",
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "opaque.mp4"),
		SHA256:          "runner-opaque-sha256",
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("InsertArtifact: %v", err)
	}
	if err := db.InsertJobDelivery(&store.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  localDestinationID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertJobDelivery: %v", err)
	}

	registry := deliveries.NewRegistry()
	registry.Register(providers.NewSocialGatewayProvider(socialclient.Config{
		BaseURL:         socialRepo.URL,
		CallbackBaseURL: socialRepo.URL,
		Timeout:         2 * time.Second,
	}))
	runner := deliveries.NewDeliveryRunner(&deliveries.RunnerConfig{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Second,
		MaxAttempts:   3,
		ClaimBatch:    1,
		Concurrency:   1,
		BackoffSchedule: []time.Duration{
			10 * time.Millisecond,
		},
	}, registry, db, "runner-opaque-integration")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	var raw []byte
	select {
	case raw = <-requestBody:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Social API request: %v", ctx.Err())
	}

	// Receiving the HTTP request only proves dispatch started. Wait until
	// the runner completes MarkDeliverySucceeded before cancelling its
	// context, otherwise the final DB assertion can race the lease worker.
	deadline := time.Now().Add(time.Second)
	for {
		row, queryErr := db.GetJobDelivery(context.Background(), deliveryID)
		if queryErr == nil && row != nil && row.Status == "SUCCEEDED" && row.RemoteID == "runner-opaque-social-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery did not reach SUCCEEDED: row=%#v err=%v", row, queryErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("runner: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop after context cancellation")
	}

	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("decode Social API request: %v; body=%s", err, raw)
	}
	if got, _ := wire["external_destination_id"].(string); got != externalDestinationID {
		t.Fatalf("external_destination_id=%q, want opaque %q; body=%s", got, externalDestinationID, raw)
	}
	for _, forbidden := range []string{"destination_id", "group_id", "platform_account_id", "channel_id"} {
		if _, present := wire[forbidden]; present {
			t.Fatalf("delivery wire must not contain %q: %s", forbidden, raw)
		}
	}

	row, err := db.GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "runner-opaque-social-1" {
		t.Fatalf("delivery result = status=%q remote_id=%q, want SUCCEEDED/runner-opaque-social-1", row.Status, row.RemoteID)
	}
}
