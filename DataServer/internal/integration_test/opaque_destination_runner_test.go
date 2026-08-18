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

	"velox-server/internal/credentials"
	"velox-server/internal/deliveries"
	"velox-server/internal/deliveries/providers"
	"velox-server/internal/jobs"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

func TestIntegration_DeliveryRunnerForwardsOpaqueDestinationID(t *testing.T) {
	requestBody := make(chan []byte, 1)
	socialRepo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			raw, _ := io.ReadAll(r.Body)
			select {
			case requestBody <- raw:
			default:
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"social_delivery_id":"runner-opaque-social-1","status":"accepted"}`))
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"delivery_id":"runner-opaque-social-1","publish_status":"published","thumbnail_status":"applied","remote_media_id":"runner-opaque-remote-final-1"}`))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
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
		jobID                 = "job-runner-opaque"
		artifactID            = "artifact-runner-opaque"
		deliveryID            = "delivery-runner-opaque"
		publicationID         = "publication-runner-opaque"
	)
	if err := db.Delivery().InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         localDestinationID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalDestinationID,
		Enabled:               true,
		ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":101}`,
	}); err != nil {
		t.Fatalf("InsertDeliveryDestination: %v", err)
	}
	keys, err := credentials.NewKeyring(1, map[int][]byte{1: []byte("01234567890123456789012345678901")})
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	vault, err := credentials.NewVault(db, keys)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	credentialRef, err := vault.Put(context.Background(), "social_gateway", "integration-test", []string{"publish"}, time.Now().Add(time.Hour), time.Time{}, credentials.Material{AccessToken: "runner-short-lived-token"})
	if err != nil {
		t.Fatalf("Put credential: %v", err)
	}
	if err := store.NewAtomicJobTaskCreator(db).CreateJobWithTask(context.Background(), &jobs.Job{
		ID:         jobID,
		VideoName:  "opaque destination test",
		ProjectID:  "integration-test",
		RunID:      jobID + "-run",
		MaxRetries: 1,
	}, &taskgraph.TaskSpec{
		Version:    taskgraph.SpecVersion,
		JobID:      jobID,
		ExecutorID: "scene.composite.v1",
		DeliveryPlan: map[string]interface{}{
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": localDestinationID,
					"retry_budget":   3,
					"metadata": map[string]interface{}{
						"credential_ref": credentialRef,
						"publication_id": publicationID,
					},
				},
			},
		},
	}, 0); err != nil {
		t.Fatalf("CreateJobWithTask: %v", err)
	}
	if err := db.CreatePublicationState(context.Background(), publicationID); err != nil {
		t.Fatalf("CreatePublicationState: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           jobID,
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
	if err := db.Delivery().InsertJobDelivery(&store.JobDelivery{
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
	socialGatewayProvider := providers.NewSocialGatewayProvider(socialclient.Config{
		BaseURL:         socialRepo.URL,
		CallbackBaseURL: socialRepo.URL,
		Timeout:         2 * time.Second,
	})
	registry.Register(socialGatewayProvider)
	// Keep the integration setup aligned with production while the social
	// gateway migrates from Deliver to the resumable phase contract.
	registry.RegisterLegacyPhaseProvider(socialGatewayProvider)
	runner := deliveries.NewDeliveryRunner(&deliveries.RunnerConfig{
		PollInterval:    5 * time.Millisecond,
		LeaseDuration:   time.Second,
		MaxAttempts:     3,
		ClaimBatch:      1,
		Concurrency:     1,
		BackoffSchedule: []time.Duration{10 * time.Millisecond},
	}, registry, db.Delivery(), db, "runner-opaque-integration")
	runner.WithCredentialVault(vault)

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
		row, queryErr := db.Delivery().GetJobDelivery(context.Background(), deliveryID)
		if queryErr == nil && row != nil && row.Status == "SUCCEEDED" && row.RemoteID == "runner-opaque-remote-final-1" {
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

	row, err := db.Delivery().GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "runner-opaque-remote-final-1" {
		t.Fatalf("delivery result = status=%q remote_id=%q, want SUCCEEDED/runner-opaque-remote-final-1", row.Status, row.RemoteID)
	}
}
