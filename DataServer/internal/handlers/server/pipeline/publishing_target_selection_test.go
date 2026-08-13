package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	targetpublishing "velox-server/internal/publishing"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

type publishingTargetDestinationReader struct {
	statuses map[string]store.DeliveryDestinationStatus
	rows     map[string]*store.DeliveryDestination
}

func (f publishingTargetDestinationReader) BatchDeliveryDestinations(_ context.Context, ids []string) (map[string]*store.DeliveryDestination, error) {
	out := make(map[string]*store.DeliveryDestination, len(ids))
	for _, id := range ids {
		if row := f.rows[id]; row != nil {
			copy := *row
			if status, ok := f.statuses[id]; ok {
				copy.Enabled = status == store.DeliveryDestinationEnabled
			}
			out[id] = &copy
			continue
		}
		if status, ok := f.statuses[id]; ok && status == store.DeliveryDestinationDisabled {
			out[id] = &store.DeliveryDestination{DestinationID: id, Enabled: false}
		}
	}
	return out, nil
}

func TestValidateSubmitJobRequestPublishingTargetRules(t *testing.T) {
	base := SubmitJobRequest{
		IdempotencyKey: "target-validation-001",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 1}},
	}
	cases := []struct {
		name   string
		target *SubmitPublishingTarget
		plan   []SubmitDeliveryPlanEntry
		issue  string
	}{
		{"channel requires destination", &SubmitPublishingTarget{WorkspaceID: 1, Type: "channel"}, nil, "required_for_channel"},
		{"group requires id", &SubmitPublishingTarget{WorkspaceID: 1, Type: "group"}, nil, "required_for_group"},
		{"channel forbids group id", &SubmitPublishingTarget{WorkspaceID: 1, Type: "channel", DestinationID: "dest", GroupID: 2}, nil, "forbidden_for_channel"},
		{"group forbids destination", &SubmitPublishingTarget{WorkspaceID: 1, Type: "group", GroupID: 2, DestinationID: "dest"}, nil, "forbidden_for_group"},
		{"selector conflicts with legacy plan", &SubmitPublishingTarget{WorkspaceID: 1, Type: "channel", DestinationID: "dest"}, []SubmitDeliveryPlanEntry{{DestinationID: "legacy"}}, "conflicts_with_delivery_plan"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := base
			req.PublishingTarget = tc.target
			req.DeliveryPlan = tc.plan
			verr, bad := ValidateSubmitJobRequest(req)
			if !bad || verr == nil {
				t.Fatalf("request should be rejected: err=%v", verr)
			}
			for _, detail := range verr.Details {
				if detail["issue"] == tc.issue {
					return
				}
			}
			t.Fatalf("missing issue %q in details: %#v", tc.issue, verr.Details)
		})
	}
}

func TestSubmitPublishingTargetStrictJSONRoundTrip(t *testing.T) {
	var req SubmitJobRequest
	if err := decodeStrictJSON(strings.NewReader(`{"idempotency_key":"target-json-001","scenes":[{"text":"scene","duration_seconds":1}],"publishing_target":{"workspace_id":42,"type":"channel","destination_id":"instaedit_ext"}}`), &req); err != nil {
		t.Fatalf("valid publishing_target rejected: %v", err)
	}
	if req.PublishingTarget == nil || req.PublishingTarget.Type != "channel" || req.PublishingTarget.DestinationID != "instaedit_ext" {
		t.Fatalf("decoded target = %#v", req.PublishingTarget)
	}
	if err := decodeStrictJSON(strings.NewReader(`{"idempotency_key":"target-json-002","scenes":[{"text":"scene","duration_seconds":1}],"publishing_target":{"workspace_id":42,"type":"channel","unknown":true}}`), &req); err == nil {
		t.Fatal("unknown publishing_target field must be rejected by strict JSON")
	}
}

func TestResolveSelectionExpandsGroupToConcreteDeliveryPlanDeterministically(t *testing.T) {
	catalog := &targetpublishing.Catalog{
		WorkspaceID: 42,
		Platform:    "youtube",
		Groups: []targetpublishing.Group{{
			GroupID:                7,
			WorkspaceID:            42,
			MemberCount:            2,
			PublishableMemberCount: 2,
			Eligible:               true,
			Members: []targetpublishing.GroupMember{
				{WorkspaceID: 42, PlatformAccountID: 102, ExternalDestinationID: "ext-b", Enabled: true, CanPost: true, Capabilities: targetpublishing.Capabilities{UploadVideo: true}},
				{WorkspaceID: 42, PlatformAccountID: 101, ExternalDestinationID: "ext-a", Enabled: true, CanPost: true, Capabilities: targetpublishing.Capabilities{UploadVideo: true}},
			},
		}},
	}
	reader := publishingTargetDestinationReader{
		statuses: map[string]store.DeliveryDestinationStatus{},
		rows:     map[string]*store.DeliveryDestination{},
	}
	for _, member := range catalog.Groups[0].Members {
		id := targetpublishing.DestinationIDForExternal(member.ExternalDestinationID)
		reader.statuses[id] = store.DeliveryDestinationEnabled
		reader.rows[id] = &store.DeliveryDestination{
			DestinationID:         id,
			Provider:              targetpublishing.ProviderSocialGateway,
			Enabled:               true,
			ExternalDestinationID: member.ExternalDestinationID,
			ConfigurationJSON:     fmt.Sprintf(`{"workspace_id":42,"platform":"youtube","platform_account_id":%d}`, member.PlatformAccountID),
		}
	}
	resolver := targetpublishing.NewTargetResolver(nil, reader)
	selection, err := resolver.ResolveSelection(context.Background(), targetpublishing.SelectionRequest{
		CatalogRequest: targetpublishing.CatalogRequest{WorkspaceID: 42, Platform: "youtube"},
		Catalog:        catalog,
		GroupIDs:       []int64{7},
	})
	if err != nil {
		t.Fatalf("ResolveSelection: %v", err)
	}
	want := []string{"instaedit_ext-a", "instaedit_ext-b"}
	if len(selection.DestinationIDs) != len(want) {
		t.Fatalf("concrete destinations = %#v, want %#v", selection.DestinationIDs, want)
	}
	for i := range want {
		if selection.DestinationIDs[i] != want[i] {
			t.Fatalf("destination order = %#v, want %#v", selection.DestinationIDs, want)
		}
	}
}

type publishingTargetCatalogClient struct {
	response *socialclient.PublishingTargetCatalogResponse
}

func (f publishingTargetCatalogClient) ListPublishingCatalog(context.Context, int64, string) (*socialclient.PublishingTargetCatalogResponse, error) {
	return f.response, nil
}

func TestResolvePublishingTargetAdapterProjectsConcretePlan(t *testing.T) {
	channel := socialclient.PublishingTarget{
		WorkspaceID:           42,
		PlatformAccountID:     101,
		Platform:              "youtube",
		ChannelID:             "UC-101",
		ChannelName:           "Channel 101",
		ExternalDestinationID: "ext-101",
		Status:                "active",
		Enabled:               true,
		CanPost:               true,
		Capabilities:          socialclient.PublishingCapabilities{UploadVideo: true},
	}
	destinationID := targetpublishing.DestinationIDForExternal(channel.ExternalDestinationID)
	reader := publishingTargetDestinationReader{
		statuses: map[string]store.DeliveryDestinationStatus{destinationID: store.DeliveryDestinationEnabled},
		rows: map[string]*store.DeliveryDestination{destinationID: {
			DestinationID:         destinationID,
			Provider:              targetpublishing.ProviderSocialGateway,
			Enabled:               true,
			ExternalDestinationID: channel.ExternalDestinationID,
			ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":101,"channel_id":"UC-101"}`,
		}},
	}
	h := &Handlers{
		targetResolver: targetpublishing.NewTargetResolver(
			publishingTargetCatalogClient{response: &socialclient.PublishingTargetCatalogResponse{
				Valid: true, ResolvedTargets: []socialclient.PublishingTarget{channel},
			}}, reader),
		socialClient: socialclient.New(socialclient.Config{BaseURL: "http://catalog.test"}),
		store:        &store.SQLiteStore{},
	}
	resolved, err := h.resolvePublishingTarget(context.Background(), SubmitJobRequest{
		IdempotencyKey: "adapter-channel-001",
		PublishingTarget: &SubmitPublishingTarget{
			WorkspaceID: 42, Type: "channel", DestinationID: destinationID,
		},
	})
	if err != nil {
		t.Fatalf("resolvePublishingTarget: %v", err)
	}
	if resolved.PublishingTarget != nil {
		t.Fatal("publishing_target must be removed after resolution")
	}
	if len(resolved.DeliveryPlan) != 1 || resolved.DeliveryPlan[0].DestinationID != destinationID {
		t.Fatalf("resolved delivery_plan = %#v", resolved.DeliveryPlan)
	}
	if resolved.DeliveryPlan[0].RetryBudget == nil || *resolved.DeliveryPlan[0].RetryBudget != DefaultRetryBudget {
		t.Fatalf("resolved retry budget = %#v, want %d", resolved.DeliveryPlan[0].RetryBudget, DefaultRetryBudget)
	}
}

func TestSubmitJobHTTPResolvesPublishingTargetIntoConcreteDeliveryPlan(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, db := newSubmitJobE2EStack(t)
	defer db.Close()

	const destinationID = "instaedit_ext-http-101"
	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedTargets: []socialclient.PublishingTarget{{
				WorkspaceID: 42, PlatformAccountID: 101, Platform: "youtube",
				ChannelID: "UC-http-101", ChannelName: "HTTP Channel",
				ExternalDestinationID: "ext-http-101", Status: "active",
				Enabled: true, CanPost: true,
				Capabilities: socialclient.PublishingCapabilities{UploadVideo: true},
			}},
		})
	}))
	defer catalogServer.Close()
	h.WithSocialClient(socialclient.New(socialclient.Config{BaseURL: catalogServer.URL}))
	if _, err := db.DB().Exec(`INSERT INTO delivery_destinations (destination_id, provider, external_destination_id, name, enabled, configuration_json, created_at, updated_at) VALUES (?, 'social_gateway', ?, 'HTTP Channel', 1, ?, datetime('now'), datetime('now'))`, destinationID, "ext-http-101", `{"workspace_id":42,"platform":"youtube","platform_account_id":101,"channel_id":"UC-http-101"}`); err != nil {
		t.Fatalf("seed destination: %v", err)
	}

	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)
	body := validSubmitJobBody("http-target-001")
	body.DeliveryPlan = nil
	body.PublishingTarget = &SubmitPublishingTarget{WorkspaceID: 42, Type: "channel", DestinationID: destinationID}
	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("POST with publishing_target: status=%d body=%s", w.Code, w.Body.String())
	}
	var response struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	var gotDestination string
	if err := db.DB().QueryRow(`SELECT destination_id FROM job_delivery_plans WHERE job_id = ?`, response.JobID).Scan(&gotDestination); err != nil {
		t.Fatalf("read concrete delivery plan: %v", err)
	}
	if gotDestination != destinationID {
		t.Fatalf("persisted destination_id=%q, want %q", gotDestination, destinationID)
	}
}

func TestSubmitJobHTTPResolvesGroupIntoDeterministicConcretePlans(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, db := newSubmitJobE2EStack(t)
	defer db.Close()

	members := []struct {
		accountID int64
		external  string
	}{
		{accountID: 202, external: "ext-group-b"},
		{accountID: 101, external: "ext-group-a"},
	}
	for _, member := range members {
		destinationID := targetpublishing.DestinationIDForExternal(member.external)
		if _, err := db.DB().Exec(`INSERT INTO delivery_destinations (destination_id, provider, external_destination_id, name, enabled, configuration_json, created_at, updated_at) VALUES (?, 'social_gateway', ?, ?, 1, ?, datetime('now'), datetime('now'))`, destinationID, member.external, member.external, fmt.Sprintf(`{"workspace_id":42,"platform":"youtube","platform_account_id":%d}`, member.accountID)); err != nil {
			t.Fatalf("seed group destination %s: %v", destinationID, err)
		}
	}

	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedGroups: []socialclient.PublishingGroup{{
				WorkspaceID: 42, GroupID: 77, Name: "Group 77", MemberCount: 2,
				PublishableMemberCount: 2, Status: "active", CanPost: true,
				Members: []socialclient.PublishingGroupMember{
					{WorkspaceID: 42, PlatformAccountID: 202, ExternalDestinationID: "ext-group-b", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
					{WorkspaceID: 42, PlatformAccountID: 101, ExternalDestinationID: "ext-group-a", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
				},
			}},
		})
	}))
	defer catalogServer.Close()
	h.WithSocialClient(socialclient.New(socialclient.Config{BaseURL: catalogServer.URL}))

	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)
	body := validSubmitJobBody("http-group-001")
	body.DeliveryPlan = nil
	body.PublishingTarget = &SubmitPublishingTarget{WorkspaceID: 42, Type: "group", GroupID: 77}
	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("group submit: status=%d body=%s", w.Code, w.Body.String())
	}
	var response struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	rows, err := db.DB().Query(`SELECT destination_id FROM job_delivery_plans WHERE job_id = ? ORDER BY priority ASC, destination_id ASC`, response.JobID)
	if err != nil {
		t.Fatalf("read group delivery plans: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var destinationID string
		if err := rows.Scan(&destinationID); err != nil {
			t.Fatal(err)
		}
		got = append(got, destinationID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"instaedit_ext-group-a", "instaedit_ext-group-b"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("persisted group delivery plan = %#v, want %#v", got, want)
	}
}

func TestSubmitJobHTTPRejectsGroupAtomicallyWhenMemberDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, db := newSubmitJobE2EStack(t)
	defer db.Close()

	for _, member := range []struct {
		accountID int64
		external  string
		enabled   int
	}{
		{accountID: 101, external: "ext-disabled-a", enabled: 1},
		{accountID: 202, external: "ext-disabled-b", enabled: 0},
	} {
		destinationID := targetpublishing.DestinationIDForExternal(member.external)
		if _, err := db.DB().Exec(`INSERT INTO delivery_destinations (destination_id, provider, external_destination_id, name, enabled, configuration_json, created_at, updated_at) VALUES (?, 'social_gateway', ?, ?, ?, ?, datetime('now'), datetime('now'))`, destinationID, member.external, member.external, member.enabled, fmt.Sprintf(`{"workspace_id":42,"platform":"youtube","platform_account_id":%d}`, member.accountID)); err != nil {
			t.Fatalf("seed disabled-group destination %s: %v", destinationID, err)
		}
	}

	catalogServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(socialclient.PublishingTargetCatalogResponse{
			Valid: true,
			ResolvedGroups: []socialclient.PublishingGroup{{
				WorkspaceID: 42, GroupID: 88, Name: "Group 88", MemberCount: 2,
				PublishableMemberCount: 2, Status: "active", CanPost: true,
				Members: []socialclient.PublishingGroupMember{
					{WorkspaceID: 42, PlatformAccountID: 101, ExternalDestinationID: "ext-disabled-a", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
					{WorkspaceID: 42, PlatformAccountID: 202, ExternalDestinationID: "ext-disabled-b", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
				},
			}},
		})
	}))
	defer catalogServer.Close()
	h.WithSocialClient(socialclient.New(socialclient.Config{BaseURL: catalogServer.URL}))

	count := func(table string) int {
		var n int
		if err := db.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		return n
	}
	baseline := map[string]int{
		"jobs": count("jobs"), "tasks": count("tasks"),
		"task_specs": count("task_specs"), "job_delivery_plans": count("job_delivery_plans"),
	}

	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)
	body := validSubmitJobBody("http-group-disabled-001")
	body.DeliveryPlan = nil
	body.PublishingTarget = &SubmitPublishingTarget{WorkspaceID: 42, Type: "group", GroupID: 88}
	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("disabled group submit: status=%d body=%s", w.Code, w.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["error"] != "invalid_payload" || response["ok"] != false {
		t.Fatalf("disabled group response = %#v", response)
	}
	message, _ := response["message"].(string)
	if !strings.Contains(message, "group_id=88") || !strings.Contains(message, "instaedit_ext-disabled-b") {
		t.Fatalf("disabled group response lacks group/member context: %#v", response)
	}
	for table, want := range baseline {
		if got := count(table); got != want {
			t.Errorf("%s rows after rejected group = %d, want %d", table, got, want)
		}
	}
}

func TestSubmitPublishingTargetDoesNotLeakIntoWorkerPayload(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "target-projection-001",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 1}},
		PublishingTarget: &SubmitPublishingTarget{
			WorkspaceID: 42, Type: "channel", DestinationID: "instaedit_ext",
		},
	}
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical.WorkerPayload == nil {
		t.Fatal("worker payload is nil")
	}
	for _, key := range []string{"publishing_target", "workspace_id", "target_type", "group_id"} {
		if _, ok := canonical.WorkerPayload[key]; ok {
			t.Fatalf("worker payload leaked selector key %q: %#v", key, canonical.WorkerPayload[key])
		}
	}
}

func TestSubmitJobRequestWireDTOIncludesPublishingTarget(t *testing.T) {
	data, err := json.Marshal(SubmitJobRequest{
		IdempotencyKey: "wire-001",
		PublishingTarget: &SubmitPublishingTarget{
			WorkspaceID: 42, Type: "group", GroupID: 7,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "publishing_target") {
		t.Fatalf("wire payload omitted publishing_target: %s", data)
	}
}
