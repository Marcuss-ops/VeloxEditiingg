package publishing

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

type fakeCatalogClient struct {
	response *socialclient.PublishingTargetCatalogResponse
	err      error
}

func (f fakeCatalogClient) ListPublishingCatalog(context.Context, int64, string) (*socialclient.PublishingTargetCatalogResponse, error) {
	return f.response, f.err
}

type fakeDestinationReader struct {
	statuses map[string]store.DeliveryDestinationStatus
	rows     map[string]*store.DeliveryDestination
	err      error
}

func (f fakeDestinationReader) BatchDeliveryDestinations(_ context.Context, ids []string) (map[string]*store.DeliveryDestination, error) {
	if f.err != nil {
		return nil, f.err
	}
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

func healthyTarget(externalID string, accountID int64) socialclient.PublishingTarget {
	return socialclient.PublishingTarget{
		WorkspaceID:           42,
		PlatformAccountID:     accountID,
		Platform:              PlatformYouTube,
		ChannelID:             "UC-" + externalID,
		ChannelName:           "Channel " + externalID,
		ExternalDestinationID: externalID,
		Status:                "active",
		Enabled:               true,
		CanPost:               true,
		Capabilities: socialclient.PublishingCapabilities{
			UploadVideo: true,
			Publish:     true,
		},
	}
}

func healthyGroup(id int64) socialclient.PublishingGroup {
	return socialclient.PublishingGroup{
		WorkspaceID:            42,
		GroupID:                id,
		Name:                   "Group",
		MemberCount:            3,
		PublishableMemberCount: 3,
		Status:                 "active",
		CanPost:                true,
		Members: []socialclient.PublishingGroupMember{
			{WorkspaceID: 42, PlatformAccountID: 201, ExternalDestinationID: "ext-member-1", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
			{WorkspaceID: 42, PlatformAccountID: 202, ExternalDestinationID: "ext-member-2", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
			{WorkspaceID: 42, PlatformAccountID: 203, ExternalDestinationID: "ext-member-3", Enabled: true, CanPost: true, Capabilities: socialclient.PublishingCapabilities{UploadVideo: true}},
		},
	}
}

func TestNormalizeCatalogValidatesScopeCapabilitiesAndDeduplicates(t *testing.T) {
	alias := healthyGroup(7)
	alias.ParentGroupID = ptrInt64(2)
	aliasCopy := healthyGroup(7)
	aliasCopy.ParentGroupID = ptrInt64(2)

	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: " YouTube "}, &socialclient.PublishingTargetCatalogResponse{
		Valid: true,
		ResolvedTargets: []socialclient.PublishingTarget{
			healthyTarget("ext-a", 101),
			healthyTarget("ext-a", 101),
		},
		ResolvedGroups: []socialclient.PublishingGroup{alias},
		Groups:         []socialclient.PublishingGroup{aliasCopy},
	})
	if err != nil {
		t.Fatalf("NormalizeCatalog: %v", err)
	}
	if len(catalog.Channels) != 1 || len(catalog.Groups) != 1 {
		t.Fatalf("deduplicated catalog = channels=%d groups=%d", len(catalog.Channels), len(catalog.Groups))
	}
	if !catalog.Channels[0].Eligible || !catalog.Groups[0].Eligible {
		t.Fatalf("healthy entries should be eligible: %#v %#v", catalog.Channels[0], catalog.Groups[0])
	}
}

func TestNormalizeCatalogRejectsWorkspacePlatformAndAccountConflicts(t *testing.T) {
	cases := []struct {
		name     string
		request  CatalogRequest
		response *socialclient.PublishingTargetCatalogResponse
		want     error
	}{
		{
			name:     "workspace",
			request:  CatalogRequest{WorkspaceID: 0, Platform: PlatformYouTube},
			response: &socialclient.PublishingTargetCatalogResponse{},
			want:     ErrInvalidRequest,
		},
		{
			name:     "platform",
			request:  CatalogRequest{WorkspaceID: 42, Platform: "tiktok"},
			response: &socialclient.PublishingTargetCatalogResponse{},
			want:     ErrInvalidRequest,
		},
		{
			name:     "channel workspace",
			request:  CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
			response: &socialclient.PublishingTargetCatalogResponse{ResolvedTargets: []socialclient.PublishingTarget{{WorkspaceID: 99, PlatformAccountID: 1, Platform: PlatformYouTube, ChannelID: "UC", ChannelName: "Wrong", ExternalDestinationID: "ext", Status: "active"}}},
			want:     ErrCatalogInvalid,
		},
		{
			name:     "account duplicate",
			request:  CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
			response: &socialclient.PublishingTargetCatalogResponse{ResolvedTargets: []socialclient.PublishingTarget{healthyTarget("ext-a", 101), healthyTarget("ext-b", 101)}},
			want:     ErrConflictingDuplicate,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeCatalog(tc.request, tc.response)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want errors.Is(%v)", err, tc.want)
			}
		})
	}
}

func TestNormalizeCatalogRejectsExplicitlyInactiveAccountOrBinding(t *testing.T) {
	inactive := healthyTarget("ext-inactive", 103)
	accountActive := false
	inactive.AccountActive = &accountActive
	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{
		ResolvedTargets: []socialclient.PublishingTarget{inactive},
	})
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Channels[0].Eligible {
		t.Fatal("explicitly inactive account must be ineligible")
	}

	group := healthyGroup(9)
	bindingEnabled := false
	group.Members[0].WorkspaceBindingEnabled = &bindingEnabled
	catalog, err = NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{
		ResolvedGroups: []socialclient.PublishingGroup{group},
	})
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Groups[0].Eligible {
		t.Fatal("explicitly disabled workspace binding must make group ineligible")
	}
}

func TestNormalizeCatalogMarksBlockedTargetsAndGroupsIneligible(t *testing.T) {
	blocked := healthyTarget("ext-blocked", 102)
	blocked.CanPost = false
	blocked.Capabilities.UploadVideo = true
	partial := healthyGroup(8)
	partial.PublishableMemberCount = 2

	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{
		ResolvedTargets: []socialclient.PublishingTarget{blocked},
		ResolvedGroups:  []socialclient.PublishingGroup{partial},
	})
	if err != nil {
		t.Fatalf("NormalizeCatalog: %v", err)
	}
	if catalog.Channels[0].Eligible || catalog.Groups[0].Eligible {
		t.Fatalf("blocked entries must be ineligible: %#v %#v", catalog.Channels[0], catalog.Groups[0])
	}
}

func TestResolveSelectionValidatesLocalDestinationAndDeduplicates(t *testing.T) {
	channel := healthyTarget("ext-a", 101)
	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{ResolvedTargets: []socialclient.PublishingTarget{channel}})
	if err != nil {
		t.Fatal(err)
	}
	reader := fakeDestinationReader{
		statuses: map[string]store.DeliveryDestinationStatus{DestinationIDForExternal("ext-a"): store.DeliveryDestinationEnabled},
		rows: map[string]*store.DeliveryDestination{DestinationIDForExternal("ext-a"): {
			DestinationID:         DestinationIDForExternal("ext-a"),
			Provider:              ProviderSocialGateway,
			ExternalDestinationID: "ext-a",
			Enabled:               true,
			ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":101,"channel_id":"UC-ext-a"}`,
		}},
	}
	resolver := NewTargetResolver(nil, reader)
	selection, err := resolver.ResolveSelection(context.Background(), SelectionRequest{
		CatalogRequest: CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
		Catalog:        catalog,
		DestinationIDs: []string{"  " + DestinationIDForExternal("ext-a") + "  ", DestinationIDForExternal("ext-a")},
	})
	if err != nil {
		t.Fatalf("ResolveSelection: %v", err)
	}
	if len(selection.Channels) != 1 {
		t.Fatalf("channels = %d, want 1 after deduplication", len(selection.Channels))
	}
}

func TestResolveSelectionRejectsDestinationAndGroupFailures(t *testing.T) {
	channel := healthyTarget("ext-a", 101)
	group := healthyGroup(7)
	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{
		ResolvedTargets: []socialclient.PublishingTarget{channel},
		ResolvedGroups:  []socialclient.PublishingGroup{group},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewTargetResolver(nil, fakeDestinationReader{
		statuses: map[string]store.DeliveryDestinationStatus{DestinationIDForExternal("ext-a"): store.DeliveryDestinationDisabled},
		rows:     map[string]*store.DeliveryDestination{},
	})
	_, err = resolver.ResolveSelection(context.Background(), SelectionRequest{
		CatalogRequest: CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
		Catalog:        catalog,
		DestinationIDs: []string{DestinationIDForExternal("ext-a")},
	})
	if !errors.Is(err, ErrDestinationDisabled) {
		t.Fatalf("disabled destination error = %v", err)
	}

	_, err = resolver.ResolveSelection(context.Background(), SelectionRequest{
		CatalogRequest: CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
		Catalog:        catalog,
		GroupIDs:       []int64{999},
	})
	if !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("missing group error = %v", err)
	}
}

func TestResolveSelectionRejectsGroupWhenAnyMemberIsMissingLocally(t *testing.T) {
	group := healthyGroup(7)
	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{
		ResolvedGroups: []socialclient.PublishingGroup{group},
	})
	if err != nil {
		t.Fatal(err)
	}

	memberExternal := group.Members[0].ExternalDestinationID
	reader := fakeDestinationReader{
		statuses: map[string]store.DeliveryDestinationStatus{
			DestinationIDForExternal(memberExternal): store.DeliveryDestinationEnabled,
			// Members 2 and 3 are intentionally absent: group resolution is all-or-nothing.
		},
		rows: map[string]*store.DeliveryDestination{
			DestinationIDForExternal(memberExternal): {
				DestinationID:         DestinationIDForExternal(memberExternal),
				Provider:              ProviderSocialGateway,
				ExternalDestinationID: memberExternal,
				Enabled:               true,
				ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":201}`,
			},
		},
	}
	_, err = NewTargetResolver(nil, reader).ResolveSelection(context.Background(), SelectionRequest{
		CatalogRequest: CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
		Catalog:        catalog,
		GroupIDs:       []int64{7},
	})
	if !errors.Is(err, ErrGroupNotPublishable) {
		t.Fatalf("missing group member error = %v, want ErrGroupNotPublishable", err)
	}
}

func TestResolveSelectionAcceptsGroupWhenAllMembersAreLocallyEnabled(t *testing.T) {
	group := healthyGroup(7)
	catalog, err := NormalizeCatalog(CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube}, &socialclient.PublishingTargetCatalogResponse{
		ResolvedGroups: []socialclient.PublishingGroup{group},
	})
	if err != nil {
		t.Fatal(err)
	}
	statuses := make(map[string]store.DeliveryDestinationStatus, len(group.Members))
	rows := make(map[string]*store.DeliveryDestination, len(group.Members))
	for _, member := range group.Members {
		id := DestinationIDForExternal(member.ExternalDestinationID)
		statuses[id] = store.DeliveryDestinationEnabled
		rows[id] = &store.DeliveryDestination{
			DestinationID:         id,
			Provider:              ProviderSocialGateway,
			ExternalDestinationID: member.ExternalDestinationID,
			Enabled:               true,
			ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":` + strconv.FormatInt(member.PlatformAccountID, 10) + `}`,
		}
	}
	selection, err := NewTargetResolver(nil, fakeDestinationReader{statuses: statuses, rows: rows}).ResolveSelection(context.Background(), SelectionRequest{
		CatalogRequest: CatalogRequest{WorkspaceID: 42, Platform: PlatformYouTube},
		Catalog:        catalog,
		GroupIDs:       []int64{7, 7},
	})
	if err != nil {
		t.Fatalf("ResolveSelection group: %v", err)
	}
	if len(selection.Groups) != 1 || selection.Groups[0].GroupID != 7 {
		t.Fatalf("groups = %#v, want one deduplicated group", selection.Groups)
	}
}

func ptrInt64(value int64) *int64 { return &value }
