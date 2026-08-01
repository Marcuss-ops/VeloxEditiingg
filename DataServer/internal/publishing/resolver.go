package publishing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

const (
	ProviderSocialGateway = "social_gateway"
	PlatformYouTube       = "youtube"
)

var (
	ErrInvalidRequest           = errors.New("publishing target resolver: invalid request")
	ErrCatalogInvalid           = errors.New("publishing target resolver: invalid catalog")
	ErrTargetNotFound           = errors.New("publishing target resolver: target not found")
	ErrTargetNotPublishable     = errors.New("publishing target resolver: target is not publishable")
	ErrTargetDestinationInvalid = errors.New("publishing target resolver: destination is invalid")
	ErrDestinationNotFound      = errors.New("publishing target resolver: destination not found")
	ErrDestinationDisabled      = errors.New("publishing target resolver: destination is disabled")
	ErrGroupNotFound            = errors.New("publishing target resolver: group not found")
	ErrGroupNotPublishable      = errors.New("publishing target resolver: group is not publishable")
	ErrConflictingDuplicate     = errors.New("publishing target resolver: conflicting duplicate")
)

// CatalogClient is the authoritative upstream catalog boundary.
type CatalogClient interface {
	ListPublishingCatalog(context.Context, int64, string) (*socialclient.PublishingTargetCatalogResponse, error)
}

// DestinationReader is the local Velox destination registry boundary.
type DestinationReader interface {
	// BatchDeliveryDestinations returns one local registry snapshot for all
	// requested opaque destinations. Missing IDs are absent from the map.
	BatchDeliveryDestinations(context.Context, []string) (map[string]*store.DeliveryDestination, error)
}

// TargetResolver validates authoritative publishing targets and selected
// destinations. Group membership belongs to the upstream source of truth and
// is expanded only from its immutable snapshot at selection time, before
// enqueue, so the delivery plan contains concrete channel destinations.
type TargetResolver struct {
	catalog      CatalogClient
	destinations DestinationReader
}

func NewTargetResolver(catalog CatalogClient, destinations DestinationReader) *TargetResolver {
	return &TargetResolver{catalog: catalog, destinations: destinations}
}

// CatalogRequest scopes validation to one workspace and platform.
type CatalogRequest struct {
	WorkspaceID int64
	Platform    string
}

// Catalog is the normalized, validated snapshot used by HTTP handlers and
// future job selection paths. Blocked channels/groups remain present with
// Eligible=false so callers can expose actionable upstream diagnostics.
type Catalog struct {
	WorkspaceID int64
	Platform    string
	Channels    []Channel
	Groups      []Group
}

type Capabilities struct {
	UploadVideo  bool
	SetThumbnail bool
	Publish      bool
	Schedule     bool
}

type Channel struct {
	Type                    string
	DestinationID           string
	ExternalDestinationID   string
	WorkspaceID             int64
	PlatformAccountID       int64
	Platform                string
	ChannelID               string
	Name                    string
	Status                  string
	UpstreamEnabled         bool
	CanPost                 bool
	AccountActive           *bool
	WorkspaceBindingEnabled *bool
	Capabilities            Capabilities
	BlockReason             string
	TargetErrorCode         string
	Eligible                bool
}

type Group struct {
	Type                   string
	GroupID                int64
	WorkspaceID            int64
	Name                   string
	ParentGroupID          *int64
	MemberCount            int
	PublishableMemberCount int
	Status                 string
	CanPost                bool
	BlockReason            string
	TargetErrorCode        string
	Members                []GroupMember
	Eligible               bool
}

type GroupMember struct {
	WorkspaceID             int64
	PlatformAccountID       int64
	ExternalDestinationID   string
	Enabled                 bool
	CanPost                 bool
	AccountActive           *bool
	WorkspaceBindingEnabled *bool
	Capabilities            Capabilities
}

// ResolveCatalog fetches and validates one complete upstream snapshot.
func (r *TargetResolver) ResolveCatalog(ctx context.Context, req CatalogRequest) (*Catalog, error) {
	if err := validateScope(req.WorkspaceID, req.Platform); err != nil {
		return nil, err
	}
	if r == nil || r.catalog == nil {
		return nil, fmt.Errorf("%w: catalog client is not configured", ErrInvalidRequest)
	}
	response, err := r.catalog.ListPublishingCatalog(ctx, req.WorkspaceID, req.Platform)
	if err != nil {
		return nil, err
	}
	return NormalizeCatalog(req, response)
}

// NormalizeCatalog validates a response already fetched from upstream. It is
// exported for deterministic unit tests and for callers that already own the
// upstream request lifecycle.
func NormalizeCatalog(req CatalogRequest, response *socialclient.PublishingTargetCatalogResponse) (*Catalog, error) {
	if err := validateScope(req.WorkspaceID, req.Platform); err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("%w: response is nil", ErrCatalogInvalid)
	}

	out := &Catalog{
		WorkspaceID: req.WorkspaceID,
		Platform:    normalizePlatform(req.Platform),
		Channels:    make([]Channel, 0, len(response.ResolvedTargets)),
		Groups:      make([]Group, 0, len(response.ResolvedGroups)+len(response.Groups)),
	}
	seenExternal := make(map[string]Channel, len(response.ResolvedTargets))
	seenAccounts := make(map[int64]string, len(response.ResolvedTargets))
	for _, target := range response.ResolvedTargets {
		channel, err := normalizeChannel(out.WorkspaceID, out.Platform, target)
		if err != nil {
			return nil, err
		}
		if previous, exists := seenExternal[channel.ExternalDestinationID]; exists {
			if channelsEqual(previous, channel) {
				continue
			}
			return nil, fmt.Errorf("%w: external_destination_id=%q", ErrConflictingDuplicate, channel.ExternalDestinationID)
		}
		if channel.PlatformAccountID > 0 {
			if previousExternal, exists := seenAccounts[channel.PlatformAccountID]; exists && previousExternal != channel.ExternalDestinationID {
				return nil, fmt.Errorf("%w: platform_account_id=%d", ErrConflictingDuplicate, channel.PlatformAccountID)
			}
			seenAccounts[channel.PlatformAccountID] = channel.ExternalDestinationID
		}
		seenExternal[channel.ExternalDestinationID] = channel
		out.Channels = append(out.Channels, channel)
	}

	groups := append(append([]socialclient.PublishingGroup{}, response.ResolvedGroups...), response.Groups...)
	seenGroups := make(map[int64]Group, len(groups))
	for _, upstream := range groups {
		group, err := normalizeGroup(out.WorkspaceID, upstream)
		if err != nil {
			return nil, err
		}
		if previous, exists := seenGroups[group.GroupID]; exists {
			if groupsEqual(previous, group) {
				continue
			}
			return nil, fmt.Errorf("%w: group_id=%d", ErrConflictingDuplicate, group.GroupID)
		}
		seenGroups[group.GroupID] = group
		out.Groups = append(out.Groups, group)
	}
	return out, nil
}

// SelectionRequest is the server-side representation of concrete channel or
// group choices. All requested entries must resolve successfully.
type SelectionRequest struct {
	CatalogRequest
	Catalog        *Catalog
	DestinationIDs []string
	GroupIDs       []int64
}

type Selection struct {
	Channels []Channel
	Groups   []Group

	// DestinationIDs is the concrete, deduplicated snapshot that the
	// enqueue boundary can place into delivery_plan. Group selections are
	// expanded here from the authoritative member snapshot; callers do not
	// need to duplicate membership or destination mapping logic.
	DestinationIDs []string
}

// ResolveSelection validates selected channels against both the upstream
// snapshot and the local destination registry. Group selection is all-or-
// nothing: every reported member must be publishable.
func (r *TargetResolver) ResolveSelection(ctx context.Context, req SelectionRequest) (*Selection, error) {
	if err := validateScope(req.WorkspaceID, req.Platform); err != nil {
		return nil, err
	}
	catalog := req.Catalog
	var err error
	if catalog == nil {
		catalog, err = r.ResolveCatalog(ctx, req.CatalogRequest)
		if err != nil {
			return nil, err
		}
	}
	if catalog.WorkspaceID != req.WorkspaceID || catalog.Platform != normalizePlatform(req.Platform) {
		return nil, fmt.Errorf("%w: catalog scope does not match selection", ErrInvalidRequest)
	}
	if len(req.DestinationIDs) == 0 && len(req.GroupIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one destination or group is required", ErrInvalidRequest)
	}
	if r == nil || r.destinations == nil {
		return nil, fmt.Errorf("%w: destination store is not configured", ErrInvalidRequest)
	}

	channelsByID := make(map[string]Channel, len(catalog.Channels))
	for _, channel := range catalog.Channels {
		channelsByID[channel.DestinationID] = channel
	}
	groupsByID := make(map[int64]Group, len(catalog.Groups))
	for _, group := range catalog.Groups {
		groupsByID[group.GroupID] = group
	}

	selection := &Selection{
		Channels:       make([]Channel, 0, len(req.DestinationIDs)),
		Groups:         make([]Group, 0, len(req.GroupIDs)),
		DestinationIDs: make([]string, 0, len(req.DestinationIDs)),
	}
	candidateByID := make(map[string]Channel)
	candidateGroupByID := make(map[string]int64)
	orderedIDs := make([]string, 0, len(req.DestinationIDs))
	seenDestinations := make(map[string]struct{}, len(req.DestinationIDs))
	seenChannels := make(map[string]struct{}, len(req.DestinationIDs))
	addCandidate := func(channel Channel, groupID int64) {
		id := strings.TrimSpace(channel.DestinationID)
		if id == "" {
			return
		}
		if _, exists := seenDestinations[id]; !exists {
			seenDestinations[id] = struct{}{}
			orderedIDs = append(orderedIDs, id)
			candidateByID[id] = channel
		}
		if groupID > 0 {
			// Keep the first group deterministically when two selected groups
			// share a concrete destination. The destination is still emitted
			// once; only the diagnostic owner needs to be stable.
			if _, exists := candidateGroupByID[id]; !exists {
				candidateGroupByID[id] = groupID
			}
		}
	}

	for _, rawID := range req.DestinationIDs {
		id := strings.TrimSpace(rawID)
		if id == "" {
			return nil, fmt.Errorf("%w: empty destination_id", ErrTargetDestinationInvalid)
		}
		channel, exists := channelsByID[id]
		if !exists {
			return nil, fmt.Errorf("%w: destination_id=%q", ErrTargetNotFound, id)
		}
		if channel.WorkspaceID != 0 && channel.WorkspaceID != req.WorkspaceID {
			return nil, fmt.Errorf("%w: destination_id=%q workspace mismatch", ErrTargetDestinationInvalid, id)
		}
		if channel.Platform != "" && normalizePlatform(channel.Platform) != normalizePlatform(req.Platform) {
			return nil, fmt.Errorf("%w: destination_id=%q platform mismatch", ErrTargetDestinationInvalid, id)
		}
		if !channel.Eligible {
			return nil, fmt.Errorf("%w: destination_id=%q", ErrTargetNotPublishable, id)
		}
		if _, duplicate := seenChannels[id]; !duplicate {
			seenChannels[id] = struct{}{}
			selection.Channels = append(selection.Channels, channel)
		}
		addCandidate(channel, 0)
	}

	groupIDs := append([]int64(nil), req.GroupIDs...)
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	seenGroups := make(map[int64]struct{}, len(groupIDs))
	for _, id := range groupIDs {
		if id <= 0 {
			return nil, fmt.Errorf("%w: group_id=%d", ErrGroupNotFound, id)
		}
		if _, duplicate := seenGroups[id]; duplicate {
			continue
		}
		seenGroups[id] = struct{}{}
		group, exists := groupsByID[id]
		if !exists {
			return nil, fmt.Errorf("%w: group_id=%d", ErrGroupNotFound, id)
		}
		if group.WorkspaceID != 0 && group.WorkspaceID != req.WorkspaceID {
			return nil, fmt.Errorf("%w: group_id=%d workspace mismatch", ErrGroupNotPublishable, id)
		}
		if !group.Eligible {
			return nil, fmt.Errorf("%w: group_id=%d", ErrGroupNotPublishable, id)
		}
		group.Members = orderedGroupMembers(group.Members)
		if len(group.Members) == 0 {
			return nil, fmt.Errorf("%w: group_id=%d has no member snapshot", ErrGroupNotPublishable, id)
		}
		for _, member := range group.Members {
			if member.WorkspaceID != 0 && member.WorkspaceID != req.WorkspaceID {
				return nil, fmt.Errorf("%w: group_id=%d member account=%d workspace mismatch", ErrGroupNotPublishable, id, member.PlatformAccountID)
			}
			if !member.Enabled || !member.CanPost || !optionalBoolTrue(member.AccountActive) || !optionalBoolTrue(member.WorkspaceBindingEnabled) || !member.Capabilities.UploadVideo {
				return nil, fmt.Errorf("%w: group_id=%d member account=%d is not publishable", ErrGroupNotPublishable, id, member.PlatformAccountID)
			}
			addCandidate(Channel{
				DestinationID:           DestinationIDForExternal(member.ExternalDestinationID),
				WorkspaceID:             req.WorkspaceID,
				PlatformAccountID:       member.PlatformAccountID,
				Platform:                normalizePlatform(req.Platform),
				ExternalDestinationID:   member.ExternalDestinationID,
				UpstreamEnabled:         member.Enabled,
				CanPost:                 member.CanPost,
				AccountActive:           member.AccountActive,
				WorkspaceBindingEnabled: member.WorkspaceBindingEnabled,
				Capabilities:            member.Capabilities,
				Eligible:                true,
			}, id)
		}
		selection.Groups = append(selection.Groups, group)
	}

	// Validate every concrete destination against one local registry snapshot.
	// No Selection is returned until all candidates pass, which gives group
	// expansion its all-or-nothing contract even when one member is missing or
	// disabled locally.
	rows, err := r.destinations.BatchDeliveryDestinations(ctx, orderedIDs)
	if err != nil {
		return nil, fmt.Errorf("%w: batch destination lookup: %v", ErrTargetDestinationInvalid, err)
	}
	for _, id := range orderedIDs {
		channel := candidateByID[id]
		row := rows[id]
		if err := validateLocalDestinationSnapshot(req.WorkspaceID, req.Platform, channel, row); err != nil {
			if groupID := candidateGroupByID[id]; groupID > 0 {
				return nil, fmt.Errorf("%w: group_id=%d member destination_id=%q: %v", ErrGroupNotPublishable, groupID, id, err)
			}
			return nil, err
		}
		selection.DestinationIDs = append(selection.DestinationIDs, id)
	}
	return selection, nil
}

func orderedGroupMembers(members []GroupMember) []GroupMember {
	ordered := append([]GroupMember(nil), members...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].PlatformAccountID != ordered[j].PlatformAccountID {
			return ordered[i].PlatformAccountID < ordered[j].PlatformAccountID
		}
		return strings.TrimSpace(ordered[i].ExternalDestinationID) < strings.TrimSpace(ordered[j].ExternalDestinationID)
	})
	return ordered
}

func validateLocalDestinationSnapshot(workspaceID int64, platform string, channel Channel, row *store.DeliveryDestination) error {
	if row == nil {
		return fmt.Errorf("%w: destination_id=%q", ErrDestinationNotFound, channel.DestinationID)
	}
	if !row.Enabled {
		return fmt.Errorf("%w: destination_id=%q", ErrDestinationDisabled, channel.DestinationID)
	}
	if row.Provider != ProviderSocialGateway || row.ExternalDestinationID != channel.ExternalDestinationID {
		return fmt.Errorf("%w: destination_id=%q provider or external id mismatch", ErrTargetDestinationInvalid, channel.DestinationID)
	}
	var metadata struct {
		WorkspaceID       int64  `json:"workspace_id"`
		Platform          string `json:"platform"`
		PlatformAccountID int64  `json:"platform_account_id"`
		ChannelID         string `json:"channel_id"`
	}
	if strings.TrimSpace(row.ConfigurationJSON) != "" {
		if err := json.Unmarshal([]byte(row.ConfigurationJSON), &metadata); err != nil {
			return fmt.Errorf("%w: destination_id=%q configuration: %v", ErrTargetDestinationInvalid, channel.DestinationID, err)
		}
	}
	if metadata.WorkspaceID != 0 && metadata.WorkspaceID != workspaceID {
		return fmt.Errorf("%w: destination_id=%q workspace mismatch", ErrTargetDestinationInvalid, channel.DestinationID)
	}
	if metadata.Platform != "" && normalizePlatform(metadata.Platform) != normalizePlatform(platform) {
		return fmt.Errorf("%w: destination_id=%q platform mismatch", ErrTargetDestinationInvalid, channel.DestinationID)
	}
	if metadata.PlatformAccountID != 0 && metadata.PlatformAccountID != channel.PlatformAccountID {
		return fmt.Errorf("%w: destination_id=%q account mismatch", ErrTargetDestinationInvalid, channel.DestinationID)
	}
	// Group snapshots may not carry a provider channel ID. Compare it only
	// when both the upstream member and local configuration expose one.
	if metadata.ChannelID != "" && channel.ChannelID != "" && metadata.ChannelID != channel.ChannelID {
		return fmt.Errorf("%w: destination_id=%q channel mismatch", ErrTargetDestinationInvalid, channel.DestinationID)
	}
	return nil
}

func validateScope(workspaceID int64, platform string) error {
	if workspaceID <= 0 {
		return fmt.Errorf("%w: workspace_id must be positive", ErrInvalidRequest)
	}
	if normalizePlatform(platform) != PlatformYouTube {
		return fmt.Errorf("%w: platform must be youtube", ErrInvalidRequest)
	}
	return nil
}

func normalizePlatform(platform string) string { return strings.ToLower(strings.TrimSpace(platform)) }

// optionalBoolTrue treats an omitted upstream field as unknown/legacy and
// therefore compatible, while an explicit false is a fail-closed rejection.
func optionalBoolTrue(value *bool) bool {
	return value == nil || *value
}

func normalizeChannel(workspaceID int64, platform string, target socialclient.PublishingTarget) (Channel, error) {
	if target.WorkspaceID != 0 && target.WorkspaceID != workspaceID {
		return Channel{}, fmt.Errorf("%w: channel workspace mismatch", ErrCatalogInvalid)
	}
	if target.Platform != "" && normalizePlatform(target.Platform) != platform {
		return Channel{}, fmt.Errorf("%w: channel platform mismatch", ErrCatalogInvalid)
	}
	externalID := strings.TrimSpace(target.ExternalDestinationID)
	if externalID == "" {
		return Channel{}, fmt.Errorf("%w: channel external_destination_id is empty", ErrCatalogInvalid)
	}
	publishable := target.Enabled && target.CanPost && optionalBoolTrue(target.AccountActive) && optionalBoolTrue(target.WorkspaceBindingEnabled) && target.Capabilities.UploadVideo && strings.EqualFold(strings.TrimSpace(target.Status), "active")
	if publishable && (target.PlatformAccountID <= 0 || strings.TrimSpace(target.ChannelID) == "" || strings.TrimSpace(target.ChannelName) == "" || strings.TrimSpace(target.Status) == "") {
		return Channel{}, fmt.Errorf("%w: channel identity/account fields are incomplete", ErrCatalogInvalid)
	}
	return Channel{
		Type:                    "channel",
		DestinationID:           DestinationIDForExternal(externalID),
		ExternalDestinationID:   externalID,
		WorkspaceID:             workspaceID,
		PlatformAccountID:       target.PlatformAccountID,
		Platform:                platform,
		ChannelID:               strings.TrimSpace(target.ChannelID),
		Name:                    strings.TrimSpace(target.ChannelName),
		Status:                  strings.TrimSpace(target.Status),
		UpstreamEnabled:         target.Enabled,
		CanPost:                 target.CanPost,
		AccountActive:           target.AccountActive,
		WorkspaceBindingEnabled: target.WorkspaceBindingEnabled,
		Capabilities: Capabilities{
			UploadVideo:  target.Capabilities.UploadVideo,
			SetThumbnail: target.Capabilities.SetThumbnail,
			Publish:      target.Capabilities.Publish,
			Schedule:     target.Capabilities.Schedule,
		},
		BlockReason: target.BlockReason, TargetErrorCode: target.TargetErrorCode,
		Eligible: publishable,
	}, nil
}

func normalizeGroup(workspaceID int64, group socialclient.PublishingGroup) (Group, error) {
	if group.WorkspaceID != 0 && group.WorkspaceID != workspaceID {
		return Group{}, fmt.Errorf("%w: group workspace mismatch", ErrCatalogInvalid)
	}
	if group.GroupID <= 0 || strings.TrimSpace(group.Name) == "" {
		return Group{}, fmt.Errorf("%w: group identity fields are incomplete", ErrCatalogInvalid)
	}
	if group.MemberCount < 0 || group.PublishableMemberCount < 0 || group.PublishableMemberCount > group.MemberCount {
		return Group{}, fmt.Errorf("%w: group member counts are invalid", ErrCatalogInvalid)
	}
	members, membersValid := normalizeGroupMembers(workspaceID, group.Members)
	eligible := group.CanPost && group.MemberCount > 0 && group.PublishableMemberCount == group.MemberCount && membersValid && len(members) == group.MemberCount
	return Group{
		Type:                   "group",
		GroupID:                group.GroupID,
		WorkspaceID:            workspaceID,
		Name:                   strings.TrimSpace(group.Name),
		ParentGroupID:          group.ParentGroupID,
		MemberCount:            group.MemberCount,
		PublishableMemberCount: group.PublishableMemberCount,
		Status:                 strings.TrimSpace(group.Status),
		CanPost:                group.CanPost,
		BlockReason:            group.BlockReason,
		TargetErrorCode:        group.TargetErrorCode,
		Members:                members,
		Eligible:               eligible,
	}, nil
}

func normalizeGroupMembers(workspaceID int64, members []socialclient.PublishingGroupMember) ([]GroupMember, bool) {
	if len(members) == 0 {
		return nil, false
	}
	// The upstream snapshot is authoritative, but its transport order is
	// not part of the contract. Sort before validation/projection so the
	// concrete delivery_plan and idempotency hash are stable across refreshes.
	sortedMembers := append([]socialclient.PublishingGroupMember(nil), members...)
	sort.SliceStable(sortedMembers, func(i, j int) bool {
		if sortedMembers[i].PlatformAccountID != sortedMembers[j].PlatformAccountID {
			return sortedMembers[i].PlatformAccountID < sortedMembers[j].PlatformAccountID
		}
		return strings.TrimSpace(sortedMembers[i].ExternalDestinationID) < strings.TrimSpace(sortedMembers[j].ExternalDestinationID)
	})
	out := make([]GroupMember, 0, len(sortedMembers))
	seenExternal := make(map[string]struct{}, len(sortedMembers))
	seenAccounts := make(map[int64]struct{}, len(sortedMembers))
	valid := true
	for _, member := range sortedMembers {
		externalID := strings.TrimSpace(member.ExternalDestinationID)
		if member.WorkspaceID != 0 && member.WorkspaceID != workspaceID || member.PlatformAccountID <= 0 || externalID == "" {
			valid = false
		}
		if _, duplicate := seenExternal[externalID]; duplicate {
			valid = false
		}
		if _, duplicate := seenAccounts[member.PlatformAccountID]; duplicate {
			valid = false
		}
		seenExternal[externalID] = struct{}{}
		seenAccounts[member.PlatformAccountID] = struct{}{}
		if !member.Enabled || !member.CanPost || !optionalBoolTrue(member.AccountActive) || !optionalBoolTrue(member.WorkspaceBindingEnabled) || !member.Capabilities.UploadVideo {
			valid = false
		}
		out = append(out, GroupMember{
			WorkspaceID:             workspaceID,
			PlatformAccountID:       member.PlatformAccountID,
			ExternalDestinationID:   externalID,
			Enabled:                 member.Enabled,
			CanPost:                 member.CanPost,
			AccountActive:           member.AccountActive,
			WorkspaceBindingEnabled: member.WorkspaceBindingEnabled,
			Capabilities: Capabilities{
				UploadVideo:  member.Capabilities.UploadVideo,
				SetThumbnail: member.Capabilities.SetThumbnail,
				Publish:      member.Capabilities.Publish,
				Schedule:     member.Capabilities.Schedule,
			},
		})
	}
	return out, valid
}

func channelsEqual(left, right Channel) bool {
	return left.DestinationID == right.DestinationID &&
		left.ExternalDestinationID == right.ExternalDestinationID &&
		left.PlatformAccountID == right.PlatformAccountID &&
		left.Platform == right.Platform &&
		left.ChannelID == right.ChannelID &&
		left.Name == right.Name &&
		left.Status == right.Status &&
		left.UpstreamEnabled == right.UpstreamEnabled &&
		left.CanPost == right.CanPost &&
		sameOptionalBool(left.AccountActive, right.AccountActive) &&
		sameOptionalBool(left.WorkspaceBindingEnabled, right.WorkspaceBindingEnabled) &&
		left.Capabilities == right.Capabilities &&
		left.BlockReason == right.BlockReason &&
		left.TargetErrorCode == right.TargetErrorCode &&
		left.Eligible == right.Eligible
}

func groupsEqual(left, right Group) bool {
	return left.GroupID == right.GroupID && left.WorkspaceID == right.WorkspaceID && left.Name == right.Name && sameParent(left.ParentGroupID, right.ParentGroupID) && left.MemberCount == right.MemberCount && left.PublishableMemberCount == right.PublishableMemberCount && left.Status == right.Status && left.CanPost == right.CanPost && left.BlockReason == right.BlockReason && left.TargetErrorCode == right.TargetErrorCode && left.Eligible == right.Eligible && groupMembersEqual(left.Members, right.Members)
}

func groupMembersEqual(left, right []GroupMember) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].WorkspaceID != right[i].WorkspaceID ||
			left[i].PlatformAccountID != right[i].PlatformAccountID ||
			left[i].ExternalDestinationID != right[i].ExternalDestinationID ||
			left[i].Enabled != right[i].Enabled ||
			left[i].CanPost != right[i].CanPost ||
			!sameOptionalBool(left[i].AccountActive, right[i].AccountActive) ||
			!sameOptionalBool(left[i].WorkspaceBindingEnabled, right[i].WorkspaceBindingEnabled) ||
			left[i].Capabilities != right[i].Capabilities {
			return false
		}
	}
	return true
}

func sameOptionalBool(left, right *bool) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func sameParent(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func DestinationIDForExternal(externalID string) string {
	return "instaedit_" + strings.TrimSpace(externalID)
}
