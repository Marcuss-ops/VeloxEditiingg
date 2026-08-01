// Package publishing / resolver_normalize.go — upstream snapshot normalization.
// Extracted from resolver.go: NormalizeCatalog and the normalize/equal helpers
// that project the authoritative upstream catalog into validated domain types.
package publishing

import (
	"fmt"
	"sort"
	"strings"

	"velox-server/internal/socialclient"
)

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
