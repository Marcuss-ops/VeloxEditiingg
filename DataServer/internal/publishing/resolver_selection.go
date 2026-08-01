// Package publishing / resolver_selection.go — concrete destination resolution.
// Extracted from resolver.go: ResolveSelection and its helpers that validate
// selected channels/groups against the local destination registry snapshot.
package publishing

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"velox-server/internal/store"
)

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
