package socialclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// PublishingCatalogRequest asks the Social API for every channel and group
// bound to one workspace and platform. The discriminator is fixed to catalog
// by the client so callers cannot accidentally send a channel/group validation
// shape.
type PublishingCatalogRequest struct {
	WorkspaceID int64  `json:"workspace_id"`
	Platform    string `json:"platform"`
	Target      struct {
		Type string `json:"type"`
	} `json:"target"`
}

// PublishingTargetCatalogRequest is the historical name kept for callers of
// the channel-only catalog method. It is an alias so both endpoints share the
// exact same upstream wire shape.
type PublishingTargetCatalogRequest = PublishingCatalogRequest

// PublishingGroup is a group selection returned by InstaEdit's catalog. Group
// expansion is deliberately not performed by this client; Velox receives the
// authoritative membership summary and the group_id for a later server-side
// resolution step.
type PublishingGroup struct {
	GroupID                int64  `json:"group_id"`
	Name                   string `json:"name"`
	ParentGroupID          *int64 `json:"parent_group_id"`
	MemberCount            int    `json:"member_count"`
	PublishableMemberCount int    `json:"publishable_member_count"`
	Status                 string `json:"status,omitempty"`
	CanPost                bool   `json:"can_post"`
	BlockReason            string `json:"block_reason,omitempty"`
	TargetErrorCode        string `json:"target_error_code,omitempty"`
}

// PublishingCapabilities mirrors the provider-neutral capability block owned
// by InstaEdit. Velox treats these booleans as discovery data only.
type PublishingCapabilities struct {
	UploadVideo  bool `json:"upload_video"`
	SetThumbnail bool `json:"set_thumbnail"`
	Publish      bool `json:"publish"`
	Schedule     bool `json:"schedule"`
}

// PublishingTarget is one selectable or blocked social channel. The opaque
// ExternalDestinationID is the only delivery identifier Velox persists;
// channel/account fields exist for display and audit.
type PublishingTarget struct {
	PlatformAccountID     int64                  `json:"platform_account_id"`
	Platform              string                 `json:"platform"`
	ChannelID             string                 `json:"channel_id"`
	ChannelName           string                 `json:"channel_name,omitempty"`
	ExternalDestinationID string                 `json:"external_destination_id,omitempty"`
	Status                string                 `json:"status"`
	Enabled               bool                   `json:"enabled"`
	CanPost               bool                   `json:"can_post"`
	BlockReason           string                 `json:"block_reason,omitempty"`
	Capabilities          PublishingCapabilities `json:"capabilities"`
	TargetErrorCode       string                 `json:"target_error_code,omitempty"`
}

// PublishingTargetCatalogResponse is returned by InstaEdit's canonical
// resolve-target endpoint for target.type=catalog.
type PublishingTargetCatalogResponse struct {
	Valid           bool               `json:"valid"`
	DestinationID   string             `json:"destination_id,omitempty"`
	ResolvedTargets []PublishingTarget `json:"resolved_targets"`
	ResolvedGroups  []PublishingGroup  `json:"resolved_groups,omitempty"`
	// Groups is accepted as an upstream compatibility alias. New callers
	// should emit resolved_groups; the handler merges both fields by group_id.
	Groups    []PublishingGroup `json:"groups,omitempty"`
	ErrorCode string            `json:"error_code,omitempty"`
	Message   string            `json:"message,omitempty"`
}

// ListPublishingCatalog discovers channels and groups through the same
// internal Social API boundary used for validation and delivery. It performs
// one HTTP attempt; the caller owns retry and response policy.
func (c *Client) ListPublishingCatalog(ctx context.Context, workspaceID int64, platform string) (*PublishingTargetCatalogResponse, error) {
	if c == nil || c.cfg.BaseURL == "" {
		return nil, ErrNotConfigured
	}
	if workspaceID <= 0 || platform == "" {
		return nil, fmt.Errorf("%w: workspace_id and platform are required", ErrPermanent)
	}

	payload := PublishingTargetCatalogRequest{WorkspaceID: workspaceID, Platform: platform}
	payload.Target.Type = "catalog"
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal target catalog request: %v", ErrPermanent, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/internal/v1/destinations/resolve-target"), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: build target catalog request: %v", ErrTransient, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: target catalog request failed: %v", ErrTransient, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, classifyStatusError(resp.StatusCode, string(bytes.TrimSpace(respBody)))
	}

	var out PublishingTargetCatalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%w: decode target catalog response: %v", ErrTransient, err)
	}
	if !out.Valid {
		return nil, fmt.Errorf("%w: target catalog invalid: %s %s", ErrPermanent, out.ErrorCode, out.Message)
	}
	if out.ResolvedTargets == nil {
		out.ResolvedTargets = []PublishingTarget{}
	}
	return &out, nil
}

// ListPublishingTargets is the backward-compatible channel catalog method.
// The upstream response may now carry groups, but this method preserves the
// historical API for callers that only consume ResolvedTargets.
func (c *Client) ListPublishingTargets(ctx context.Context, workspaceID int64, platform string) (*PublishingTargetCatalogResponse, error) {
	return c.ListPublishingCatalog(ctx, workspaceID, platform)
}
