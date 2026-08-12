package socialclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"velox-server/internal/credentials"
)

// PublishingCatalogRequest is the internal InstaEdit resolve-target request
// shape retained for the submit resolver. It does not create or persist a
// Velox catalog or membership mirror.
type PublishingCatalogRequest struct {
	WorkspaceID int64  `json:"workspace_id"`
	Platform    string `json:"platform"`
	Target      struct {
		Type string `json:"type"`
	} `json:"target"`
}

// PublishingGroupMember is an upstream InstaEdit membership snapshot used
// transiently during submit validation. Velox never persists or mirrors it.
type PublishingGroupMember struct {
	WorkspaceID             int64                  `json:"workspace_id,omitempty"`
	PlatformAccountID       int64                  `json:"platform_account_id"`
	ExternalDestinationID   string                 `json:"external_destination_id"`
	Enabled                 bool                   `json:"enabled"`
	CanPost                 bool                   `json:"can_post"`
	AccountActive           *bool                  `json:"account_active,omitempty"`
	WorkspaceBindingEnabled *bool                  `json:"workspace_binding_enabled,omitempty"`
	Capabilities            PublishingCapabilities `json:"capabilities"`
}

// PublishingGroup is an upstream InstaEdit group snapshot used transiently
// by the submit resolver. Velox does not own or persist group membership.
type PublishingGroup struct {
	WorkspaceID            int64                   `json:"workspace_id,omitempty"`
	GroupID                int64                   `json:"group_id"`
	Name                   string                  `json:"name"`
	ParentGroupID          *int64                  `json:"parent_group_id"`
	MemberCount            int                     `json:"member_count"`
	PublishableMemberCount int                     `json:"publishable_member_count"`
	Status                 string                  `json:"status,omitempty"`
	CanPost                bool                    `json:"can_post"`
	BlockReason            string                  `json:"block_reason,omitempty"`
	TargetErrorCode        string                  `json:"target_error_code,omitempty"`
	Members                []PublishingGroupMember `json:"members,omitempty"`
}

// PublishingCapabilities carries provider-neutral capability data owned by
// InstaEdit; Velox treats it as transient validation input only.
type PublishingCapabilities struct {
	UploadVideo  bool `json:"upload_video"`
	SetThumbnail bool `json:"set_thumbnail"`
	Publish      bool `json:"publish"`
	Schedule     bool `json:"schedule"`
}

// PublishingTarget is an upstream InstaEdit channel verdict. The opaque
// ExternalDestinationID is the only delivery identifier Velox may persist;
// channel/account fields are transient validation/display data.
type PublishingTarget struct {
	WorkspaceID             int64                  `json:"workspace_id,omitempty"`
	PlatformAccountID       int64                  `json:"platform_account_id"`
	Platform                string                 `json:"platform"`
	ChannelID               string                 `json:"channel_id"`
	ChannelName             string                 `json:"channel_name,omitempty"`
	ExternalDestinationID   string                 `json:"external_destination_id,omitempty"`
	Status                  string                 `json:"status"`
	Enabled                 bool                   `json:"enabled"`
	CanPost                 bool                   `json:"can_post"`
	AccountActive           *bool                  `json:"account_active,omitempty"`
	WorkspaceBindingEnabled *bool                  `json:"workspace_binding_enabled,omitempty"`
	BlockReason             string                 `json:"block_reason,omitempty"`
	Capabilities            PublishingCapabilities `json:"capabilities"`
	TargetErrorCode         string                 `json:"target_error_code,omitempty"`
}

// PublishingTargetCatalogResponse is returned by InstaEdit's canonical
// resolve-target endpoint and is consumed transiently by submit validation;
// it is never synchronized into a Velox catalog.
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

	payload := PublishingCatalogRequest{WorkspaceID: workspaceID, Platform: platform}
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
		return nil, classifyStatusError(resp.StatusCode, credentials.JSON(string(bytes.TrimSpace(respBody))))
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
