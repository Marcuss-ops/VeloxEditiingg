// Package publishing / resolver.go — publishing target resolution (planning).
//
// TargetResolver validates authoritative publishing targets and selected
// destinations. Group membership belongs to the upstream source of truth and
// is expanded only from its immutable snapshot at selection time, before
// enqueue, so the delivery plan contains concrete channel destinations.
//
// File layout (per-responsibility split):
//
//	resolver.go           — planning + shared domain: consts, errors,
//	                        interfaces, request/selection types,
//	                        TargetResolver, NewTargetResolver,
//	                        ResolveCatalog, validateScope.
//	resolver_normalize.go — normalization: NormalizeCatalog + normalize/equal
//	                        helpers + DestinationIDForExternal.
//	resolver_selection.go — destination resolution: ResolveSelection,
//	                        orderedGroupMembers,
//	                        validateLocalDestinationSnapshot.
package publishing

import (
	"context"
	"errors"
	"fmt"

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

func validateScope(workspaceID int64, platform string) error {
	if workspaceID <= 0 {
		return fmt.Errorf("%w: workspace_id must be positive", ErrInvalidRequest)
	}
	if normalizePlatform(platform) != PlatformYouTube {
		return fmt.Errorf("%w: platform must be youtube", ErrInvalidRequest)
	}
	return nil
}
