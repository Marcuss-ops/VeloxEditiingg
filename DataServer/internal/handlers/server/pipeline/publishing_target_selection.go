package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	targetpublishing "velox-server/internal/publishing"
	"velox-server/internal/socialclient"
)

var errPublishingTargetResolverNotConfigured = errors.New("publishing target resolver is not configured")

// resolvePublishingTarget is the single submit-boundary adapter between the
// public selector and the legacy enqueue contract. It deliberately returns a
// concrete DeliveryPlan so the renderer and delivery persistence paths remain
// unchanged for both channel and group submissions.
func (h *Handlers) resolvePublishingTarget(ctx context.Context, req SubmitJobRequest) (SubmitJobRequest, error) {
	if req.PublishingTarget == nil {
		return req, nil
	}
	if h == nil || h.targetResolver == nil || h.socialClient == nil || h.store == nil {
		return req, errPublishingTargetResolverNotConfigured
	}

	target := req.PublishingTarget
	// The resolver treats platform as opaque/provider-neutral. The adapter
	// still pins the legacy value until the platform is derived from the
	// destination registry (provider-neutral selection follow-up).
	catalog, err := h.targetResolver.ResolveCatalog(ctx, targetpublishing.CatalogRequest{
		WorkspaceID: target.WorkspaceID,
		Platform:    "youtube",
	})
	if err != nil {
		return req, err
	}

	selectionRequest := targetpublishing.SelectionRequest{
		CatalogRequest: targetpublishing.CatalogRequest{
			WorkspaceID: target.WorkspaceID,
			Platform:    "youtube",
		},
		Catalog: catalog,
	}
	switch strings.TrimSpace(target.Type) {
	case "channel":
		selectionRequest.DestinationIDs = []string{strings.TrimSpace(target.DestinationID)}
	case "group":
		selectionRequest.GroupIDs = []int64{target.GroupID}
	default:
		// Shape validation normally catches this. Keep the helper defensive
		// for internal callers that bypass the HTTP validator.
		return req, fmt.Errorf("%w: unsupported target type %q", targetpublishing.ErrInvalidRequest, target.Type)
	}

	selection, err := h.targetResolver.ResolveSelection(ctx, selectionRequest)
	if err != nil {
		return req, err
	}
	if len(selection.DestinationIDs) == 0 {
		return req, fmt.Errorf("%w: selection resolved no concrete destinations", targetpublishing.ErrTargetNotPublishable)
	}

	plan := make([]SubmitDeliveryPlanEntry, 0, len(selection.DestinationIDs))
	for _, destinationID := range selection.DestinationIDs {
		plan = append(plan, SubmitDeliveryPlanEntry{
			DestinationID: destinationID,
			RetryBudget:   intPtr(DefaultRetryBudget),
		})
	}
	req.DeliveryPlan = plan
	// Keep the selector out of every downstream payload and make the
	// canonical request accurately describe the concrete route selected.
	req.PublishingTarget = nil
	return req, nil
}

func writePublishingTargetError(c *gin.Context, err error) {
	status := http.StatusUnprocessableEntity
	code := "invalid_payload"
	switch {
	case errors.Is(err, socialclient.ErrNotConfigured),
		errors.Is(err, socialclient.ErrAuth),
		errors.Is(err, socialclient.ErrRateLimit),
		errors.Is(err, socialclient.ErrTransient),
		errors.Is(err, socialclient.ErrPermanent),
		errors.Is(err, errPublishingTargetResolverNotConfigured):
		// Keep the public error code inside the documented ErrorCode enum;
		// transport/configuration detail remains in the message and status.
		code = "resolver_failure"
		if errors.Is(err, socialclient.ErrNotConfigured) || errors.Is(err, errPublishingTargetResolverNotConfigured) {
			status = http.StatusServiceUnavailable
		} else if errors.Is(err, socialclient.ErrRateLimit) {
			status = http.StatusTooManyRequests
		} else {
			status = http.StatusBadGateway
		}
	}
	detailCode := publishingTargetErrorCode(err)
	c.JSON(status, gin.H{
		"ok":      false,
		"error":   code,
		"message": err.Error(),
		"details": []gin.H{{"path": "publishing_target", "issue": detailCode}},
	})
}

func publishingTargetErrorCode(err error) string {
	switch {
	case errors.Is(err, targetpublishing.ErrTargetNotFound), errors.Is(err, targetpublishing.ErrGroupNotFound):
		return "PUBLISHING_TARGET_NOT_FOUND"
	case errors.Is(err, targetpublishing.ErrTargetNotPublishable), errors.Is(err, targetpublishing.ErrGroupNotPublishable):
		return "PUBLISHING_TARGET_NOT_PUBLISHABLE"
	case errors.Is(err, targetpublishing.ErrDestinationNotFound):
		return "DESTINATION_NOT_FOUND"
	case errors.Is(err, targetpublishing.ErrDestinationDisabled):
		return BlockedCodeVeloxDisabled
	case errors.Is(err, targetpublishing.ErrTargetDestinationInvalid):
		return "PUBLISHING_DESTINATION_INVALID"
	case errors.Is(err, targetpublishing.ErrCatalogInvalid):
		return "PUBLISHING_CATALOG_INVALID"
	case errors.Is(err, targetpublishing.ErrConflictingDuplicate):
		return "PUBLISHING_TARGET_CONFLICT"
	default:
		return "PUBLISHING_TARGET_INVALID"
	}
}
