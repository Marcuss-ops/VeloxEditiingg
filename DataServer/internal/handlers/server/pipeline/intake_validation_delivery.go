package pipeline

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
)

func validateSubmitDelivery(req SubmitJobRequest) []gin.H {
	var details []gin.H
	// publishing_target is an alternate server-side selector. A non-empty
	// delivery_plan and a selector are ambiguous, so reject the combination
	// before any catalog/store lookup. An explicitly empty delivery_plan is
	// treated as absent for backward compatibility with clients that always
	// serialize arrays.
	if req.PublishingTarget != nil {
		target := req.PublishingTarget
		path := "publishing_target"
		if target.WorkspaceID <= 0 {
			details = append(details, gin.H{"path": path + ".workspace_id", "issue": "must_be_positive"})
		}
		targetType := strings.TrimSpace(target.Type)
		if targetType != "channel" && targetType != "group" {
			details = append(details, gin.H{
				"path":    path + ".type",
				"issue":   "unsupported_value",
				"allowed": []string{"channel", "group"},
			})
		}
		if len(req.DeliveryPlan) > 0 {
			details = append(details, gin.H{
				"path":  path,
				"issue": "conflicts_with_delivery_plan",
			})
		}
		switch targetType {
		case "channel":
			if strings.TrimSpace(target.DestinationID) == "" {
				details = append(details, gin.H{"path": path + ".destination_id", "issue": "required_for_channel"})
			}
			if target.GroupID != 0 {
				details = append(details, gin.H{"path": path + ".group_id", "issue": "forbidden_for_channel"})
			}
		case "group":
			if target.GroupID <= 0 {
				details = append(details, gin.H{"path": path + ".group_id", "issue": "required_for_group"})
			}
			if strings.TrimSpace(target.DestinationID) != "" {
				details = append(details, gin.H{"path": path + ".destination_id", "issue": "forbidden_for_group"})
			}
		}
	}

	// Per-delivery-plan-entry validation: destination_id non-empty
	// after trim. RetryBudget has NO upper bound at the OpenAPI layer
	// (only "minimum: 0"); allowing 0 is the whole point of the *int
	// change so the explicit zero-round-trip contract holds.
	for i, d := range req.DeliveryPlan {
		pathPrefix := fmt.Sprintf("delivery_plan.%d", i)
		if strings.TrimSpace(d.DestinationID) == "" {
			details = append(details, gin.H{
				"path":  pathPrefix + ".destination_id",
				"issue": "empty",
			})
		}
	}

	return details
}
