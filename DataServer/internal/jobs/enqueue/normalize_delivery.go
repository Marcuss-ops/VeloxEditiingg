// Package enqueue - delivery plan validation and retry extraction.
package enqueue

import (
	"fmt"

	"velox-server/internal/jobs"
	"velox-shared/contract/deliveryplan"
)

func validatePlanPayload(plan *ResolvedPlan, job *jobs.Job) error {
	if plan == nil || len(plan.Destinations) == 0 {
		return deliveryplan.NewValidationError(
			"delivery_plan",
			"no explicit delivery plan; create job_delivery_plans rows for this job before enqueueing",
		)
	}
	maxRetry := 0
	for i, d := range plan.Destinations {
		if d.RetryBudget < 0 {
			return deliveryplan.NewValidationError(
				fmt.Sprintf("delivery_plan.%d.retry_budget", i),
				"must be >= 0",
			)
		}
		if d.RetryBudget > maxRetry {
			maxRetry = d.RetryBudget
		}
	}
	if job != nil {
		job.MaxRetries = maxRetry
	}
	return nil
}
func extractPlanMaxRetry(payload map[string]interface{}) int {
	if payload == nil {
		return 0
	}
	planRaw, ok := payload["delivery_plan"]
	if !ok {
		return 0
	}
	arr, ok := planRaw.([]interface{})
	if !ok {
		return 0
	}
	maxRetry := 0
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		switch v := m["retry_budget"].(type) {
		case int:
			if v > maxRetry {
				maxRetry = v
			}
		case int64:
			if int(v) > maxRetry {
				maxRetry = int(v)
			}
		case float64:
			if int(v) > maxRetry {
				maxRetry = int(v)
			}
		}
	}
	return maxRetry
}
