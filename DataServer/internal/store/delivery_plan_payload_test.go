package store

import "testing"

func TestParseDeliveryPlanPayloadExplicit(t *testing.T) {
	t.Parallel()
	plan, err := parseDeliveryPlanPayload(map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "destination-main",
				"priority":       3.0,
				"retry_budget":   7.0,
			},
		},
	})
	if err != nil {
		t.Fatalf("parse delivery plan: %v", err)
	}
	if len(plan) != 1 {
		t.Fatalf("plan length = %d, want 1", len(plan))
	}
	if plan[0].DestinationID != "destination-main" || plan[0].Priority != 3 || plan[0].RetryBudget != 7 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestParseDeliveryPlanPayloadAliases(t *testing.T) {
	t.Parallel()
	plan, err := parseDeliveryPlanPayload(map[string]interface{}{
		"delivery_destination_ids": []interface{}{"drive-main", "video-main"},
	})
	if err != nil {
		t.Fatalf("parse delivery plan: %v", err)
	}
	if len(plan) != 2 {
		t.Fatalf("plan length = %d, want 2", len(plan))
	}
	if plan[0].Priority != 0 || plan[1].Priority != 1 {
		t.Fatalf("unexpected priorities: %#v", plan)
	}
	if plan[0].RetryBudget != 5 || plan[1].RetryBudget != 5 {
		t.Fatalf("unexpected retry budgets: %#v", plan)
	}
}
