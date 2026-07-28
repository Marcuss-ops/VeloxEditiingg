package store

import (
	"strings"
	"testing"
)

func TestParseDeliveryPlanPayloadRejectsInvalidPlans(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload map[string]interface{}
		want    string
	}{
		{
			name: "duplicate",
			payload: map[string]interface{}{
				"delivery_destination_ids": []string{"drive-main", "drive-main"},
			},
			want: "duplicate destination_id",
		},
		{
			name: "disabled",
			payload: map[string]interface{}{
				"delivery_plan": map[string]interface{}{
					"destination_id": "drive-main",
					"enabled":        false,
				},
			},
			want: "disabled",
		},
		{
			// retry_budget=0 is now ALLOWED (openapi.yaml minimum is 0).
			// The parse-layer rejection boundary moved from <=0 to <0.
			// Use -1 so the test keeps pinning the rejection path.
			name: "retry budget",
			payload: map[string]interface{}{
				"delivery_plan": map[string]interface{}{
					"destination_id": "drive-main",
					"retry_budget":   -1,
				},
			},
			want: "retry_budget must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseDeliveryPlanPayload(tt.payload)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}
