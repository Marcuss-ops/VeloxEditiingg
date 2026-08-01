package apiwire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSubmitJobBatchRequest_RoundtripPreservesPublicationsAndDeliveryPlan(t *testing.T) {
	retryBudget := 0
	input := SubmitJobBatchRequest{
		BatchID: "batch-publications-001",
		Items: []SubmitJobRequest{{
			IdempotencyKey: "batch-en-001",
			VideoName:      "English video",
			DeliveryPlan: []SubmitDeliveryPlanEntry{{
				DestinationID: "legacy-drive",
				RetryBudget:   3,
			}},
			Publications: []SubmitPublication{{
				PublicationID: "publication-en",
				OutputRef:     SubmitPublicationOutputRef{VariantID: "en"},
				Language:      "en",
				Metadata:      SubmitPublicationMetadata{Title: "English title", Description: "English description"},
				Localizations: map[string]SubmitLocalizedMetadata{"it": {Title: "Titolo italiano"}},
				Destinations: []SubmitPublicationDestination{{
					DestinationID: "youtube-en",
					RetryBudget:   &retryBudget,
				}},
			}},
		}, {
			IdempotencyKey: "batch-it-001",
			Publications: []SubmitPublication{{
				PublicationID: "publication-it",
				OutputRef:     SubmitPublicationOutputRef{ArtifactRole: "final_video"},
				Metadata:      SubmitPublicationMetadata{Title: "Titolo italiano"},
				Destinations:  []SubmitPublicationDestination{{DestinationID: "youtube-it"}},
			}},
		}},
	}

	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded SubmitJobBatchRequest
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.BatchID != input.BatchID || len(decoded.Items) != 2 {
		t.Fatalf("batch envelope roundtrip = %+v", decoded)
	}
	if len(decoded.Items[0].Publications) != 1 || decoded.Items[0].Publications[0].Metadata.Title != "English title" {
		t.Fatalf("publication metadata was not preserved: %+v", decoded.Items[0].Publications)
	}
	if decoded.Items[0].Publications[0].Localizations["it"].Title != "Titolo italiano" {
		t.Fatalf("localization was not preserved: %+v", decoded.Items[0].Publications[0].Localizations)
	}
	if len(decoded.Items[0].DeliveryPlan) != 1 || decoded.Items[0].DeliveryPlan[0].DestinationID != "legacy-drive" {
		t.Fatalf("delivery_plan was not preserved: %+v", decoded.Items[0].DeliveryPlan)
	}
	if decoded.Items[0].Publications[0].Destinations[0].RetryBudget == nil || *decoded.Items[0].Publications[0].Destinations[0].RetryBudget != 0 {
		t.Fatalf("explicit publication retry_budget zero was not preserved: %+v", decoded.Items[0].Publications[0].Destinations)
	}
}

func TestSubmitJobBatchRequest_EmptyPublicationsRemainOmitted(t *testing.T) {
	encoded, err := json.Marshal(SubmitJobBatchRequest{
		BatchID: "legacy-batch-001",
		Items: []SubmitJobRequest{{
			IdempotencyKey: "legacy-item-001",
			DeliveryPlan:   []SubmitDeliveryPlanEntry{{DestinationID: "drive"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := string(encoded)
	if strings.Contains(wire, `"publications"`) {
		t.Fatalf("legacy batch item unexpectedly emitted publications: %s", wire)
	}
	if !strings.Contains(wire, `"delivery_plan"`) {
		t.Fatalf("legacy delivery_plan was omitted: %s", wire)
	}
}
