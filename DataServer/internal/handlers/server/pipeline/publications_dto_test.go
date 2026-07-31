package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSubmitJobRequest_PublicationsRoundtrip(t *testing.T) {
	madeForKids := true
	retryBudget := 0
	req := SubmitJobRequest{
		IdempotencyKey: "publication-dto-001",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 5}},
		Publications: []SubmitPublication{{
			PublicationID: "gervais-en",
			OutputRef:     SubmitPublicationOutputRef{VariantID: "en"},
			Language:      "en",
			Metadata:      SubmitPublicationMetadata{Title: "English title", Description: "English description", Tags: []string{"comedy"}, MadeForKids: &madeForKids},
			Localizations: map[string]SubmitLocalizedMetadata{"it": {Title: "Titolo italiano", Description: "Descrizione italiana"}},
			Destinations: []SubmitPublicationDestination{{
				DestinationID: "youtube-en",
				RetryBudget:   &retryBudget,
				MetadataOverride: &SubmitPublicationMetadata{
					Title: "Destination title",
				},
			}},
			ProviderOptions: map[string]any{"category": "comedy"},
		}},
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var back SubmitJobRequest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Publications) != 1 {
		t.Fatalf("publications length = %d, want 1", len(back.Publications))
	}
	publication := back.Publications[0]
	if publication.PublicationID != "gervais-en" || publication.OutputRef.VariantID != "en" || publication.Language != "en" {
		t.Fatalf("publication identity roundtrip: %+v", publication)
	}
	if publication.Metadata.Title != "English title" || publication.Metadata.Description != "English description" || !*publication.Metadata.MadeForKids {
		t.Fatalf("publication metadata roundtrip: %+v", publication.Metadata)
	}
	if publication.Localizations["it"].Title != "Titolo italiano" {
		t.Fatalf("localization roundtrip: %+v", publication.Localizations)
	}
	if len(publication.Destinations) != 1 || publication.Destinations[0].RetryBudget == nil || *publication.Destinations[0].RetryBudget != 0 {
		t.Fatalf("destination retry budget roundtrip: %+v", publication.Destinations)
	}
	if publication.Destinations[0].MetadataOverride == nil || publication.Destinations[0].MetadataOverride.Title != "Destination title" {
		t.Fatalf("destination override roundtrip: %+v", publication.Destinations[0])
	}
}

func TestSubmitJobRequest_PublicationsOmittedForLegacyRequest(t *testing.T) {
	data, err := json.Marshal(SubmitJobRequest{
		IdempotencyKey: "legacy-dto-001",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"publications"`) {
		t.Fatalf("legacy request unexpectedly emitted publications: %s", data)
	}
}
