package apiwire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSubmitJobRequest_PublicationsRoundtrip(t *testing.T) {
	retryBudget := 0
	req := SubmitJobRequest{
		IdempotencyKey: "publication-apiwire-001",
		Publications: []SubmitPublication{{
			PublicationID:   "gervais-main",
			OutputRef:       SubmitPublicationOutputRef{ArtifactRole: "final_video"},
			DefaultLanguage: "en",
			Metadata:        SubmitPublicationMetadata{Title: "Main title", Description: "Main description"},
			Localizations:   map[string]SubmitLocalizedMetadata{"it": {Title: "Titolo", Description: "Descrizione"}},
			Destinations: []SubmitPublicationDestination{{
				DestinationID: "youtube-main",
				RetryBudget:   &retryBudget,
			}},
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
	if publication.PublicationID != "gervais-main" || publication.OutputRef.ArtifactRole != "final_video" {
		t.Fatalf("publication identity roundtrip: %+v", publication)
	}
	if publication.DefaultLanguage != "en" || publication.Localizations["it"].Title != "Titolo" {
		t.Fatalf("localized metadata roundtrip: %+v", publication)
	}
	if len(publication.Destinations) != 1 || publication.Destinations[0].RetryBudget == nil || *publication.Destinations[0].RetryBudget != 0 {
		t.Fatalf("retry budget roundtrip: %+v", publication.Destinations)
	}
}

func TestSubmitPublication_JSONTagsMatchRuntimeContract(t *testing.T) {
	typeOfPublication := reflect.TypeOf(SubmitPublication{})
	want := map[string]bool{
		"publication_id":   true,
		"output_ref":       true,
		"language":         true,
		"default_language": true,
		"metadata":         true,
		"localizations":    true,
		"destinations":     true,
		"provider_options": true,
	}
	for i := 0; i < typeOfPublication.NumField(); i++ {
		field := typeOfPublication.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if !want[name] {
			t.Errorf("unexpected SubmitPublication JSON field %q", name)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("missing SubmitPublication JSON field %q", name)
	}
}

func TestSubmitJobRequest_PublicationsOmittedForLegacyRequest(t *testing.T) {
	data, err := json.Marshal(SubmitJobRequest{IdempotencyKey: "legacy-apiwire-001"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"publications"`) {
		t.Fatalf("legacy request unexpectedly emitted publications: %s", data)
	}
}
