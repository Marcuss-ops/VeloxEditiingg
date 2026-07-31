package pipeline

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func validSubmitPublication() SubmitPublication {
	zero := 0
	return SubmitPublication{
		PublicationID: "publication-en",
		OutputRef:     SubmitPublicationOutputRef{VariantID: "en"},
		Language:      "en",
		Metadata:      SubmitPublicationMetadata{Title: "English title", Description: "English description", Tags: []string{"comedy"}},
		Destinations:  []SubmitPublicationDestination{{DestinationID: "youtube-en", RetryBudget: &zero}},
	}
}

func TestValidateSubmitPublications_ValidSpec(t *testing.T) {
	publication := validSubmitPublication()
	if details := validateSubmitPublications([]SubmitPublication{publication}); len(details) != 0 {
		t.Fatalf("valid publication rejected: %#v", details)
	}
}

func TestValidateSubmitPublications_RejectsDuplicateIDs(t *testing.T) {
	first := validSubmitPublication()
	second := validSubmitPublication()
	second.OutputRef = SubmitPublicationOutputRef{VariantID: "it"}
	details := validateSubmitPublications([]SubmitPublication{first, second})
	assertPublicationIssue(t, details, "publications.1.publication_id", "duplicate")
}

func TestValidateSubmitPublications_RejectsWhitespaceOutputSelectors(t *testing.T) {
	input := validSubmitPublication()
	input.OutputRef = SubmitPublicationOutputRef{VariantID: " ", ArtifactRole: "final_video"}

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.output_ref.variant_id", "required")
}

func TestValidateSubmitPublications_RejectsOutputRefShapes(t *testing.T) {
	tests := []struct {
		name  string
		ref   SubmitPublicationOutputRef
		issue string
	}{
		{name: "missing", ref: SubmitPublicationOutputRef{}, issue: "selector_required"},
		{name: "both", ref: SubmitPublicationOutputRef{VariantID: "en", ArtifactRole: "final_video"}, issue: "selectors_mutually_exclusive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validSubmitPublication()
			input.OutputRef = test.ref
			details := validateSubmitPublications([]SubmitPublication{input})
			assertPublicationIssue(t, details, "publications.0.output_ref", test.issue)
		})
	}
}

func TestValidateSubmitPublications_RejectsWhitespaceIdentifiersAndTrimmedDuplicates(t *testing.T) {
	input := validSubmitPublication()
	input.PublicationID = "   "
	input.OutputRef = SubmitPublicationOutputRef{VariantID: "  "}
	input.Destinations = []SubmitPublicationDestination{{DestinationID: " youtube "}, {DestinationID: "youtube"}}

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.publication_id", "required")
	assertPublicationIssue(t, details, "publications.0.output_ref", "selector_required")
	assertPublicationIssue(t, details, "publications.0.destinations.1.destination_id", "duplicate")
}

func TestValidateSubmitPublications_RejectsDestinations(t *testing.T) {
	input := validSubmitPublication()
	input.Destinations = []SubmitPublicationDestination{
		{DestinationID: "duplicate"},
		{DestinationID: "duplicate", Priority: -1},
	}
	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.destinations.1.destination_id", "duplicate")
	assertPublicationIssue(t, details, "publications.0.destinations.1.priority", "out_of_range")

	input.Destinations = nil
	details = validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.destinations", "required")
}

func TestValidateSubmitPublications_RejectsEmptyLocalizationKey(t *testing.T) {
	input := validSubmitPublication()
	input.Localizations = map[string]SubmitLocalizedMetadata{"": {Title: "Missing locale"}}

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.localizations.", "required")
}

func TestValidateSubmitPublications_RejectsNonCanonicalLanguages(t *testing.T) {
	input := validSubmitPublication()
	input.Language = "EN"
	input.DefaultLanguage = "it_IT"
	input.Localizations = map[string]SubmitLocalizedMetadata{"pt_BR": {Title: "Portuguese"}}

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.language", "not_normalized")
	assertPublicationIssue(t, details, "publications.0.default_language", "not_normalized")
	assertPublicationIssue(t, details, "publications.0.localizations.pt_BR", "not_normalized")
}

func TestValidateSubmitPublications_RejectsLanguagesAndLocaleCollisions(t *testing.T) {
	input := validSubmitPublication()
	input.Language = "en--US"
	input.DefaultLanguage = "it"
	input.Localizations = map[string]SubmitLocalizedMetadata{
		"it_IT": {Title: "Italian"},
		"it-IT": {Title: "Duplicate Italian"},
	}
	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.language", "invalid_language")
	assertPublicationIssue(t, details, "publications.0.localizations.it-IT", "duplicate_locale")

	input.Localizations = map[string]SubmitLocalizedMetadata{"it": {Title: "Italian"}}
	input.DefaultLanguage = ""
	details = validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.default_language", "required")
}

func TestValidateSubmitPublications_RejectsReservedIdentifiers(t *testing.T) {
	input := validSubmitPublication()
	input.PublicationID = "publication/en"
	input.OutputRef = SubmitPublicationOutputRef{ArtifactRole: "final/video"}
	input.Destinations[0].DestinationID = "youtube.en"
	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.publication_id", "reserved_separator")
	assertPublicationIssue(t, details, "publications.0.output_ref.artifact_role", "reserved_separator")
	assertPublicationIssue(t, details, "publications.0.destinations.0.destination_id", "reserved_separator")
}

func TestValidateSubmitPublications_RejectsInvalidUTF8Everywhere(t *testing.T) {
	input := validSubmitPublication()
	invalid := string([]byte{0xff, 0xfe})
	input.Metadata.Title = invalid
	input.Metadata.Tags = []string{invalid}
	input.Localizations = map[string]SubmitLocalizedMetadata{"it": {Description: invalid}}
	input.Destinations[0].MetadataOverride = &SubmitPublicationMetadata{Description: invalid}
	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.metadata.title", "invalid_utf8")
	assertPublicationIssue(t, details, "publications.0.metadata.tags.0", "invalid_utf8")
	assertPublicationIssue(t, details, "publications.0.localizations.it.description", "invalid_utf8")
	assertPublicationIssue(t, details, "publications.0.destinations.0.metadata_override.description", "invalid_utf8")
}

func TestValidateSubmitPublications_RejectsCyclicProviderOptions(t *testing.T) {
	input := validSubmitPublication()
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	input.ProviderOptions = cyclic

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.provider_options.self", "invalid_json_payload")
}

func TestValidateSubmitPublications_RejectsCyclicProviderOptionSlice(t *testing.T) {
	input := validSubmitPublication()
	cyclic := make([]any, 1)
	cyclic[0] = cyclic
	input.ProviderOptions = map[string]any{"items": cyclic}

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.provider_options.items.0", "invalid_json_payload")
}

func TestValidateSubmitPublications_AllowsSharedProviderOptionReferences(t *testing.T) {
	input := validSubmitPublication()
	shared := map[string]any{"value": "safe"}
	input.ProviderOptions = map[string]any{"first": shared, "second": shared}

	if details := validateSubmitPublications([]SubmitPublication{input}); len(details) != 0 {
		t.Fatalf("shared provider option value rejected: %#v", details)
	}
}

func TestValidateSubmitPublications_AggregatesProviderAndStructuralErrors(t *testing.T) {
	input := validSubmitPublication()
	input.OutputRef = SubmitPublicationOutputRef{}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	input.ProviderOptions = cyclic

	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.provider_options.self", "invalid_json_payload")
	assertPublicationIssue(t, details, "publications.0.output_ref", "selector_required")
}

func TestValidateSubmitPublications_RejectsInvalidProviderOptions(t *testing.T) {
	input := validSubmitPublication()
	input.OutputRef = SubmitPublicationOutputRef{}
	input.ProviderOptions = map[string]any{"channel": make(chan int)}
	input.Destinations[0].ProviderOptions = map[string]any{"nested": map[string]any{"bad": func() {}}}
	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.provider_options", "invalid_json_payload")
	assertPublicationIssue(t, details, "publications.0.destinations.0.provider_options", "invalid_json_payload")
	assertPublicationIssue(t, details, "publications.0.output_ref", "selector_required")
}

func TestValidateSubmitPublications_RejectsInvalidProviderOptionUTF8(t *testing.T) {
	input := validSubmitPublication()
	input.ProviderOptions = map[string]any{"title": string([]byte{0xff})}
	details := validateSubmitPublications([]SubmitPublication{input})
	assertPublicationIssue(t, details, "publications.0.provider_options.title", "invalid_utf8")
}

func TestValidateSubmitJobRequest_ValidatesPublications(t *testing.T) {
	input := validSubmitPublication()
	input.OutputRef = SubmitPublicationOutputRef{}
	req := SubmitJobRequest{
		VideoName:    "publication integration",
		Scenes:       []SubmitScene{{Text: "scene", DurationSeconds: 1}},
		Publications: []SubmitPublication{input},
	}

	validationErr, bad := ValidateSubmitJobRequest(req)
	if !bad || validationErr == nil {
		t.Fatalf("expected integrated publication validation failure, got err=%v bad=%v", validationErr, bad)
	}
	assertPublicationIssue(t, validationErr.Details, "publications.0.output_ref", "selector_required")
}

func TestValidateSubmitPublications_DetailsAreDeterministicallyOrdered(t *testing.T) {
	input := validSubmitPublication()
	input.OutputRef = SubmitPublicationOutputRef{}
	input.Localizations = map[string]SubmitLocalizedMetadata{
		"pt": {Title: string([]byte{0xff})},
		"it": {Description: string([]byte{0xfe})},
	}

	first := validateSubmitPublications([]SubmitPublication{input})
	second := validateSubmitPublications([]SubmitPublication{input})
	if len(first) != len(second) {
		t.Fatalf("detail count changed: first=%d second=%d", len(first), len(second))
	}
	for index := range first {
		firstJSON, firstErr := json.Marshal(first[index])
		secondJSON, secondErr := json.Marshal(second[index])
		if firstErr != nil || secondErr != nil || string(firstJSON) != string(secondJSON) {
			t.Fatalf("detail order/content changed at %d: first=%#v second=%#v", index, first[index], second[index])
		}
	}
}

func TestValidateSubmitPublications_AllowsJSONProviderOptions(t *testing.T) {
	input := validSubmitPublication()
	input.ProviderOptions = map[string]any{
		"channel": "youtube",
		"flags":   []any{"localized", true, float64(3)},
		"nested":  map[string]any{"description": "safe"},
	}
	input.Destinations[0].ProviderOptions = map[string]any{"privacy": "private"}
	if details := validateSubmitPublications([]SubmitPublication{input}); len(details) != 0 {
		t.Fatalf("valid provider options rejected: %#v", details)
	}
}

func assertPublicationIssue(t *testing.T, details []gin.H, path, issue string) {
	t.Helper()
	for _, detail := range details {
		if detail["path"] == path && detail["issue"] == issue {
			return
		}
	}
	var paths []string
	for _, detail := range details {
		paths = append(paths, detail["path"].(string)+":"+detail["issue"].(string))
	}
	t.Fatalf("missing validation detail %s:%s; got %s", path, issue, strings.Join(paths, ", "))
}
