package publication

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func validSpec() Spec {
	return Spec{
		PublicationID: "gervais-en",
		OutputRef:     OutputRef{VariantID: "en"},
		Language:      "en",
		Metadata:      Metadata{Title: "Main title", Description: "Main description", Tags: []string{"comedy"}},
		Destinations:  []Destination{{DestinationID: "youtube-en", RetryBudget: intPtr(3)}},
	}
}

func TestSpecJSONUsesCanonicalShape(t *testing.T) {
	data, err := json.Marshal(validSpec())
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, key := range []string{"publication_id", "output_ref", "variant_id", "destinations", "destination_id", "retry_budget"} {
		if !strings.Contains(text, `"`+key+`"`) {
			t.Errorf("JSON missing key %q: %s", key, text)
		}
	}
	for _, forbidden := range []string{"video_name", "scenes", "script_text", "render_plan"} {
		if strings.Contains(text, `"`+forbidden+`"`) {
			t.Errorf("publication JSON must not contain renderer field %q: %s", forbidden, text)
		}
	}
}

func TestSpecValidateAggregatesContractErrors(t *testing.T) {
	spec := Spec{
		PublicationID: "bad/id",
		OutputRef:     OutputRef{VariantID: "en", ArtifactRole: "final_video"},
		Destinations: []Destination{
			{DestinationID: "same", Priority: -1},
			{DestinationID: "same", RetryBudget: intPtr(-2)},
		},
		Localizations: map[string]Localization{"it_IT": {Title: "Italian"}},
	}
	err := spec.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("Validate() error type = %T, want ValidationErrors: %v", err, err)
	}
	for _, path := range []string{"publication_id", "output_ref", "destinations.0.priority", "destinations.1.destination_id", "destinations.1.retry_budget", "default_language", "localizations.it_IT"} {
		if !hasValidationPath(violations, path) {
			t.Errorf("missing validation path %q in %+v", path, violations)
		}
	}
	if !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("validation error should preserve ErrInvalidSpec: %v", err)
	}
}

func TestNormalizeDoesNotMutateInput(t *testing.T) {
	spec := validSpec()
	spec.Language = "pt_br"
	spec.DefaultLanguage = "pt_br"
	spec.Localizations = map[string]Localization{"it_IT": {Title: "Italian"}}
	before := spec.Clone()
	got, err := spec.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec, before) {
		t.Fatal("Normalize mutated its receiver")
	}
	if got.Language != "pt-BR" || got.Localizations["it-IT"].Title != "Italian" {
		t.Fatalf("normalization mismatch: %+v", got)
	}
}

func TestResolveMetadataPrecedenceAndCopy(t *testing.T) {
	overrideTitle := "Destination title"
	spec := validSpec()
	spec.DefaultLanguage = "en"
	spec.Localizations = map[string]Localization{"it": {Title: "Italian title", Description: "Italian description"}}
	destination := Destination{DestinationID: "youtube-en", MetadataOverride: &Metadata{Title: overrideTitle}}
	resolved, err := ResolveMetadata("Job fallback", spec, destination, "it")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Title != overrideTitle || resolved.Description != "Italian description" || resolved.Language != "it" {
		t.Fatalf("unexpected resolved metadata: %+v", resolved)
	}
	resolved.Tags[0] = "changed"
	if spec.Metadata.Tags[0] != "comedy" {
		t.Fatal("resolved metadata shares tags backing array with spec")
	}
}

func TestResolveMetadataFallsBackToJobName(t *testing.T) {
	spec := validSpec()
	spec.Metadata = Metadata{}
	resolved, err := ResolveMetadata("Video name", spec, spec.Destinations[0], "")
	if err != nil || resolved.Title != "Video name" {
		t.Fatalf("fallback resolution = %+v, %v", resolved, err)
	}
	_, err = ResolveMetadata("", spec, spec.Destinations[0], "")
	if !errors.Is(err, ErrMissingTitle) {
		t.Fatalf("missing title error = %v, want ErrMissingTitle", err)
	}
}

func TestSpecHashCanonicalizesEquivalentLanguagesAndMaps(t *testing.T) {
	left := validSpec()
	left.Language = "pt_br"
	left.DefaultLanguage = "en"
	left.Localizations = map[string]Localization{"it_IT": {Title: "Italian"}}
	right := validSpec()
	right.Language = "pt-BR"
	right.DefaultLanguage = "en"
	right.Localizations = map[string]Localization{"it-IT": {Title: "Italian"}}
	leftHash, err := left.SpecHash()
	if err != nil {
		t.Fatal(err)
	}
	rightHash, err := right.SpecHash()
	if err != nil {
		t.Fatal(err)
	}
	if leftHash != rightHash {
		t.Fatalf("equivalent specs produced different hashes: %s != %s", leftHash, rightHash)
	}
}

func TestValidationErrorWrapsInvalidSpec(t *testing.T) {
	err := NewValidationError("output_ref", "selector_required", "exactly one selector")
	if !errors.Is(err, ErrInvalidSpec) || err.Field() != "output_ref" || err.Issue() != "selector_required" {
		t.Fatalf("unexpected typed error: %v", err)
	}
}

func hasValidationPath(errors ValidationErrors, path string) bool {
	for _, violation := range errors {
		if violation.Path == path {
			return true
		}
	}
	return false
}

func intPtr(value int) *int { return &value }

func TestCloneIsDeepEnoughForContractCollections(t *testing.T) {
	spec := validSpec()
	spec.ProviderOptions = map[string]any{"nested": map[string]any{"key": "value"}}
	clone := spec.Clone()
	clone.Metadata.Tags[0] = "other"
	clone.Destinations[0].RetryBudget = intPtr(9)
	clone.ProviderOptions["nested"] = "changed"
	if spec.Metadata.Tags[0] == clone.Metadata.Tags[0] || *spec.Destinations[0].RetryBudget == *clone.Destinations[0].RetryBudget || reflect.DeepEqual(spec.ProviderOptions, clone.ProviderOptions) {
		t.Fatal("Clone did not isolate contract collections")
	}
}
