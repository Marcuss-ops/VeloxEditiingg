package publication

import (
	"errors"
	"testing"
)

func TestResolvePublicationAppliesCanonicalPrecedence(t *testing.T) {
	titleOverride := "Destination title"
	spec := Spec{
		PublicationID:   "publication-1",
		OutputRef:       OutputRef{ArtifactRole: "final_video"},
		Language:        "en",
		DefaultLanguage: "en",
		Metadata: Metadata{
			Title:       "Primary title",
			Description: "Primary description",
			Tags:        []string{"main"},
			Privacy:     "private",
		},
		Localizations: map[string]Localization{
			"it": {Title: "Titolo italiano", Description: "Descrizione italiana"},
		},
		ProviderOptions: map[string]any{"shared": true, "mode": "default"},
		Destinations: []Destination{{
			DestinationID:    "youtube-it",
			ProviderOptions:  map[string]any{"mode": "localized"},
			MetadataOverride: &Metadata{Title: titleOverride},
		}},
	}

	resolved, err := ResolvePublication("Job fallback", spec, spec.Destinations[0], "it")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.PublicationID != "publication-1" || resolved.DestinationID != "youtube-it" {
		t.Fatalf("identity = %+v", resolved)
	}
	if resolved.Metadata.Title != titleOverride {
		t.Fatalf("title = %q, want destination override", resolved.Metadata.Title)
	}
	if resolved.Metadata.Description != "Descrizione italiana" {
		t.Fatalf("description = %q, want localization", resolved.Metadata.Description)
	}
	if resolved.Metadata.Language != "it" {
		t.Fatalf("language = %q, want it", resolved.Metadata.Language)
	}
	if resolved.ProviderOptions["shared"] != true || resolved.ProviderOptions["mode"] != "localized" {
		t.Fatalf("provider options = %#v", resolved.ProviderOptions)
	}
	if resolved.MetadataHash == "" {
		t.Fatal("metadata hash is empty")
	}
}

func TestResolvePublicationUsesVideoNameFallback(t *testing.T) {
	spec := Spec{
		PublicationID: "publication-1",
		OutputRef:     OutputRef{VariantID: "en"},
		Destinations:  []Destination{{DestinationID: "drive"}},
	}
	resolved, err := ResolvePublication("Fallback title", spec, spec.Destinations[0], "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Metadata.Title != "Fallback title" {
		t.Fatalf("title = %q, want fallback title", resolved.Metadata.Title)
	}

	spec.Metadata.Title = ""
	_, err = ResolvePublication("", spec, spec.Destinations[0], "")
	if !errors.Is(err, ErrMissingTitle) {
		t.Fatalf("error = %v, want ErrMissingTitle", err)
	}
}

func TestResolvePublicationRejectsInjectedDestinationOverrides(t *testing.T) {
	spec := validSpec()
	declared := spec.Destinations[0]
	injected := declared.Clone()
	injected.MetadataOverride = &Metadata{Title: "Injected title"}
	resolved, err := ResolvePublication("Job title", spec, injected, "en")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Metadata.Title == "Injected title" {
		t.Fatal("resolver accepted metadata from an untrusted destination copy")
	}
}

func TestResolvePublicationForDestinationIDRejectsUnknownDestination(t *testing.T) {
	spec := validSpec()
	_, err := ResolvePublicationForDestinationID("job", spec, "missing", "en")
	var validationError *ValidationError
	if !errors.As(err, &validationError) || validationError.Issue() != "destination_not_found" {
		t.Fatalf("error = %T %v, want destination_not_found", err, err)
	}
}

func TestSpecValidateRejectsWhitespaceAndReservedOutputSelectors(t *testing.T) {
	spec := validSpec()
	spec.PublicationID = "  "
	spec.OutputRef = OutputRef{VariantID: "final/video"}
	spec.Destinations = []Destination{{DestinationID: "destination"}}
	err := spec.Validate()
	var violations ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("Validate() error = %T %v, want ValidationErrors", err, err)
	}
	if !hasValidationPath(violations, "publication_id") || !hasValidationPath(violations, "output_ref.variant_id") {
		t.Fatalf("validation paths = %+v", violations)
	}
}

func TestResolvedPublicationMetadataHashChangesWithProviderOptionsAndLocalizations(t *testing.T) {
	base := ResolvedPublication{
		PublicationID: "publication-1",
		DestinationID: "youtube",
		Metadata:      ResolvedMetadata{Title: "Title", Language: "en"},
		Localizations: map[string]Localization{"it": {Title: "Titolo"}},
		ProviderOptions: map[string]any{
			"privacy_mode": "strict",
		},
	}
	first, err := base.MetadataHashValue()
	if err != nil {
		t.Fatal(err)
	}
	base.ProviderOptions["privacy_mode"] = "relaxed"
	second, err := base.MetadataHashValue()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("provider option change did not change metadata hash")
	}
	base.ProviderOptions["privacy_mode"] = "strict"
	base.Localizations["it"] = Localization{Title: "Titolo aggiornato"}
	third, err := base.MetadataHashValue()
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("localization change did not change metadata hash")
	}
}
