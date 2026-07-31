package publication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ResolvePublication resolves one publication for one concrete destination.
// The returned value is an immutable-by-convention snapshot suitable for a
// delivery task: it contains the selected metadata, all localizations, the
// effective provider options, and the canonical metadata hash used for
// delivery idempotency.
func ResolvePublication(
	jobVideoName string,
	spec Spec,
	destination Destination,
	requestedLocale string,
) (ResolvedPublication, error) {
	canonical, err := spec.Normalize()
	if err != nil {
		return ResolvedPublication{}, err
	}

	destination = destination.Clone()
	destination.DestinationID = strings.TrimSpace(destination.DestinationID)
	declaredDestination, ok := declaredDestinationByID(canonical, destination.DestinationID)
	if !ok {
		return ResolvedPublication{}, NewValidationError(
			"destinations",
			"destination_not_found",
			fmt.Sprintf("destination_id %q is not part of publication", destination.DestinationID),
		)
	}
	// Resolve only the destination declared by the canonical spec. This prevents
	// a caller from injecting a different metadata override or provider option
	// while reusing an existing destination_id.
	destination = declaredDestination
	if err := validateResolutionDestination(destination); err != nil {
		return ResolvedPublication{}, err
	}

	metadata, err := resolveCanonicalMetadata(jobVideoName, canonical, destination, requestedLocale)
	if err != nil {
		return ResolvedPublication{}, err
	}

	providerOptions := mergeProviderOptions(canonical.ProviderOptions, destination.ProviderOptions)
	resolved := ResolvedPublication{
		PublicationID:   canonical.PublicationID,
		DestinationID:   destination.DestinationID,
		OutputRef:       canonical.OutputRef,
		Metadata:        metadata,
		Localizations:   cloneLocalizations(canonical.Localizations),
		ProviderOptions: providerOptions,
	}
	metadataHash, err := resolved.MetadataHashValue()
	if err != nil {
		return ResolvedPublication{}, err
	}
	resolved.MetadataHash = metadataHash
	return resolved, nil
}

// ResolvePublicationForDestinationID resolves a destination by ID from the
// canonical spec. It is the convenience entry point used by task compilers
// that persist only a destination identifier between stages.
func ResolvePublicationForDestinationID(
	jobVideoName string,
	spec Spec,
	destinationID string,
	requestedLocale string,
) (ResolvedPublication, error) {
	canonical, err := spec.Normalize()
	if err != nil {
		return ResolvedPublication{}, err
	}
	wanted := strings.TrimSpace(destinationID)
	for _, destination := range canonical.Destinations {
		if destination.DestinationID == wanted {
			return ResolvePublication(jobVideoName, canonical, destination, requestedLocale)
		}
	}
	return ResolvedPublication{}, NewValidationError(
		"destinations",
		"destination_not_found",
		fmt.Sprintf("destination_id %q is not part of publication", wanted),
	)
}

// MetadataHashValue returns the canonical SHA-256 hash used for delivery
// idempotency. Its representation deliberately follows the publication
// contract: title, description, tags, language, localizations, and effective
// provider options. Equivalent maps produce identical JSON because the Go
// JSON encoder orders string map keys deterministically.
func (p ResolvedPublication) MetadataHashValue() (string, error) {
	envelope := metadataHashEnvelope{
		Title:           p.Metadata.Title,
		Description:     p.Metadata.Description,
		Tags:            append([]string(nil), p.Metadata.Tags...),
		Language:        p.Metadata.Language,
		Localizations:   cloneLocalizations(p.Localizations),
		ProviderOptions: cloneMap(p.ProviderOptions),
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("publication metadata hash: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type metadataHashEnvelope struct {
	Title           string                  `json:"title,omitempty"`
	Description     string                  `json:"description,omitempty"`
	Tags            []string                `json:"tags,omitempty"`
	Language        string                  `json:"language,omitempty"`
	Localizations   map[string]Localization `json:"localizations,omitempty"`
	ProviderOptions map[string]any          `json:"provider_options,omitempty"`
}

// ResolveMetadata is intentionally kept as the legacy low-level helper. It
// requires the destination ID to be declared by the spec, but preserves its
// historical behavior of applying the caller-provided override. New delivery
// code should use ResolvePublication, which takes the destination snapshot
// exclusively from the canonical spec.
func effectiveLocale(spec Spec, requestedLocale string) string {
	locale := NormalizeLanguage(requestedLocale)
	if locale == "" {
		locale = spec.Language
	}
	if locale == "" && len(spec.Localizations) > 0 {
		locale = spec.DefaultLanguage
	}
	return locale
}

func declaredDestinationByID(spec Spec, destinationID string) (Destination, bool) {
	for _, destination := range spec.Destinations {
		if strings.TrimSpace(destination.DestinationID) == destinationID {
			destination.DestinationID = destinationID
			return destination, true
		}
	}
	return Destination{}, false
}

func validateResolutionDestination(destination Destination) error {
	if destination.DestinationID == "" {
		return NewValidationError("destination_id", "required", "non-empty identifier")
	}
	if publicationIDSeparatorPattern.MatchString(destination.DestinationID) {
		return NewValidationError("destination_id", "reserved_separator", "must not contain '.', '/', or '\\'")
	}
	if destination.Priority < 0 {
		return NewValidationError("priority", "out_of_range", "non-negative integer")
	}
	if destination.RetryBudget != nil && *destination.RetryBudget < 0 {
		return NewValidationError("retry_budget", "out_of_range", "non-negative integer")
	}
	if destination.MetadataOverride != nil {
		if violations := validateMetadata(*destination.MetadataOverride, "metadata_override"); len(violations) > 0 {
			return violations
		}
	}
	return nil
}

func mergeProviderOptions(base, override map[string]any) map[string]any {
	if base == nil && override == nil {
		return nil
	}
	merged := cloneMap(base)
	if merged == nil {
		merged = make(map[string]any, len(override))
	}
	for key, value := range override {
		merged[key] = cloneValue(value)
	}
	return merged
}
