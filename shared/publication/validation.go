package publication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

var publicationIDSeparatorPattern = regexp.MustCompile(`[./\\]`)

// Normalize returns a canonical copy of the specification. It trims IDs,
// normalizes language tags to the contract's stable casing, and normalizes
// localization map keys. The receiver and all nested values remain unchanged.
func (s Spec) Normalize() (Spec, error) {
	out := s.Clone()
	if out.Version == 0 {
		out.Version = Version
	}
	out.PublicationID = strings.TrimSpace(out.PublicationID)
	out.Language = NormalizeLanguage(out.Language)
	out.DefaultLanguage = NormalizeLanguage(out.DefaultLanguage)
	out.OutputRef.VariantID = strings.TrimSpace(out.OutputRef.VariantID)
	out.OutputRef.ArtifactRole = strings.TrimSpace(out.OutputRef.ArtifactRole)

	if len(out.Localizations) > 0 {
		normalized := make(map[string]Localization, len(out.Localizations))
		for locale, metadata := range out.Localizations {
			key := NormalizeLanguage(locale)
			if key == "" {
				return Spec{}, ValidationErrors{NewValidationError("localizations", "invalid_locale", "locale key is required")}
			}
			if _, exists := normalized[key]; exists {
				return Spec{}, ValidationErrors{NewValidationError("localizations."+key, "duplicate_locale", "locale is unique after normalization")}
			}
			normalized[key] = metadata
		}
		out.Localizations = normalized
	}
	for i := range out.Destinations {
		out.Destinations[i].DestinationID = strings.TrimSpace(out.Destinations[i].DestinationID)
	}
	if err := out.Validate(); err != nil {
		return Spec{}, err
	}
	return out, nil
}

// Validate checks the structural and encoding invariants shared by all
// publication producers. Platform-specific metadata limits belong to the
// destination adapter and are intentionally not checked here.
func (s Spec) Validate() error {
	var violations ValidationErrors
	if s.Version != Version {
		violations = append(violations, NewValidationError("version", "unsupported_version", fmt.Sprintf("must be %d", Version)))
	}
	if s.PublicationID == "" {
		violations = append(violations, NewValidationError("publication_id", "required", "non-empty identifier"))
	} else if publicationIDSeparatorPattern.MatchString(s.PublicationID) {
		violations = append(violations, NewValidationError("publication_id", "reserved_separator", "must not contain '.', '/', or '\\'"))
	}
	if s.OutputRef.VariantID == "" && s.OutputRef.ArtifactRole == "" {
		violations = append(violations, NewValidationError("output_ref", "selector_required", "exactly one of variant_id or artifact_role"))
	}
	if s.OutputRef.VariantID != "" && s.OutputRef.ArtifactRole != "" {
		violations = append(violations, NewValidationError("output_ref", "selectors_mutually_exclusive", "only one of variant_id or artifact_role"))
	}
	if len(s.Destinations) == 0 {
		violations = append(violations, NewValidationError("destinations", "required", "at least one destination"))
	}
	seenDestinations := make(map[string]struct{}, len(s.Destinations))
	for i, destination := range s.Destinations {
		path := fmt.Sprintf("destinations.%d", i)
		if destination.DestinationID == "" {
			violations = append(violations, NewValidationError(path+".destination_id", "required", "non-empty identifier"))
		} else if _, exists := seenDestinations[destination.DestinationID]; exists {
			violations = append(violations, NewValidationError(path+".destination_id", "duplicate", "unique destination_id per publication"))
		} else {
			seenDestinations[destination.DestinationID] = struct{}{}
		}
		if destination.Priority < 0 {
			violations = append(violations, NewValidationError(path+".priority", "out_of_range", "non-negative integer"))
		}
		if destination.RetryBudget != nil && *destination.RetryBudget < 0 {
			violations = append(violations, NewValidationError(path+".retry_budget", "out_of_range", "non-negative integer"))
		}
		if destination.MetadataOverride != nil {
			violations = append(violations, validateMetadata(*destination.MetadataOverride, path+".metadata_override")...)
		}
	}

	if s.Language != "" {
		if normalized := NormalizeLanguage(s.Language); normalized != s.Language {
			violations = append(violations, NewValidationError("language", "not_normalized", "canonical BCP-47-like casing"))
		}
	}
	if len(s.Localizations) > 0 {
		if s.DefaultLanguage == "" {
			violations = append(violations, NewValidationError("default_language", "required", "required when localizations is non-empty"))
		} else if normalized := NormalizeLanguage(s.DefaultLanguage); normalized != s.DefaultLanguage {
			violations = append(violations, NewValidationError("default_language", "not_normalized", "canonical BCP-47-like casing"))
		}
		for locale, metadata := range s.Localizations {
			path := "localizations." + locale
			if NormalizeLanguage(locale) != locale {
				violations = append(violations, NewValidationError(path, "not_normalized", "canonical locale key"))
			}
			violations = append(violations, validateMetadata(Metadata{
				Title:       metadata.Title,
				Description: metadata.Description,
			}, path)...)
		}
	}
	violations = append(violations, validateMetadata(s.Metadata, "metadata")...)
	if len(violations) > 0 {
		return violations
	}
	return nil
}

func validateMetadata(metadata Metadata, path string) ValidationErrors {
	var violations ValidationErrors
	for field, value := range map[string]string{
		"title":       metadata.Title,
		"description": metadata.Description,
		"category_id": metadata.CategoryID,
		"privacy":     metadata.Privacy,
		"publish_at":  metadata.PublishAt,
	} {
		if !utf8.ValidString(value) {
			violations = append(violations, NewValidationError(path+"."+field, "invalid_utf8", "valid UTF-8 string"))
		}
	}
	for i, tag := range metadata.Tags {
		if !utf8.ValidString(tag) {
			violations = append(violations, NewValidationError(fmt.Sprintf("%s.tags.%d", path, i), "invalid_utf8", "valid UTF-8 string"))
		}
	}
	return violations
}

// NormalizeLanguage returns the canonical casing used by this package. It is
// deliberately dependency-free and handles the common language[-script][-region]
// form used by publication APIs. Empty input remains empty.
func NormalizeLanguage(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "_", "-"))
	if value == "" {
		return ""
	}
	parts := strings.Split(value, "-")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" {
			return ""
		}
		switch {
		case i == 0:
			parts[i] = strings.ToLower(parts[i])
		case len(parts[i]) == 2 && allLetters(parts[i]):
			parts[i] = strings.ToUpper(parts[i])
		case len(parts[i]) == 4 && allLetters(parts[i]):
			parts[i] = strings.ToUpper(parts[i][:1]) + strings.ToLower(parts[i][1:])
		default:
			parts[i] = strings.ToLower(parts[i])
		}
	}
	return strings.Join(parts, "-")
}

func allLetters(value string) bool {
	for _, r := range value {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return value != ""
}

// CanonicalJSON returns the JSON bytes used for idempotency and persistence
// hashing. encoding/json sorts map keys, while struct field order is fixed by
// this contract, so equivalent specs have identical bytes.
func (s Spec) CanonicalJSON() ([]byte, error) {
	canonical, err := s.Normalize()
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("publication canonical json: %w", err)
	}
	return data, nil
}

// SpecHash returns the SHA-256 digest of the canonical normalized spec.
func (s Spec) SpecHash() (string, error) {
	data, err := s.CanonicalJSON()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
