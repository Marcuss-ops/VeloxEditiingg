package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"velox-shared/publication"

	"github.com/gin-gonic/gin"
)

var publicationLanguagePattern = regexp.MustCompile(`^[a-z]{2,3}(?:-[A-Z][a-z]{3})?(?:-(?:[A-Z]{2}|[0-9]{3}))?(?:-[a-z0-9]+)*$`)
var publicationReservedIdentifierPattern = regexp.MustCompile(`[./\\]`)

// validateSubmitPublications validates the HTTP representation of the
// canonical publication contract. Single-publication invariants are delegated
// to velox-shared/publication; this function adds request-level uniqueness and
// JSON-boundary checks that the shared domain cannot own.
func validateSubmitPublications(publications []SubmitPublication) []gin.H {
	if len(publications) == 0 {
		return nil
	}

	details := make([]gin.H, 0)
	seenPublicationIDs := make(map[string]int, len(publications))
	for index, input := range publications {
		prefix := fmt.Sprintf("publications.%d", index)
		publicationID := strings.TrimSpace(input.PublicationID)
		if publicationID != "" {
			if previous, exists := seenPublicationIDs[publicationID]; exists {
				details = append(details, publicationDetail(prefix+".publication_id", "duplicate", map[string]any{
					"observed":     publicationID,
					"duplicate_of": fmt.Sprintf("publications.%d.publication_id", previous),
				}))
			} else {
				seenPublicationIDs[publicationID] = index
			}
		}

		details = append(details, validatePublicationStrings(input, prefix)...)
		details = append(details, validateProviderOptions(input.ProviderOptions, prefix+".provider_options")...)
		for destinationIndex, destination := range input.Destinations {
			destinationPrefix := fmt.Sprintf("%s.destinations.%d", prefix, destinationIndex)
			details = append(details, validateProviderOptions(destination.ProviderOptions, destinationPrefix+".provider_options")...)
		}
		details = append(details, validatePublicationLanguageFields(input, prefix)...)

		// Spec.Validate checks structural publication invariants and does not
		// clone or traverse provider_options. Run it even when provider options
		// also contain errors so the response aggregates every independent issue.
		spec := submitPublicationToSharedSpec(input)
		if err := spec.Validate(); err != nil {
			details = append(details, mapSharedPublicationErrors(err, prefix)...)
		}
	}
	sort.SliceStable(details, func(i, j int) bool {
		leftPath, _ := details[i]["path"].(string)
		rightPath, _ := details[j]["path"].(string)
		if leftPath != rightPath {
			return leftPath < rightPath
		}
		leftIssue, _ := details[i]["issue"].(string)
		rightIssue, _ := details[j]["issue"].(string)
		if leftIssue != rightIssue {
			return leftIssue < rightIssue
		}
		return publicationDetailSortKey(details[i]) < publicationDetailSortKey(details[j])
	})
	return details
}

func publicationDetailSortKey(detail gin.H) string {
	encoded, err := json.Marshal(detail)
	if err != nil {
		return fmt.Sprintf("%#v", detail)
	}
	return string(encoded)
}

func submitPublicationToSharedSpec(input SubmitPublication) publication.Spec {
	destinations := make([]publication.Destination, len(input.Destinations))
	for index, destination := range input.Destinations {
		var metadataOverride *publication.Metadata
		if destination.MetadataOverride != nil {
			metadata := submitMetadataToShared(destination.MetadataOverride)
			metadataOverride = &metadata
		}
		destinations[index] = publication.Destination{
			DestinationID:    strings.TrimSpace(destination.DestinationID),
			CredentialRef:    strings.TrimSpace(destination.CredentialRef),
			Priority:         destination.Priority,
			RetryBudget:      cloneIntPointer(destination.RetryBudget),
			MetadataOverride: metadataOverride,
			ProviderOptions:  destination.ProviderOptions,
		}
	}

	localizations := make(map[string]publication.Localization, len(input.Localizations))
	for locale, metadata := range input.Localizations {
		localizations[locale] = publication.Localization{
			Title:       metadata.Title,
			Description: metadata.Description,
		}
	}

	return publication.Spec{
		Version:         publication.Version,
		PublicationID:   strings.TrimSpace(input.PublicationID),
		OutputRef:       publication.OutputRef{VariantID: strings.TrimSpace(input.OutputRef.VariantID), ArtifactRole: strings.TrimSpace(input.OutputRef.ArtifactRole)},
		Language:        input.Language,
		DefaultLanguage: input.DefaultLanguage,
		Metadata:        submitMetadataToShared(&input.Metadata),
		Localizations:   localizations,
		Destinations:    destinations,
		ProviderOptions: input.ProviderOptions,
	}
}

func submitMetadataToShared(input *SubmitPublicationMetadata) publication.Metadata {
	if input == nil {
		return publication.Metadata{}
	}
	return publication.Metadata{
		Title:                  input.Title,
		Description:            input.Description,
		Tags:                   append([]string(nil), input.Tags...),
		CategoryID:             input.CategoryID,
		Privacy:                input.Privacy,
		PublishAt:              input.PublishAt,
		MadeForKids:            cloneBoolPointer(input.MadeForKids),
		ContainsSyntheticMedia: cloneBoolPointer(input.ContainsSyntheticMedia),
	}
}

func cloneIntPointer(input *int) *int {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func cloneBoolPointer(input *bool) *bool {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func validatePublicationStrings(input SubmitPublication, prefix string) []gin.H {
	var details []gin.H
	fields := []struct {
		path  string
		value string
	}{
		{prefix + ".publication_id", input.PublicationID},
		{prefix + ".output_ref.variant_id", input.OutputRef.VariantID},
		{prefix + ".output_ref.artifact_role", input.OutputRef.ArtifactRole},
		{prefix + ".language", input.Language},
		{prefix + ".default_language", input.DefaultLanguage},
		{prefix + ".metadata.title", input.Metadata.Title},
		{prefix + ".metadata.description", input.Metadata.Description},
		{prefix + ".metadata.category_id", input.Metadata.CategoryID},
		{prefix + ".metadata.privacy", input.Metadata.Privacy},
		{prefix + ".metadata.publish_at", input.Metadata.PublishAt},
	}
	for _, field := range fields {
		path, value := field.path, field.value
		if !utf8.ValidString(value) {
			details = append(details, publicationDetail(path, "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
		}
	}
	for index, tag := range input.Metadata.Tags {
		if !utf8.ValidString(tag) {
			details = append(details, publicationDetail(fmt.Sprintf("%s.metadata.tags.%d", prefix, index), "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
		}
	}
	if input.PublicationID != "" && publicationReservedIdentifierPattern.MatchString(strings.TrimSpace(input.PublicationID)) {
		details = append(details, publicationDetail(prefix+".publication_id", "reserved_separator", map[string]any{
			"expected": "identifier without '.', '/', or '\\'",
		}))
	}
	if input.OutputRef.VariantID != "" && strings.TrimSpace(input.OutputRef.VariantID) == "" {
		details = append(details, publicationDetail(prefix+".output_ref.variant_id", "required", map[string]any{
			"expected": "non-empty identifier",
		}))
	}
	if input.OutputRef.ArtifactRole != "" && strings.TrimSpace(input.OutputRef.ArtifactRole) == "" {
		details = append(details, publicationDetail(prefix+".output_ref.artifact_role", "required", map[string]any{
			"expected": "non-empty identifier",
		}))
	}
	if input.OutputRef.VariantID != "" && publicationReservedIdentifierPattern.MatchString(strings.TrimSpace(input.OutputRef.VariantID)) {
		details = append(details, publicationDetail(prefix+".output_ref.variant_id", "reserved_separator", map[string]any{
			"expected": "identifier without '.', '/', or '\\'",
		}))
	}
	if input.OutputRef.ArtifactRole != "" && publicationReservedIdentifierPattern.MatchString(strings.TrimSpace(input.OutputRef.ArtifactRole)) {
		details = append(details, publicationDetail(prefix+".output_ref.artifact_role", "reserved_separator", map[string]any{
			"expected": "identifier without '.', '/', or '\\'",
		}))
	}
	for index, destination := range input.Destinations {
		path := fmt.Sprintf("%s.destinations.%d.destination_id", prefix, index)
		if !utf8.ValidString(destination.DestinationID) {
			details = append(details, publicationDetail(path, "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
		}
		if destination.DestinationID != "" && publicationReservedIdentifierPattern.MatchString(strings.TrimSpace(destination.DestinationID)) {
			details = append(details, publicationDetail(path, "reserved_separator", map[string]any{
				"expected": "identifier without '.', '/', or '\\'",
			}))
		}
		if destination.MetadataOverride != nil {
			metadataFields := []struct {
				path  string
				value string
			}{
				{fmt.Sprintf("%s.destinations.%d.metadata_override.title", prefix, index), destination.MetadataOverride.Title},
				{fmt.Sprintf("%s.destinations.%d.metadata_override.description", prefix, index), destination.MetadataOverride.Description},
				{fmt.Sprintf("%s.destinations.%d.metadata_override.category_id", prefix, index), destination.MetadataOverride.CategoryID},
				{fmt.Sprintf("%s.destinations.%d.metadata_override.privacy", prefix, index), destination.MetadataOverride.Privacy},
				{fmt.Sprintf("%s.destinations.%d.metadata_override.publish_at", prefix, index), destination.MetadataOverride.PublishAt},
			}
			for _, metadataField := range metadataFields {
				metadataPath, value := metadataField.path, metadataField.value
				if !utf8.ValidString(value) {
					details = append(details, publicationDetail(metadataPath, "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
				}
			}
			for tagIndex, tag := range destination.MetadataOverride.Tags {
				if !utf8.ValidString(tag) {
					details = append(details, publicationDetail(fmt.Sprintf("%s.metadata_override.tags.%d", prefix, tagIndex), "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
				}
			}
		}
	}
	locales := make([]string, 0, len(input.Localizations))
	for locale := range input.Localizations {
		locales = append(locales, locale)
	}
	sort.Strings(locales)
	for _, locale := range locales {
		metadata := input.Localizations[locale]
		path := prefix + ".localizations." + locale
		if !utf8.ValidString(locale) {
			details = append(details, publicationDetail(path, "invalid_utf8", map[string]any{"expected": "valid UTF-8 locale key"}))
		}
		if !utf8.ValidString(metadata.Title) {
			details = append(details, publicationDetail(path+".title", "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
		}
		if !utf8.ValidString(metadata.Description) {
			details = append(details, publicationDetail(path+".description", "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
		}
	}
	return details
}

func validatePublicationLanguageFields(input SubmitPublication, prefix string) []gin.H {
	var details []gin.H
	check := func(path, value string) {
		if value == "" {
			return
		}
		normalized := publication.NormalizeLanguage(value)
		if normalized == "" || !publicationLanguagePattern.MatchString(normalized) {
			details = append(details, publicationDetail(path, "invalid_language", map[string]any{
				"observed": value,
				"expected": "BCP-47-like language tag such as en, it, or pt-BR",
			}))
		} else if normalized != value {
			details = append(details, publicationDetail(path, "not_normalized", map[string]any{
				"observed":   value,
				"normalized": normalized,
				"expected":   "canonical BCP-47-like language tag",
			}))
		}
	}
	check(prefix+".language", input.Language)
	check(prefix+".default_language", input.DefaultLanguage)
	locales := make([]string, 0, len(input.Localizations))
	for locale := range input.Localizations {
		locales = append(locales, locale)
	}
	sort.Strings(locales)
	seenLocales := make(map[string]string, len(locales))
	for _, locale := range locales {
		localePath := prefix + ".localizations." + locale
		if locale == "" {
			details = append(details, publicationDetail(localePath, "required", map[string]any{
				"expected": "non-empty locale key",
			}))
			continue
		}
		check(localePath, locale)
		normalized := publication.NormalizeLanguage(locale)
		if normalized == "" {
			continue
		}
		if previous, exists := seenLocales[normalized]; exists && previous != locale {
			details = append(details, publicationDetail(prefix+".localizations."+normalized, "duplicate_locale", map[string]any{
				"observed":     locale,
				"duplicate_of": prefix + ".localizations." + previous,
			}))
		} else {
			seenLocales[normalized] = locale
		}
	}
	return details
}

func mapSharedPublicationErrors(err error, prefix string) []gin.H {
	var validationErrors publication.ValidationErrors
	if errors.As(err, &validationErrors) {
		details := make([]gin.H, 0, len(validationErrors))
		for _, validationError := range validationErrors {
			if validationError == nil {
				continue
			}
			details = append(details, sharedPublicationDetail(prefix, validationError))
		}
		return details
	}
	var validationError *publication.ValidationError
	if errors.As(err, &validationError) && validationError != nil {
		return []gin.H{sharedPublicationDetail(prefix, validationError)}
	}
	return []gin.H{publicationDetail(prefix, "invalid_publication", map[string]any{"message": err.Error()})}
}

func sharedPublicationDetail(prefix string, validationError *publication.ValidationError) gin.H {
	path := prefix
	if validationError.Field() != "" {
		path += "." + validationError.Field()
	}
	return publicationDetail(path, validationError.Issue(), map[string]any{
		"message": validationError.Message,
	})
}

func publicationDetail(path, issue string, fields map[string]any) gin.H {
	detail := gin.H{"path": path, "issue": issue}
	for key, value := range fields {
		detail[key] = value
	}
	return detail
}

func validateProviderOptions(options map[string]any, path string) []gin.H {
	if options == nil {
		return nil
	}
	details := validateJSONValue(reflect.ValueOf(options), path, make(map[providerVisit]bool))
	if err := jsonMarshalProviderOptions(options); err != nil {
		details = append(details, publicationDetail(path, "invalid_json_payload", map[string]any{
			"message": err.Error(),
		}))
	}
	return details
}

func jsonMarshalProviderOptions(options map[string]any) error {
	_, err := json.Marshal(options)
	return err
}

type providerVisit struct {
	kind   reflect.Kind
	typeOf reflect.Type
	ptr    uintptr
}

func validateJSONValue(value reflect.Value, path string, active map[providerVisit]bool) []gin.H {
	if !value.IsValid() {
		return nil
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}

	// Maps, slices, and pointers can form recursive provider-option values.
	// Track only the active recursion branch: the same object may legitimately
	// be referenced by two independent fields without being a cycle.
	if visit, trackable := providerVisitFor(value); trackable {
		if active[visit] {
			return []gin.H{publicationDetail(path, "invalid_json_payload", map[string]any{"message": "cyclic provider options"})}
		}
		active[visit] = true
		defer delete(active, visit)
	}

	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return validateJSONValue(value.Elem(), path, active)
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return []gin.H{publicationDetail(path, "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"})}
		}
	case reflect.Map:
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i].Interface()) < fmt.Sprint(keys[j].Interface()) })
		var details []gin.H
		for _, key := range keys {
			keyPath := path + "." + fmt.Sprint(key.Interface())
			if key.Kind() == reflect.String && !utf8.ValidString(key.String()) {
				details = append(details, publicationDetail(keyPath, "invalid_utf8", map[string]any{"expected": "valid UTF-8 string"}))
			}
			details = append(details, validateJSONValue(value.MapIndex(key), keyPath, active)...)
		}
		return details
	case reflect.Slice, reflect.Array:
		var details []gin.H
		for index := 0; index < value.Len(); index++ {
			details = append(details, validateJSONValue(value.Index(index), fmt.Sprintf("%s.%d", path, index), active)...)
		}
		return details
	case reflect.Struct:
		var details []gin.H
		typeOf := value.Type()
		for index := 0; index < value.NumField(); index++ {
			field := typeOf.Field(index)
			if field.PkgPath != "" {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			details = append(details, validateJSONValue(value.Field(index), path+"."+name, active)...)
		}
		return details
	}
	return nil
}

func providerVisitFor(value reflect.Value) (providerVisit, bool) {
	if !value.IsValid() {
		return providerVisit{}, false
	}
	switch value.Kind() {
	case reflect.Map:
		if value.IsNil() {
			return providerVisit{}, false
		}
		return providerVisit{kind: value.Kind(), typeOf: value.Type(), ptr: value.Pointer()}, true
	case reflect.Slice:
		if value.IsNil() || value.Pointer() == 0 {
			return providerVisit{}, false
		}
		return providerVisit{kind: value.Kind(), typeOf: value.Type(), ptr: value.Pointer()}, true
	case reflect.Pointer:
		if value.IsNil() {
			return providerVisit{}, false
		}
		return providerVisit{kind: value.Kind(), typeOf: value.Type(), ptr: value.Pointer()}, true
	default:
		return providerVisit{}, false
	}
}
