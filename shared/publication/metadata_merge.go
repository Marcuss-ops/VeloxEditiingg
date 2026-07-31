package publication

import (
	"fmt"
	"strings"
)

// Apply copies only fields explicitly present in source. Empty strings and nil
// pointers are treated as absent, preserving inherited values. Tags are copied
// when non-nil, including an explicitly empty non-nil slice.
func (m *ResolvedMetadata) Apply(source Metadata) {
	if m == nil {
		return
	}
	if source.Title != "" {
		m.Title = source.Title
	}
	if source.Description != "" {
		m.Description = source.Description
	}
	if source.Tags != nil {
		m.Tags = make([]string, len(source.Tags))
		copy(m.Tags, source.Tags)
	}
	if source.CategoryID != "" {
		m.CategoryID = source.CategoryID
	}
	if source.Privacy != "" {
		m.Privacy = source.Privacy
	}
	if source.PublishAt != "" {
		m.PublishAt = source.PublishAt
	}
	if source.MadeForKids != nil {
		value := *source.MadeForKids
		m.MadeForKids = &value
	}
	if source.ContainsSyntheticMedia != nil {
		value := *source.ContainsSyntheticMedia
		m.ContainsSyntheticMedia = &value
	}
}

// ResolveMetadata calculates the exact metadata for one destination. The
// precedence is destination override, requested localization, publication
// metadata, then job video_name. Apply is intentionally called from lowest to
// highest precedence so absent fields inherit rather than being erased.
func ResolveMetadata(jobVideoName string, spec Spec, destination Destination, requestedLocale string) (ResolvedMetadata, error) {
	canonical, err := spec.Normalize()
	if err != nil {
		return ResolvedMetadata{}, err
	}
	locale := NormalizeLanguage(requestedLocale)
	if locale == "" {
		locale = canonical.Language
	}
	if locale == "" && len(canonical.Localizations) > 0 {
		locale = canonical.DefaultLanguage
	}

	resolved := ResolvedMetadata{Title: strings.TrimSpace(jobVideoName), Language: locale}
	resolved.Apply(canonical.Metadata)
	if localized, ok := canonical.Localizations[locale]; ok {
		resolved.Apply(Metadata{Title: localized.Title, Description: localized.Description})
	}
	if destination.MetadataOverride != nil {
		resolved.Apply(*destination.MetadataOverride)
	}
	if strings.TrimSpace(resolved.Title) == "" {
		return ResolvedMetadata{}, fmt.Errorf("%w: publication_id %q", ErrMissingTitle, canonical.PublicationID)
	}
	return resolved, nil
}
