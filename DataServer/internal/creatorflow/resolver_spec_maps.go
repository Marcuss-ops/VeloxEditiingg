package creatorflow

import "velox-shared/publication"

// resolver_spec_maps.go owns the map conversions between the typed
// control-plane contracts and the map-shaped TaskSpec fields used by
// Resolver.Resolve:
//
//   - preparePayloadWithControlPlaneDelivery: short-lived enqueue envelope.
//   - clonePublicationSpecs / publicationSpecToMap / publicationMetadataToMap /
//     clonePublicationValue: typed publication.Spec projection to the
//     map shape required by TaskSpec.PublicationSpecs.
//   - cloneControlPlaneMap: shallow map copy for the delivery envelope.

// preparePayloadWithControlPlaneDelivery builds the short-lived enqueue
// envelope. Delivery routing is present only long enough for enqueue's
// validation/parser; compileSceneVideoJob removes it before TaskSpec.Payload
// is persisted for the renderer. Publication specs never enter this map.
func preparePayloadWithControlPlaneDelivery(rendererPayload, deliveryPlan map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(rendererPayload)+len(deliveryPlan))
	for key, value := range rendererPayload {
		out[key] = value
	}
	for key, value := range deliveryPlan {
		out[key] = value
	}
	return out
}

func clonePublicationSpecs(specs []publication.Spec) []map[string]interface{} {
	if len(specs) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, len(specs))
	for i, spec := range specs {
		out[i] = publicationSpecToMap(spec)
	}
	return out
}

// publicationSpecToMap projects the typed control-plane contract directly to
// the map shape required by TaskSpec.PublicationSpecs. The previous
// implementation marshaled and unmarshaled every spec through JSON, adding
// substantial CPU and allocation cost to every fresh Resolve. Keep the
// projection explicit so renderer payloads remain untouched and the wire
// contract stays map-based.
func publicationSpecToMap(spec publication.Spec) map[string]interface{} {
	out := make(map[string]interface{}, 9)
	if spec.Version != 0 {
		out["version"] = spec.Version
	}
	out["publication_id"] = spec.PublicationID

	outputRef := make(map[string]interface{}, 2)
	if spec.OutputRef.VariantID != "" {
		outputRef["variant_id"] = spec.OutputRef.VariantID
	}
	if spec.OutputRef.ArtifactRole != "" {
		outputRef["artifact_role"] = spec.OutputRef.ArtifactRole
	}
	out["output_ref"] = outputRef
	if spec.Language != "" {
		out["language"] = spec.Language
	}
	if spec.DefaultLanguage != "" {
		out["default_language"] = spec.DefaultLanguage
	}
	out["metadata"] = publicationMetadataToMap(spec.Metadata)

	if len(spec.Localizations) > 0 {
		localizations := make(map[string]interface{}, len(spec.Localizations))
		for locale, metadata := range spec.Localizations {
			value := make(map[string]interface{}, 2)
			if metadata.Title != "" {
				value["title"] = metadata.Title
			}
			if metadata.Description != "" {
				value["description"] = metadata.Description
			}
			localizations[locale] = value
		}
		out["localizations"] = localizations
	}

	if spec.Destinations == nil {
		out["destinations"] = nil
	} else {
		destinations := make([]interface{}, len(spec.Destinations))
		for i, destination := range spec.Destinations {
			value := make(map[string]interface{}, 6)
			value["destination_id"] = destination.DestinationID
			if destination.CredentialRef != "" {
				value["credential_ref"] = destination.CredentialRef
			}
			if destination.Priority != 0 {
				value["priority"] = destination.Priority
			}
			if destination.RetryBudget != nil {
				value["retry_budget"] = *destination.RetryBudget
			}
			if destination.MetadataOverride != nil {
				value["metadata_override"] = publicationMetadataToMap(*destination.MetadataOverride)
			}
			if len(destination.ProviderOptions) > 0 {
				value["provider_options"] = clonePublicationValue(destination.ProviderOptions)
			}
			destinations[i] = value
		}
		out["destinations"] = destinations
	}
	if len(spec.ProviderOptions) > 0 {
		out["provider_options"] = clonePublicationValue(spec.ProviderOptions)
	}
	return out
}

func publicationMetadataToMap(metadata publication.Metadata) map[string]interface{} {
	out := make(map[string]interface{}, 8)
	if metadata.Title != "" {
		out["title"] = metadata.Title
	}
	if metadata.Description != "" {
		out["description"] = metadata.Description
	}
	if len(metadata.Tags) > 0 {
		out["tags"] = append([]string(nil), metadata.Tags...)
	}
	if metadata.CategoryID != "" {
		out["category_id"] = metadata.CategoryID
	}
	if metadata.Privacy != "" {
		out["privacy"] = metadata.Privacy
	}
	if metadata.PublishAt != "" {
		out["publish_at"] = metadata.PublishAt
	}
	if metadata.MadeForKids != nil {
		out["made_for_kids"] = *metadata.MadeForKids
	}
	if metadata.ContainsSyntheticMedia != nil {
		out["contains_synthetic_media"] = *metadata.ContainsSyntheticMedia
	}
	return out
}

// clonePublicationValue deep-copies JSON-compatible provider options without
// re-encoding them. Resolver inputs originate from decoded JSON, so these are
// the map/slice forms that need recursive copying; scalar values are immutable.
func clonePublicationValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			out[key] = clonePublicationValue(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = clonePublicationValue(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, len(typed))
		for i, item := range typed {
			if item != nil {
				out[i] = clonePublicationValue(item).(map[string]interface{})
			}
		}
		return out
	case []int:
		return append([]int(nil), typed...)
	case []int64:
		return append([]int64(nil), typed...)
	case []float64:
		return append([]float64(nil), typed...)
	case []bool:
		return append([]bool(nil), typed...)
	default:
		return value
	}
}

func cloneControlPlaneMap(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
