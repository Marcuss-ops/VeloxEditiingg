// Package publication defines the canonical publication intent shared by the
// master, task graph and delivery workers. It deliberately contains no render
// payload fields: OutputRef points at an output artifact without describing how
// that artifact was produced.
package publication

import (
	"errors"
	"fmt"
	"strings"
)

// Version is the current wire version of a publication specification.
const Version = 1

// ErrInvalidSpec identifies a structurally invalid publication specification.
var ErrInvalidSpec = errors.New("publication: invalid spec")

// ErrMissingTitle identifies a publication that cannot be sent because no
// title could be resolved from its metadata or the job's video name.
var ErrMissingTitle = errors.New("publication: missing publication title")

// Spec is one concrete publication of a rendered output. A single spec may
// target several destinations; each destination is independently routable and
// may provide a metadata override.
type Spec struct {
	Version         int                     `json:"version,omitempty"`
	PublicationID   string                  `json:"publication_id"`
	OutputRef       OutputRef               `json:"output_ref"`
	Language        string                  `json:"language,omitempty"`
	DefaultLanguage string                  `json:"default_language,omitempty"`
	Metadata        Metadata                `json:"metadata,omitempty"`
	Localizations   map[string]Localization `json:"localizations,omitempty"`
	Destinations    []Destination           `json:"destinations"`
	ProviderOptions map[string]any          `json:"provider_options,omitempty"`
}

// OutputRef selects the render output consumed by a publication. Exactly one
// selector is allowed: VariantID for language-specific outputs or ArtifactRole
// for a role such as final_video or thumbnail.
type OutputRef struct {
	VariantID    string `json:"variant_id,omitempty"`
	ArtifactRole string `json:"artifact_role,omitempty"`
}

// Metadata contains provider-independent publication metadata. Provider
// adapters remain responsible for platform-specific length and capability
// limits; the shared contract validates encoding and structural invariants.
type Metadata struct {
	Title                  string   `json:"title,omitempty"`
	Description            string   `json:"description,omitempty"`
	Tags                   []string `json:"tags,omitempty"`
	CategoryID             string   `json:"category_id,omitempty"`
	Privacy                string   `json:"privacy,omitempty"`
	PublishAt              string   `json:"publish_at,omitempty"`
	MadeForKids            *bool    `json:"made_for_kids,omitempty"`
	ContainsSyntheticMedia *bool    `json:"contains_synthetic_media,omitempty"`
}

// Localization contains the title and description for one locale.
type Localization struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// Destination is one independent delivery target for a publication.
type Destination struct {
	DestinationID    string         `json:"destination_id"`
	Priority         int            `json:"priority,omitempty"`
	RetryBudget      *int           `json:"retry_budget,omitempty"`
	MetadataOverride *Metadata      `json:"metadata_override,omitempty"`
	ProviderOptions  map[string]any `json:"provider_options,omitempty"`
}

// ResolvedMetadata is the exact metadata selected for one delivery attempt.
// Language records the locale used for the resolution and is not serialized as
// part of the provider metadata object by default.
type ResolvedMetadata struct {
	Title                  string   `json:"title,omitempty"`
	Description            string   `json:"description,omitempty"`
	Tags                   []string `json:"tags,omitempty"`
	CategoryID             string   `json:"category_id,omitempty"`
	Privacy                string   `json:"privacy,omitempty"`
	PublishAt              string   `json:"publish_at,omitempty"`
	MadeForKids            *bool    `json:"made_for_kids,omitempty"`
	ContainsSyntheticMedia *bool    `json:"contains_synthetic_media,omitempty"`
	Language               string   `json:"language,omitempty"`
}

// ResolvedPublication is the immutable delivery snapshot produced by the
// canonical resolver. It contains the metadata for one publication and one
// destination, while retaining all localizations and effective provider
// options needed by an adapter. MetadataHash is the idempotency key component
// for this exact delivery snapshot.
type ResolvedPublication struct {
	PublicationID   string                  `json:"publication_id"`
	DestinationID   string                  `json:"destination_id"`
	OutputRef       OutputRef               `json:"output_ref"`
	Metadata        ResolvedMetadata        `json:"metadata"`
	Localizations   map[string]Localization `json:"localizations,omitempty"`
	ProviderOptions map[string]any          `json:"provider_options,omitempty"`
	MetadataHash    string                  `json:"metadata_hash"`
}

// ValidationError is a typed contract violation. Path uses dot notation (for
// example destinations.0.destination_id) so HTTP and storage layers can expose
// the same field location without parsing human-readable messages.
type ValidationError struct {
	Path    string
	Code    string
	Message string
	Wrapped error
}

func (e *ValidationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Path == "" {
		return e.Code + ": " + e.Message
	}
	return e.Path + ": " + e.Code + ": " + e.Message
}

// Unwrap preserves sentinel identity for callers using errors.Is.
func (e *ValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Wrapped
}

// Field returns the machine-readable field path.
func (e *ValidationError) Field() string {
	if e == nil {
		return ""
	}
	return e.Path
}

// Issue returns the stable machine-readable error code.
func (e *ValidationError) Issue() string {
	if e == nil {
		return ""
	}
	return e.Code
}

// ValidationErrors aggregates all violations found in one spec.
type ValidationErrors []*ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	parts := make([]string, len(e))
	for i := range e {
		parts[i] = e[i].Error()
	}
	return strings.Join(parts, "; ")
}

// Unwrap allows errors.Is/errors.As to inspect every collected violation.
func (e ValidationErrors) Unwrap() []error {
	out := make([]error, len(e))
	for i := range e {
		out[i] = e[i]
	}
	return out
}

// NewValidationError constructs a typed contract violation.
func NewValidationError(path, code, message string) *ValidationError {
	return &ValidationError{Path: path, Code: code, Message: message, Wrapped: ErrInvalidSpec}
}

// NewValidationErrorWrapped constructs a typed violation that preserves a
// more specific sentinel in addition to ErrInvalidSpec when appropriate.
func NewValidationErrorWrapped(path, code, message string, cause error) *ValidationError {
	return &ValidationError{Path: path, Code: code, Message: message, Wrapped: cause}
}

// Clone returns a deep copy suitable for handing to another pipeline stage.
// Callers can mutate the returned maps/slices without changing the original
// spec, which is important when destination-specific overrides are applied.
func (s Spec) Clone() Spec {
	out := s
	out.OutputRef = s.OutputRef
	out.Metadata = s.Metadata.Clone()
	out.Localizations = cloneLocalizations(s.Localizations)
	out.Destinations = make([]Destination, len(s.Destinations))
	for i, destination := range s.Destinations {
		out.Destinations[i] = destination.Clone()
	}
	out.ProviderOptions = cloneMap(s.ProviderOptions)
	return out
}

// Clone returns a deep copy of metadata.
func (m Metadata) Clone() Metadata {
	out := m
	out.Tags = append([]string(nil), m.Tags...)
	if m.MadeForKids != nil {
		value := *m.MadeForKids
		out.MadeForKids = &value
	}
	if m.ContainsSyntheticMedia != nil {
		value := *m.ContainsSyntheticMedia
		out.ContainsSyntheticMedia = &value
	}
	return out
}

// Clone returns a deep copy of a destination and its provider options.
func (d Destination) Clone() Destination {
	out := d
	out.ProviderOptions = cloneMap(d.ProviderOptions)
	if d.MetadataOverride != nil {
		metadata := d.MetadataOverride.Clone()
		out.MetadataOverride = &metadata
	}
	if d.RetryBudget != nil {
		budget := *d.RetryBudget
		out.RetryBudget = &budget
	}
	return out
}

func cloneLocalizations(in map[string]Localization) map[string]Localization {
	if in == nil {
		return nil
	}
	out := make(map[string]Localization, len(in))
	for locale, value := range in {
		out[locale] = value
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneBoolPointer(input *bool) *bool {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

// Clone returns a defensive copy of the resolved delivery snapshot.
func (p ResolvedPublication) Clone() ResolvedPublication {
	p.Metadata.Tags = append([]string(nil), p.Metadata.Tags...)
	p.Metadata.MadeForKids = cloneBoolPointer(p.Metadata.MadeForKids)
	p.Metadata.ContainsSyntheticMedia = cloneBoolPointer(p.Metadata.ContainsSyntheticMedia)
	p.Localizations = cloneLocalizations(p.Localizations)
	p.ProviderOptions = cloneMap(p.ProviderOptions)
	return p
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneMap(value)
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = cloneValue(item)
		}
		return out
	case []string:
		out := make([]string, len(value))
		copy(out, value)
		return out
	case map[string]string:
		out := make(map[string]string, len(value))
		for key, item := range value {
			out[key] = item
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(value))
		for i, item := range value {
			out[i] = cloneMap(item)
		}
		return out
	default:
		return value
	}
}

func (e ValidationErrors) formatCount() string {
	return fmt.Sprintf("%d validation error(s)", len(e))
}
