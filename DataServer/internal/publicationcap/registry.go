// Package publicationcap describes provider capabilities before expensive
// rendering/upload work starts.
package publicationcap

import (
	"fmt"
	"strings"
)

type Capability struct {
	SupportsScheduledPublish bool
	SupportsLocalizations    bool
	SupportsCustomThumbnail  bool
	SupportsPlaylists        bool
	SupportsChapters         bool
	SupportsCaptions         bool
	MaxTitleBytes            int
	MaxDescriptionBytes      int
	MaxTags                  int
	MaxTagBytes              int
}

type Registry struct{ providers map[string]Capability }

func NewRegistry() *Registry { return &Registry{providers: map[string]Capability{}} }
func (r *Registry) Register(name string, cap Capability) {
	if r != nil && strings.TrimSpace(name) != "" {
		r.providers[strings.ToLower(strings.TrimSpace(name))] = cap
	}
}
func (r *Registry) Lookup(name string) (Capability, bool) {
	if r == nil {
		return Capability{}, false
	}
	cap, ok := r.providers[strings.ToLower(strings.TrimSpace(name))]
	return cap, ok
}

type Metadata struct {
	Title, Description string
	Tags               []string
	HasSchedule        bool
	HasLocalizations   bool
	HasThumbnail       bool
	HasPlaylists       bool
	HasChapters        bool
	HasCaptions        bool
}

func (r *Registry) Validate(provider string, metadata Metadata) error {
	cap, ok := r.Lookup(provider)
	if !ok {
		return fmt.Errorf("provider_capability_unknown: %s", provider)
	}
	if cap.MaxTitleBytes > 0 && len([]byte(metadata.Title)) > cap.MaxTitleBytes {
		return fmt.Errorf("provider_title_too_long: %d > %d", len([]byte(metadata.Title)), cap.MaxTitleBytes)
	}
	if cap.MaxDescriptionBytes > 0 && len([]byte(metadata.Description)) > cap.MaxDescriptionBytes {
		return fmt.Errorf("provider_description_too_long: %d > %d", len([]byte(metadata.Description)), cap.MaxDescriptionBytes)
	}
	if cap.MaxTags > 0 && len(metadata.Tags) > cap.MaxTags {
		return fmt.Errorf("provider_too_many_tags: %d > %d", len(metadata.Tags), cap.MaxTags)
	}
	for _, tag := range metadata.Tags {
		if cap.MaxTagBytes > 0 && len([]byte(tag)) > cap.MaxTagBytes {
			return fmt.Errorf("provider_tag_too_long: %d > %d", len([]byte(tag)), cap.MaxTagBytes)
		}
	}
	checks := []struct {
		requested, supported bool
		code                 string
	}{
		{metadata.HasSchedule, cap.SupportsScheduledPublish, "scheduled_publish"},
		{metadata.HasLocalizations, cap.SupportsLocalizations, "localizations"},
		{metadata.HasThumbnail, cap.SupportsCustomThumbnail, "custom_thumbnail"},
		{metadata.HasPlaylists, cap.SupportsPlaylists, "playlists"},
		{metadata.HasChapters, cap.SupportsChapters, "chapters"},
		{metadata.HasCaptions, cap.SupportsCaptions, "captions"},
	}
	for _, check := range checks {
		if check.requested && !check.supported {
			return fmt.Errorf("provider_capability_unsupported: %s", check.code)
		}
	}
	return nil
}

func DefaultRegistry() *Registry {
	r := NewRegistry()
	common := Capability{SupportsScheduledPublish: true, SupportsLocalizations: true, SupportsCustomThumbnail: true, SupportsPlaylists: true, SupportsChapters: true, SupportsCaptions: true, MaxTitleBytes: 100, MaxDescriptionBytes: 5000, MaxTags: 500, MaxTagBytes: 500}
	r.Register("youtube", common)
	r.Register("google_drive", Capability{MaxTitleBytes: 255, MaxDescriptionBytes: 100000})
	r.Register("drive", Capability{MaxTitleBytes: 255, MaxDescriptionBytes: 100000})
	r.Register("facebook", Capability{SupportsCustomThumbnail: true, SupportsCaptions: true, MaxTitleBytes: 255, MaxDescriptionBytes: 63206, MaxTags: 50, MaxTagBytes: 100})
	r.Register("tiktok", Capability{SupportsCaptions: true, MaxTitleBytes: 150, MaxDescriptionBytes: 2200, MaxTags: 100, MaxTagBytes: 100})
	return r
}
