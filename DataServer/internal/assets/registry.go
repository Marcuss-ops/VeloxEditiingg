package assets

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// Source is what a resolver returns: a reader plus metadata.
type Source struct {
	Reader        io.ReadCloser
	SuggestedName string
	MIMEType      string
	ExpectedSize  int64
	SourceType    string
	Metadata      map[string]string
}

// Resolver turns a reference into a readable source of bytes.
type Resolver interface {
	// Scheme returns the URI scheme this resolver handles (e.g. "https", "drive", "velox-asset", "file").
	Scheme() string
	// Open fetches the bytes for reference and returns a Source with a reader.
	// The caller MUST close the Reader when done.
	Open(ctx context.Context, reference string) (*Source, error)
	// ServerOnly reports whether this resolver must only run on the master
	// (not be delegated to workers). The "file" resolver returns true.
	ServerOnly() bool
}

// ResolverRegistry dispatches references to the resolver matching their scheme.
type ResolverRegistry struct {
	resolvers map[string]Resolver
	ordered   []Resolver
}

// NewResolverRegistry creates a registry from the provided resolvers.
func NewResolverRegistry(resolvers ...Resolver) *ResolverRegistry {
	r := &ResolverRegistry{
		resolvers: make(map[string]Resolver, len(resolvers)),
	}
	for _, res := range resolvers {
		if res == nil {
			continue
		}
		scheme := strings.ToLower(strings.TrimSpace(res.Scheme()))
		if scheme == "" {
			continue
		}
		r.resolvers[scheme] = res
		r.ordered = append(r.ordered, res)
	}
	return r
}

// Register adds a resolver to the registry.
func (r *ResolverRegistry) Register(resolver Resolver) {
	if r == nil || resolver == nil {
		return
	}
	scheme := strings.ToLower(strings.TrimSpace(resolver.Scheme()))
	if scheme == "" {
		return
	}
	r.resolvers[scheme] = resolver
	r.ordered = append(r.ordered, resolver)
}

// ResolveByScheme dispatches to the resolver matching the scheme of reference.
// It parses the scheme from "scheme://..." prefix.
func (r *ResolverRegistry) ResolveByScheme(ctx context.Context, reference string) (*Source, error) {
	if r == nil {
		return nil, fmt.Errorf("resolver registry unavailable")
	}
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return nil, fmt.Errorf("empty reference")
	}

	scheme := extractScheme(trimmed)
	if scheme == "" {
		return nil, fmt.Errorf("cannot determine scheme for reference: %s", trimmed)
	}

	resolver, ok := r.resolvers[scheme]
	if !ok {
		return nil, fmt.Errorf("no resolver for scheme %q", scheme)
	}

	return resolver.Open(ctx, trimmed)
}

// ResolveByInference infers the scheme from the reference format and dispatches.
// Handles drive.google.com URLs, bare file paths, etc.
func (r *ResolverRegistry) ResolveByInference(ctx context.Context, reference string) (*Source, error) {
	if r == nil {
		return nil, fmt.Errorf("resolver registry unavailable")
	}
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return nil, fmt.Errorf("empty reference")
	}

	scheme := inferScheme(trimmed)
	resolver, ok := r.resolvers[scheme]
	if !ok {
		return nil, fmt.Errorf("no resolver for inferred scheme %q", scheme)
	}
	return resolver.Open(ctx, trimmed)
}

// Schemes returns the list of registered schemes.
func (r *ResolverRegistry) Schemes() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.ordered))
	for _, res := range r.ordered {
		out = append(out, res.Scheme())
	}
	return out
}

// extractScheme parses "scheme://..." prefix.
func extractScheme(reference string) string {
	idx := strings.Index(reference, "://")
	if idx <= 0 {
		return ""
	}
	return strings.ToLower(reference[:idx])
}

// inferScheme classifies a reference by format when no explicit scheme is present.
func inferScheme(reference string) string {
	lower := strings.ToLower(strings.TrimSpace(reference))
	switch {
	case strings.HasPrefix(lower, "https://"):
		if looksLikeDriveURL(reference) {
			return "drive"
		}
		return "https"
	case strings.HasPrefix(lower, "http://"):
		return "http"
	case strings.HasPrefix(lower, "velox-asset://"):
		return "velox-asset"
	case strings.HasPrefix(lower, "file://"):
		return "file"
	default:
		if looksLikeDriveURL(reference) {
			return "drive"
		}
		return "file"
	}
}


