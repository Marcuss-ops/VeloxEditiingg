package assets

import (
	"context"
	"fmt"
	"strings"
)

// Resolver turns a reference into a canonical stored asset.
type Resolver interface {
	Supports(reference string) bool
	Resolve(ctx context.Context, reference string) (*ResolvedAsset, error)
}

// Registry dispatches references to the first resolver that supports them.
type Registry struct {
	resolvers []Resolver
}

// NewRegistry creates a registry with the provided resolvers in priority order.
func NewRegistry(resolvers ...Resolver) *Registry {
	r := &Registry{}
	for _, resolver := range resolvers {
		if resolver != nil {
			r.resolvers = append(r.resolvers, resolver)
		}
	}
	return r
}

// Register appends a resolver to the registry.
func (r *Registry) Register(resolver Resolver) {
	if r == nil || resolver == nil {
		return
	}
	r.resolvers = append(r.resolvers, resolver)
}

// Resolve resolves the first matching reference.
func (r *Registry) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
	if r == nil {
		return nil, newAcquisitionError("voiceover_path", inferSourceType(reference), "asset registry unavailable", nil)
	}
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return nil, newAcquisitionError("voiceover_path", "unknown", "empty voiceover reference", nil)
	}

	for _, resolver := range r.resolvers {
		if resolver == nil || !resolver.Supports(trimmed) {
			continue
		}
		resolved, err := resolver.Resolve(ctx, trimmed)
		if err != nil {
			return nil, wrapAcquisitionError("voiceover_path", inferSourceType(trimmed), err)
		}
		if resolved == nil {
			return nil, newAcquisitionError("voiceover_path", inferSourceType(trimmed), "resolver returned no asset", nil)
		}
		if resolved.Reference == "" {
			resolved.Reference = resolved.VeloxReference()
		}
		return resolved, nil
	}

	return nil, newAcquisitionError("voiceover_path", inferSourceType(trimmed), fmt.Sprintf("unsupported voiceover reference: %s", sourceClass(trimmed)), nil)
}

func inferSourceType(reference string) string {
	trimmed := strings.TrimSpace(reference)
	switch {
	case trimmed == "":
		return "unknown"
	case strings.HasPrefix(trimmed, VeloxAssetScheme+"://"):
		return "velox_asset"
	case strings.HasPrefix(strings.ToLower(trimmed), "http://"):
		return "http"
	case strings.HasPrefix(strings.ToLower(trimmed), "https://"):
		if looksLikeDriveURL(trimmed) {
			return "drive"
		}
		return "http"
	case strings.HasPrefix(strings.ToLower(trimmed), "file://"):
		return "local_file"
	default:
		if looksLikeDriveURL(trimmed) {
			return "drive"
		}
		return "local_file"
	}
}

func sourceClass(reference string) string {
	switch inferSourceType(reference) {
	case "drive":
		return "drive"
	case "http":
		return "http"
	case "velox_asset":
		return "velox-asset"
	case "local_file":
		return "local file"
	default:
		return "unknown"
	}
}
