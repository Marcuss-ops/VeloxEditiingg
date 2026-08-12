package assets

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// AssetPreflightRequirement identifies one registered asset needed by a job
// before it is accepted. The asset may be backed by a local final blob or by
// an external source such as Drive; SHA256 and SizeBytes are optional
// request-side constraints and registry metadata remains authoritative when
// they are omitted.
type AssetPreflightRequirement struct {
	AssetID   string
	SHA256    string
	SizeBytes int64
}

// AssetPreflightItem is the operator-facing result for one unique asset.
type AssetPreflightItem struct {
	AssetID string `json:"asset_id"`
	// Metadata reports the registry row exists and the asset is READY.
	Metadata bool `json:"metadata"` // MediaMetadata reports verified media metadata for a media asset
	// (video/audio MIME), or N/A (true) for non-media assets. A media
	// asset that cannot be verified is false (Fase C2 fail-closed gate).
	// Assets with unknown/empty MIME are treated as non-media (N/A) —
	// they cannot be classified as media, so the gate does not apply.
	MediaMetadata bool `json:"media_metadata"`
	// BlobResolvable means bytes are available either from the promoted final
	// blob or from a registered source that can be resolved at execution time.
	// It deliberately does not download external sources during preflight.
	BlobResolvable bool   `json:"blob_resolvable"`
	SHA256Valid    bool   `json:"sha256_valid"`
	SizeValid      bool   `json:"size_valid"`
	Issue          string `json:"issue,omitempty"`
}

// AssetPreflightReport is deliberately a validation report, not a resolver.
// It never downloads, stages, or promotes an asset; the ONLY possible
// registry write is the canonical one-time media-metadata probe
// (EnsureMediaMetadata) for media assets (Fase C2), which is idempotent
// and never invents metadata.
type AssetPreflightReport struct {
	Requested              int                  `json:"requested"`
	MetadataAvailable      int                  `json:"metadata_available"`
	MediaMetadataAvailable int                  `json:"media_metadata_available"`
	BlobResolvable         int                  `json:"blob_resolvable"`
	SHA256Valid            int                  `json:"sha256_valid"`
	SizeValid              int                  `json:"size_valid"`
	Items                  []AssetPreflightItem `json:"items"`
}

type finalBlobReader interface {
	ReadFinal(storageKey string) (*os.File, error)
}

// Preflight checks registry metadata and either already-promoted final blobs
// or registered external sources for the supplied assets, then enforces the
// Fase C2 fail-closed media gate: a media asset (video/audio MIME) MUST carry
// verified registry metadata before the job is admitted. The final blob is
// read only to verify its digest; external sources are not downloaded here.
// For media assets,
// the canonical EnsureMediaMetadata runs (registry hit → no probe; missing →
// probe ONCE via the single MediaMetadataResolver + persist; unverifiable →
// item flagged media_metadata_unavailable, fail closed).
func (s *AssetService) Preflight(ctx context.Context, requirements []AssetPreflightRequirement) (*AssetPreflightReport, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("asset preflight unavailable: asset registry is not configured")
	}
	reader, _ := s.blobStore.(finalBlobReader)
	report := &AssetPreflightReport{
		Requested: len(requirements),
		Items:     make([]AssetPreflightItem, 0, len(requirements)),
	}
	for _, req := range requirements {
		item := AssetPreflightItem{AssetID: strings.TrimSpace(req.AssetID)}
		if item.AssetID == "" {
			item.Issue = "asset_id_empty"
			report.Items = append(report.Items, item)
			continue
		}
		asset, err := s.Get(ctx, item.AssetID)
		if err != nil {
			item.Issue = "metadata_lookup_failed"
			report.Items = append(report.Items, item)
			continue
		}
		if asset == nil || asset.Status != AssetStatusReady {
			item.Issue = "metadata_unavailable"
			report.Items = append(report.Items, item)
			continue
		}
		item.Metadata = true
		report.MetadataAvailable++

		metadataSHA := strings.ToLower(strings.TrimSpace(asset.SHA256))
		requestedSHA := strings.ToLower(strings.TrimSpace(req.SHA256))
		item.SHA256Valid = len(metadataSHA) == 64 && (requestedSHA == "" || requestedSHA == metadataSHA)
		if item.SHA256Valid {
			report.SHA256Valid++
		}
		item.SizeValid = asset.SizeBytes > 0 && (req.SizeBytes <= 0 || req.SizeBytes == asset.SizeBytes)
		if item.SizeValid {
			report.SizeValid++
		}

		localVerified := false
		localIssue := "blob_unresolvable"
		if reader != nil && strings.TrimSpace(asset.StorageKey) != "" {
			file, readErr := reader.ReadFinal(asset.StorageKey)
			if readErr == nil {
				info, statErr := file.Stat()
				if statErr != nil || !info.Mode().IsRegular() {
					localIssue = "blob_read_failed"
				} else if info.Size() != asset.SizeBytes {
					localIssue = "blob_size_mismatch"
				} else {
					if _, seekErr := file.Seek(0, io.SeekStart); seekErr == nil {
						actualSHA, hashErr := ComputeSHA256(file)
						localVerified = hashErr == nil && actualSHA == metadataSHA
						if !localVerified {
							localIssue = "blob_sha256_mismatch"
						}
					} else {
						localIssue = "blob_read_failed"
					}
				}
				_ = file.Close()
			}
		}
		if localVerified || s.HasResolvableExternalSource(ctx, item.AssetID) {
			item.BlobResolvable = true
			report.BlobResolvable++
		} else {
			item.Issue = localIssue
			report.Items = append(report.Items, item)
			continue
		}

		// Fase C2 fail-closed media gate: a local media asset (video/audio
		// MIME) MUST carry verified registry metadata before the job is
		// admitted. EnsureMediaMetadata consumes the registry when a
		// verified row exists (no probe); otherwise it probes ONCE through
		// the single MediaMetadataResolver and persists the result. An
		// unverifiable media asset is REJECTED — metadata is never
		// invented. Non-media assets (fonts, subtitles, project files) are
		// N/A and pass.
		if _, mediaErr := s.EnsureMediaMetadata(ctx, item.AssetID); mediaErr != nil {
			item.Issue = "media_metadata_unavailable"
			report.Items = append(report.Items, item)
			continue
		}
		item.MediaMetadata = true
		report.MediaMetadataAvailable++

		if item.Issue == "" && item.SHA256Valid && item.SizeValid {
			// A fully verified item has no issue. Keep the individual flags
			// explicit so callers can render a useful matrix.
		} else if item.Issue == "" {
			item.Issue = "metadata_integrity_mismatch"
		}
		report.Items = append(report.Items, item)
	}
	return report, nil
}
