package assets

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// AssetPreflightRequirement identifies one local asset needed by a job before
// it is accepted. SHA256 and SizeBytes are optional request-side constraints;
// the registry metadata remains authoritative when they are omitted.
type AssetPreflightRequirement struct {
	AssetID   string
	SHA256    string
	SizeBytes int64
}

// AssetPreflightItem is the operator-facing result for one unique asset.
type AssetPreflightItem struct {
	AssetID        string `json:"asset_id"`
	Metadata       bool   `json:"metadata"`
	BlobResolvable bool   `json:"blob_resolvable"`
	SHA256Valid    bool   `json:"sha256_valid"`
	SizeValid      bool   `json:"size_valid"`
	Issue          string `json:"issue,omitempty"`
}

// AssetPreflightReport is deliberately a validation report, not a resolver.
// It never downloads, stages, promotes, or mutates an asset.
type AssetPreflightReport struct {
	Requested         int                  `json:"requested"`
	MetadataAvailable int                  `json:"metadata_available"`
	BlobResolvable    int                  `json:"blob_resolvable"`
	SHA256Valid       int                  `json:"sha256_valid"`
	SizeValid         int                  `json:"size_valid"`
	Items             []AssetPreflightItem `json:"items"`
}

type finalBlobReader interface {
	ReadFinal(storageKey string) (*os.File, error)
}

// Preflight checks registry metadata and already-promoted final blobs for the
// supplied assets. The final blob is read only to verify its digest; no
// worker-side cache or Drive path is touched.
func (s *AssetService) Preflight(ctx context.Context, requirements []AssetPreflightRequirement) (*AssetPreflightReport, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("asset preflight unavailable: asset registry is not configured")
	}
	reader, ok := s.blobStore.(finalBlobReader)
	if !ok || reader == nil {
		return nil, fmt.Errorf("asset preflight unavailable: final blob reader is not configured")
	}
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
		if asset == nil || asset.Status != AssetStatusReady || strings.TrimSpace(asset.StorageKey) == "" {
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

		file, err := reader.ReadFinal(asset.StorageKey)
		if err != nil {
			item.Issue = "blob_unresolvable"
			report.Items = append(report.Items, item)
			continue
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != asset.SizeBytes {
			_ = file.Close()
			item.Issue = "blob_size_mismatch"
			report.Items = append(report.Items, item)
			continue
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			_ = file.Close()
			item.Issue = "blob_read_failed"
			report.Items = append(report.Items, item)
			continue
		}
		actualSHA, hashErr := ComputeSHA256(file)
		_ = file.Close()
		if hashErr != nil || actualSHA != metadataSHA {
			item.Issue = "blob_sha256_mismatch"
			report.Items = append(report.Items, item)
			continue
		}
		item.BlobResolvable = true
		report.BlobResolvable++
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
