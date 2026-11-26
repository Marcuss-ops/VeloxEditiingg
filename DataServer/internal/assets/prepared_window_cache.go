package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"velox-shared/contract"
)

// LookupPreparedWindowAsset returns the certified fragment registered by any
// worker for a content-preparation identity. FINALIZE consults this before
// publishing a preparation task, so a window prepared for one job is reused
// by another job and by a different assembly worker without re-encoding,
// re-probing or transferring the source again.
//
// The identity is a hash over the canonical source window, output profile
// revision and preparation revision, so a hit already implies matching
// evidence; READY status plus a certified SHA-256/size still fail closed.
func (s *AssetService) LookupPreparedWindowAsset(ctx context.Context, identity string) (*PreparedWindowAssetBinding, error) {
	assets, err := s.LookupPreparedWindowAssets(ctx, []string{identity})
	if err != nil {
		return nil, err
	}
	binding, ok := assets[strings.TrimSpace(identity)]
	if !ok {
		return nil, nil
	}
	return &binding, nil
}

// LookupPreparedWindowAssets checks multiple immutable window identities with
// one source-index query when the repository supports it. Cache certificates
// and local blob presence are still validated for every returned asset.
func (s *AssetService) LookupPreparedWindowAssets(ctx context.Context, identities []string) (map[string]PreparedWindowAssetBinding, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("prepared-window cache lookup is unavailable")
	}
	unique := make([]string, 0, len(identities))
	references := make([]string, 0, len(identities))
	seen := make(map[string]struct{}, len(identities))
	for _, identity := range identities {
		identity = strings.TrimSpace(identity)
		if len(identity) != 64 {
			return nil, fmt.Errorf("prepared-window cache identity must be a SHA-256")
		}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		unique = append(unique, identity)
		references = append(references, "prepared-window:"+identity)
	}
	reader, batchOK := s.repo.(assetSourceAssetsReader)
	var records map[string]*AssetRecord
	var assetsByReference map[string]*Asset
	if batchOK {
		var err error
		records, err = reader.GetBySourceReferences(ctx, references)
		if err != nil {
			return nil, fmt.Errorf("prepared-window cache batch lookup: %w", err)
		}
	} else {
		assetsByReference = make(map[string]*Asset, len(references))
		for _, reference := range references {
			asset, err := s.GetBySourceReference(ctx, reference)
			if err != nil {
				return nil, fmt.Errorf("prepared-window cache lookup: %w", err)
			}
			assetsByReference[reference] = asset
		}
	}
	out := make(map[string]PreparedWindowAssetBinding, len(records))
	for i, identity := range unique {
		var asset *Asset
		if batchOK {
			rec := records[references[i]]
			if rec == nil || rec.Status != AssetStatusReady {
				continue
			}
			if reader, ok := s.blobStore.(interface {
				ReadFinal(string) (*os.File, error)
			}); ok && rec.StorageKey != "" && !registeredBlobPresent(reader, rec.StorageKey, rec.SizeBytes) {
				continue
			}
			asset = s.recordToAsset(rec)
		} else {
			asset = assetsByReference[references[i]]
		}
		binding, err := preparedWindowBindingFromAsset(identity, asset)
		if err != nil {
			return nil, err
		}
		if binding != nil {
			out[identity] = *binding
		}
	}
	return out, nil
}

func preparedWindowBindingFromAsset(identity string, asset *Asset) (*PreparedWindowAssetBinding, error) {
	if asset == nil {
		return nil, nil
	}
	if asset.Status != AssetStatusReady || !validStockSourceSHA256(asset.SHA256) || asset.SizeBytes <= 0 {
		return nil, fmt.Errorf("prepared-window cache asset %q is not a certified READY fragment", identity)
	}
	var metadata struct {
		WindowCertificate struct {
			OutputEvidence struct {
				ExtradataSHA256 string `json:"extradata_sha256"`
			} `json:"output_evidence"`
		} `json:"window_certificate"`
	}
	if err := json.Unmarshal([]byte(asset.MetadataJSON), &metadata); err != nil {
		return nil, nil
	}
	extradata := strings.ToLower(strings.TrimSpace(metadata.WindowCertificate.OutputEvidence.ExtradataSHA256))
	if extradata != contract.CanonicalVideoExtradataSHA256 {
		return nil, nil
	}
	return &PreparedWindowAssetBinding{AssetID: asset.AssetID, SHA256: asset.SHA256, SizeBytes: asset.SizeBytes, ExtradataSHA256: extradata}, nil
}
