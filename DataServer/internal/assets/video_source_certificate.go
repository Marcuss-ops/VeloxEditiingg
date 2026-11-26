package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/identity"
	"velox-shared/contract"
)

func sourceCertificateReference(sha, revision string) string {
	return "certified-video-source:" + sha + ":" + revision
}

// Source facts are indexed independently by immutable source identity. Older
// repositories retain a fragment-anchored fallback during migration.
func (s *AssetService) registerSourceCertificate(ctx context.Context, asset *Asset, source *contract.VideoSourceCertificateV1) (*Asset, error) {
	if source == nil {
		return asset, nil
	}
	if err := source.Validate(); err != nil {
		return nil, err
	}
	metadata, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	if registry, ok := s.repo.(sourceCertificateRegistry); ok {
		reference := sourceCertificateReference(source.SHA256, source.PreparationRevision)
		existing, readErr := registry.GetVideoSourceCertificates(ctx, []string{reference})
		if readErr != nil {
			return nil, fmt.Errorf("read source certificate before registration: %w", readErr)
		}
		if previousJSON := existing[reference]; previousJSON != "" {
			var previous contract.VideoSourceCertificateV1
			if json.Unmarshal([]byte(previousJSON), &previous) != nil || previous.Validate() != nil {
				return nil, fmt.Errorf("stored source certificate is invalid")
			}
			previousChunks := previous.Chunks
			if !sameSourceCertificateFacts(previous, *source) {
				return nil, fmt.Errorf("source certificate conflicts with existing immutable evidence")
			}
			if previousChunks != nil {
				if source.Chunks != nil && !sameSourceChunks(previousChunks, source.Chunks) {
					return nil, fmt.Errorf("source blocks conflict with existing immutable evidence")
				}
				return asset, nil
			}
			if source.Chunks == nil {
				return asset, nil
			}
			upgraded, updateErr := registry.ReplaceVideoSourceCertificate(ctx, reference, previousJSON, string(metadata))
			if updateErr != nil {
				return nil, updateErr
			}
			if !upgraded {
				latest, latestErr := registry.GetVideoSourceCertificates(ctx, []string{reference})
				if latestErr != nil {
					return nil, latestErr
				}
				var concurrent contract.VideoSourceCertificateV1
				if json.Unmarshal([]byte(latest[reference]), &concurrent) != nil || concurrent.Validate() != nil || concurrent.Chunks == nil || source.Chunks == nil || !sameSourceCertificateFacts(concurrent, *source) || !sameSourceChunks(concurrent.Chunks, source.Chunks) {
					return nil, fmt.Errorf("concurrent source certificate upgrade conflicts with immutable evidence")
				}
			}
			return asset, nil
		}
		if err := registry.PutVideoSourceCertificate(ctx, reference, source.SHA256, source.SizeBytes, source.PreparationRevision, string(metadata), s.clock.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return nil, fmt.Errorf("register source certificate: %w", err)
		}
		// INSERT OR IGNORE makes registration safe under races. Verify that a
		// concurrent producer did not win with conflicting immutable evidence.
		stored, readErr := registry.GetVideoSourceCertificates(ctx, []string{reference})
		if readErr != nil {
			return nil, fmt.Errorf("verify source certificate registration: %w", readErr)
		}
		if stored[reference] != string(metadata) {
			var previous contract.VideoSourceCertificateV1
			if json.Unmarshal([]byte(stored[reference]), &previous) != nil || previous.Validate() != nil || !sameSourceCertificateFacts(previous, *source) || !sameSourceChunks(previous.Chunks, source.Chunks) {
				return nil, fmt.Errorf("concurrent source certificate conflicts with immutable evidence")
			}
		}
		return asset, nil
	}
	if reader, ok := s.repo.(assetSourceAssetReader); ok {
		existing, readErr := reader.GetBySourceReference(ctx, sourceCertificateReference(source.SHA256, source.PreparationRevision))
		if readErr != nil {
			return nil, fmt.Errorf("read source certificate before registration: %w", readErr)
		}
		if existing != nil && existing.Status == AssetStatusReady {
			var previous contract.VideoSourceCertificateV1
			if json.Unmarshal([]byte(existing.MetadataJSON), &previous) == nil && previous.Validate() == nil {
				previousChunks := previous.Chunks
				previous.Chunks = nil
				current := *source
				current.Chunks = nil
				before, _ := json.Marshal(previous)
				after, _ := json.Marshal(current)
				if string(before) != string(after) {
					return nil, fmt.Errorf("source certificate conflicts with existing immutable evidence")
				}
				if previousChunks != nil {
					if source.Chunks != nil {
						before, _ = json.Marshal(previousChunks)
						after, _ = json.Marshal(source.Chunks)
						if string(before) != string(after) {
							return nil, fmt.Errorf("source blocks conflict with existing immutable evidence")
						}
					}
					return asset, nil
				}
				if source.Chunks == nil {
					return asset, nil
				}

			}
		}
	}
	id, err := identity.NewHex128()
	if err != nil {
		return nil, err
	}
	err = s.repo.InsertSource(ctx, AssetSourceRecord{SourceID: id, AssetID: asset.AssetID, SourceType: "video.source.certificate.v1", SourceReference: sourceCertificateReference(source.SHA256, source.PreparationRevision), MetadataJSON: string(metadata), CreatedAt: s.clock.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return nil, fmt.Errorf("register source certificate: %w", err)
	}
	return asset, nil
}

func sameSourceCertificateFacts(left, right contract.VideoSourceCertificateV1) bool {
	left.Chunks = nil
	right.Chunks = nil
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && string(a) == string(b)
}

func sameSourceChunks(left, right *contract.VideoSourceChunksV1) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && string(a) == string(b)
}

// EnrichPlanWithSourceCertificates projects existing immutable source evidence
// into the plan before stock selection and dependency planning. Missing proofs
// retain worker preparation. Store failures fail FINALIZE instead of hiding a miss.
func (s *AssetService) EnrichPlanWithSourceCertificates(ctx context.Context, planJSON string, runtimeAssets []map[string]interface{}) (string, error) {
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return "", err
	}
	profile, err := contract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return planJSON, nil
	}
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		return "", err
	}
	reader, legacyReaderOK := s.repo.(assetSourceAssetReader)
	registry, registryOK := s.repo.(sourceCertificateRegistry)
	if !legacyReaderOK && !registryOK {
		return "", fmt.Errorf("source certificate reverse index unavailable")
	}
	bindings := stockRuntimeBindings(runtimeAssets)
	// Pool slots do not have source assets yet. Attach only certified members;
	// expansion will retain their exact source evidence rather than invent it.
	added := make(map[string]bool)
	if plan.StockPool != nil {
		existing := make(map[string]bool)
		for _, asset := range plan.Assets {
			existing[asset.AssetID] = true
		}
		for _, binding := range runtimeAssets {
			member, _ := binding["stock_pool_member"].(bool)
			drive := bindingString(binding, "drive_file_id")
			if !member || drive == "" || existing["stockpool:"+drive] {
				continue
			}
			plan.Assets = append(plan.Assets, contract.AssetRefV2{AssetID: "stockpool:" + drive, AssetKey: drive, SourceKind: "drive", DriveFileID: drive, Kind: "video", MIME: "video/mp4", SHA256: bindingString(binding, "source_sha256", "sha256", "asset_sha256"), SizeBytes: bindingInt64(binding, "size_bytes")})
			existing["stockpool:"+drive] = true
			added["stockpool:"+drive] = true
		}
	}
	changed := false
	for i := range plan.Assets {
		asset := &plan.Assets[i]
		if asset.Kind != "video" {
			continue
		}
		binding := stockRuntimeBindingForAsset(*asset, bindings)
		runtimeSHA := bindingString(binding, "source_sha256", "sha256", "asset_sha256")
		if validStockSourceSHA256(asset.SHA256) && validStockSourceSHA256(runtimeSHA) && asset.SHA256 != runtimeSHA {
			return "", fmt.Errorf("runtime source digest conflicts with compiled asset %s", asset.AssetID)
		}
		if asset.SHA256 == "" && validStockSourceSHA256(runtimeSHA) {
			asset.SHA256 = runtimeSHA
			changed = true
		}
		runtimeSize := bindingInt64(binding, "size_bytes")
		if asset.SizeBytes > 0 && runtimeSize > 0 && asset.SizeBytes != runtimeSize {
			return "", fmt.Errorf("runtime source size conflicts with compiled asset %s", asset.AssetID)
		}
		if asset.SizeBytes <= 0 && runtimeSize > 0 {
			asset.SizeBytes = runtimeSize
			changed = true
		}
	}
	var records map[string]*AssetRecord
	var independent map[string]string
	references := make([]string, 0, len(plan.Assets))
	seenReferences := make(map[string]bool)
	for _, asset := range plan.Assets {
		if asset.Kind != "video" {
			continue
		}
		sha := asset.SHA256
		if sha == "" {
			sha = bindingString(stockRuntimeBindingForAsset(asset, bindings), "source_sha256", "sha256", "asset_sha256")
		}
		if !validStockSourceSHA256(sha) {
			continue
		}
		reference := sourceCertificateReference(sha, revision)
		if !seenReferences[reference] {
			references = append(references, reference)
			seenReferences[reference] = true
		}
	}
	if registryOK && len(references) > 0 {
		independent, err = registry.GetVideoSourceCertificates(ctx, references)
		if err != nil {
			return "", fmt.Errorf("batch lookup source certificates: %w", err)
		}
	}
	batch, batchOK := s.repo.(assetSourceAssetsReader)
	legacyReferences := references
	if registryOK {
		legacyReferences = legacyReferences[:0]
		for _, reference := range references {
			if independent[reference] == "" {
				legacyReferences = append(legacyReferences, reference)
			}
		}
	}
	if batchOK && len(legacyReferences) > 0 {
		var readErr error
		records, readErr = batch.GetBySourceReferences(ctx, legacyReferences)
		if readErr != nil {
			return "", fmt.Errorf("batch lookup source certificates: %w", readErr)
		}
	}
	retained := plan.Assets[:0]
	for _, asset := range plan.Assets {
		if asset.Kind != "video" {
			retained = append(retained, asset)
			continue
		}
		binding := stockRuntimeBindingForAsset(asset, bindings)
		sha := asset.SHA256
		if sha == "" {
			sha = bindingString(binding, "source_sha256", "sha256", "asset_sha256")
		}
		size := asset.SizeBytes
		if size <= 0 {
			size = bindingInt64(binding, "size_bytes")
		}
		if !validStockSourceSHA256(sha) || size <= 0 {
			if !added[asset.AssetID] {
				retained = append(retained, asset)
			}
			continue
		}
		ref := sourceCertificateReference(sha, revision)
		var metadataJSON string
		if registryOK {
			metadataJSON = independent[ref]
		}
		if metadataJSON == "" {
			if record := records[ref]; record != nil && record.Status == AssetStatusReady {
				metadataJSON = record.MetadataJSON
			}
		}
		if metadataJSON == "" && !batchOK && !registryOK {
			var readErr error
			var record *AssetRecord
			record, readErr = reader.GetBySourceReference(ctx, ref)
			if readErr != nil {
				return "", fmt.Errorf("lookup source certificate: %w", readErr)
			}
			if record != nil && record.Status == AssetStatusReady {
				metadataJSON = record.MetadataJSON
			}
		}
		if metadataJSON == "" {
			if !added[asset.AssetID] {
				retained = append(retained, asset)
			}
			continue
		}
		var source contract.VideoSourceCertificateV1
		if json.Unmarshal([]byte(metadataJSON), &source) != nil || source.Validate() != nil || source.SHA256 != sha || source.SizeBytes != size || source.ProfileID != profile.ProfileID || source.PreparationRevision != revision {
			if !added[asset.AssetID] {
				retained = append(retained, asset)
			}
			continue
		}
		asset.SHA256 = sha
		asset.SizeBytes = size
		asset.ProfileID = profile.StreamProfile()
		asset.Width = profile.Width
		asset.Height = profile.Height
		asset.FrameCount = source.Evidence.FrameCount
		asset.DurationUS = source.DurationUS
		asset.FirstFrameKeyframe = true
		asset.ClosedGOP = true
		asset.ExtradataSHA256 = source.Evidence.ExtradataSHA256
		asset.SourceCertificate = &source
		retained = append(retained, asset)
		changed = true
	}
	plan.Assets = retained
	if !changed {
		return planJSON, nil
	}
	// Bind segment identities to the certified source bytes. Conflicting declared
	// identities are never silently replaced.
	byID := make(map[string]contract.AssetRefV2)
	for _, asset := range plan.Assets {
		byID[asset.AssetID] = asset
	}
	for ti := range plan.VideoTracks {
		for si := range plan.VideoTracks[ti].Segments {
			segment := &plan.VideoTracks[ti].Segments[si]
			if asset, ok := byID[segment.AssetID]; ok {
				if segment.SHA256 != "" && asset.SHA256 != "" && segment.SHA256 != asset.SHA256 {
					return "", fmt.Errorf("source certificate conflicts with segment %s identity", segment.SegmentID)
				}
				if segment.SHA256 == "" {
					segment.SHA256 = asset.SHA256
				}
			}
		}
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}
