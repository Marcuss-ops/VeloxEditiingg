package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/runtimeassets"
)

// resolveCompiledRenderPlanAssets is an adapter/orchestrator around the
// existing common asset resolver. It deliberately does not download bytes or
// write cache rows itself: resolveCommonAssetPayload owns that verified path.
//
// The synthetic payload exists only in memory so the common resolver can
// consume the typed V2 references. Local paths returned by that resolver are
// projected into runtimeassets.Bindings and never written back into the
// canonical CompiledRenderPlanV2 JSON.
func (w *Worker) resolveCompiledRenderPlanAssets(ctx context.Context, jobID string, payload map[string]interface{}) (runtimeassets.Bindings, error) {
	if payload == nil {
		return nil, fmt.Errorf("compiled render plan v2: payload is required")
	}
	plan, err := contract.DecodeCompiledRenderPlanV2Payload(payload)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: decode: %w", err)
	}
	if plan == nil {
		return nil, fmt.Errorf("compiled render plan v2: %q is required", contract.PayloadKeyCompiledRenderPlanJSON)
	}

	// This structure is an adapter input, not a replacement plan. The
	// resolver understands the existing asset envelope and returns verified
	// worker-local paths for each uri. AssetKey is the canonical cache/transfer
	// identity when supplied; AssetID remains the stable plan/binding identity.
	planAssets := append([]contract.AssetRefV2(nil), plan.Assets...)
	// Folder members are a Master-resolved runtime snapshot, not editorial
	// per-file assignments in the immutable plan. Resolve them through the same
	// verified cache path as every other V2 asset.
	var runtimePool []struct {
		AssetID         string `json:"asset_id"`
		DriveFileID     string `json:"drive_file_id"`
		SizeBytes       int64  `json:"size_bytes"`
		DurationMS      int64  `json:"duration_ms"`
		StockPoolMember bool   `json:"stock_pool_member"`
	}
	if raw, marshalErr := json.Marshal(payload["runtime_assets"]); marshalErr == nil {
		_ = json.Unmarshal(raw, &runtimePool)
	}
	if plan.StockPool != nil {
		var framesNeeded int64
		for _, track := range plan.VideoTracks {
			for _, segment := range track.Segments {
				if segment.StockPoolSlot {
					framesNeeded += segment.FrameCount
				}
			}
		}
		if framesNeeded <= 0 {
			return nil, fmt.Errorf("compiled render plan v2: stock_pool has no positive slot duration")
		}
		pool := make([]struct {
			AssetID         string `json:"asset_id"`
			DriveFileID     string `json:"drive_file_id"`
			SizeBytes       int64  `json:"size_bytes"`
			DurationMS      int64  `json:"duration_ms"`
			StockPoolMember bool   `json:"stock_pool_member"`
		}, 0, len(runtimePool))
		for _, item := range runtimePool {
			if item.StockPoolMember && item.DurationMS > 0 && item.SizeBytes > 0 && strings.TrimSpace(item.DriveFileID) != "" {
				pool = append(pool, item)
			}
		}
		// Drive listing order is not stable, so canonicalize it before applying
		// the same per-job seed used by the slice executor. Retries then resolve
		// the same subset of pool members before any bytes are downloaded.
		sort.Slice(pool, func(i, j int) bool { return pool[i].DriveFileID < pool[j].DriveFileID })
		seedBytes := sha256.Sum256([]byte(jobID + ":" + plan.TimelineSHA256 + ":" + plan.StockPool.DriveFolderID))
		rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(seedBytes[:8])))).Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
		var framesAvailable int64
		for _, item := range pool {
			if framesAvailable >= framesNeeded {
				break
			}
			if !item.StockPoolMember || strings.TrimSpace(item.DriveFileID) == "" {
				continue
			}
			frames := item.DurationMS * int64(plan.Output.FPSNum) / (1000 * int64(plan.Output.FPSDen))
			if frames <= 0 {
				continue
			}
			planAssets = append(planAssets, contract.AssetRefV2{
				AssetID: "stockpool:" + item.DriveFileID, AssetKey: item.DriveFileID,
				SourceKind: "drive", DriveFileID: item.DriveFileID,
				Kind: "video", SizeBytes: item.SizeBytes,
			})
			framesAvailable += frames
		}
		if framesAvailable < framesNeeded {
			return nil, fmt.Errorf("compiled render plan v2: stock_pool duration metadata covers %d of %d required frames", framesAvailable, framesNeeded)
		}
	}
	assetEnvelopes := make([]interface{}, 0, len(planAssets))
	assetCacheKeys := make([]string, 0, len(planAssets))
	seenIdentities := make(map[string]string, len(planAssets))
	runtimeDriveIDs := compiledRuntimeDriveFileIDs(payload["runtime_assets"])
	runtimeDriveSources := compiledRuntimeDriveSourceURIs(payload["runtime_assets"])
	for _, asset := range planAssets {
		identity := compiledAssetIdentity(asset)
		if identity == "" {
			return nil, fmt.Errorf("compiled render plan v2: asset %q has no canonical identity", asset.AssetID)
		}
		if previous, exists := seenIdentities[identity]; exists && previous != asset.AssetID {
			return nil, fmt.Errorf("compiled render plan v2: assets %q and %q share canonical identity %q", previous, asset.AssetID, identity)
		}
		seenIdentities[identity] = asset.AssetID
		uri := compiledAssetReference(asset)
		driveID := strings.TrimSpace(asset.DriveFileID)
		if driveID == "" {
			driveID = runtimeDriveIDs[strings.TrimSpace(asset.AssetID)]
		}
		if driveID == "" {
			driveID = runtimeDriveIDs[strings.TrimSpace(asset.AssetKey)]
		}
		if asset.SourceKind == "drive" || driveID != "" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(asset.AssetID)), "drive:") {
			if driveID == "" && strings.HasPrefix(strings.ToLower(asset.AssetID), "drive:") {
				driveID = strings.TrimSpace(asset.AssetID[len("drive:"):])
			}
			if driveID == "" {
				return nil, fmt.Errorf("compiled render plan v2: Drive asset %q has no drive_file_id", asset.AssetID)
			}
			// Keep the plan's canonical asset identity as the cache key. The
			// Drive file ID is only a locator; replacing the identity with it
			// makes the later lease reserve a different row than the Master
			// certified under asset_id.
			uri = "velox-drive://" + identity
			sourceURI := firstNonBlank(
				runtimeDriveSources[strings.TrimSpace(asset.AssetID)],
				runtimeDriveSources[strings.TrimSpace(asset.AssetKey)],
				runtimeDriveSources[driveID],
			)
			if sourceURI == "" {
				sourceURI = "https://drive.google.com/uc?export=download&id=" + url.QueryEscape(driveID)
			}
			w.rememberSourceLocator(identity, sourceURI)
		}
		assetCacheKeys = append(assetCacheKeys, identity)
		assetEnvelopes = append(assetEnvelopes, map[string]interface{}{
			"asset_id":   identity,
			"uri":        uri,
			"kind":       asset.Kind,
			"sha256":     asset.SHA256,
			"size_bytes": asset.SizeBytes,
		})
	}
	// The common resolver walks slices serially. Resolve each plan item through
	// that same resolver concurrently so the downloader's bounded pool is
	// actually used for V2 plans. The manager still coalesces duplicate keys;
	// the result slice remains ordered and every item keeps the same validation
	// and integrity-verification path as before.
	resolvedAssets := make([]interface{}, len(assetEnvelopes))
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	const maxConcurrentResolves = 8
	sem := make(chan struct{}, maxConcurrentResolves)
	for index, envelope := range assetEnvelopes {
		index, envelope := index, envelope
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			resolvedPayload, resolveErr := w.resolveCommonAssetPayloadMutable(ctx, map[string]interface{}{
				"assets": []interface{}{envelope},
			})
			if resolveErr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("asset %d: %w", index, resolveErr)
				}
				errMu.Unlock()
				return
			}
			items, ok := resolvedPayload["assets"].([]interface{})
			if !ok || len(items) != 1 {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("asset %d: resolver returned invalid result", index)
				}
				errMu.Unlock()
				return
			}
			resolvedAssets[index] = items[0]
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, fmt.Errorf("compiled render plan v2: resolve assets: %w", firstErr)
	}
	bindings := make(runtimeassets.Bindings, len(planAssets))
	for index, asset := range planAssets {
		resolved, ok := resolvedAssets[index].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("compiled render plan v2: resolved assets[%d] is %T, want object", index, resolvedAssets[index])
		}
		path, ok := resolved["uri"].(string)
		if !ok || strings.TrimSpace(path) == "" || strings.HasPrefix(path, "velox-") {
			return nil, fmt.Errorf("compiled render plan v2: assets[%d] did not resolve to a local path", index)
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			if err == nil {
				err = fmt.Errorf("file is empty or not regular")
			}
			return nil, fmt.Errorf("compiled render plan v2: assets[%d] local path invalid: %w", index, err)
		}
		sha := asset.SHA256
		if resolvedSHA, ok := resolved["sha256"].(string); ok && strings.TrimSpace(resolvedSHA) != "" {
			sha = strings.TrimPrefix(strings.TrimSpace(resolvedSHA), "sha256:")
		}
		size := asset.SizeBytes
		if resolvedSize, ok := resolved["size_bytes"].(float64); ok && resolvedSize > 0 {
			size = int64(resolvedSize)
		}
		if info.Size() > 0 {
			size = info.Size()
		}
		if (asset.SourceKind == "drive" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(asset.AssetID)), "drive:")) && (sha == "" || size <= 0) {
			driveID := strings.TrimSpace(asset.DriveFileID)
			if driveID == "" && strings.HasPrefix(strings.ToLower(asset.AssetID), "drive:") {
				driveID = strings.TrimSpace(asset.AssetID[len("drive:"):])
			}
			if remembered, ok := w.rememberedAssetIntegrity(driveID); ok {
				sha = remembered.SHA256
				size = remembered.SizeBytes
			}
			if sha == "" || size <= 0 {
				computedSHA, computedSize, hashErr := hashLocalAsset(path)
				if hashErr != nil {
					return nil, fmt.Errorf("compiled render plan v2: assets[%d] could not verify downloaded Drive bytes: %w", index, hashErr)
				}
				sha, size = computedSHA, computedSize
			}
		}
		if sha == "" || size <= 0 {
			return nil, fmt.Errorf("compiled render plan v2: assets[%d] did not produce worker-verified SHA-256/size", index)
		}
		bindings[asset.AssetID] = runtimeassets.Binding{
			AssetID:  asset.AssetID,
			CacheKey: assetCacheKeys[index],
			Path:     path,
			SHA256:   sha,
			Size:     size,
			Verified: true,
		}
	}
	return bindings, nil
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func hashLocalAsset(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	if size <= 0 {
		return "", 0, fmt.Errorf("downloaded file is empty")
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// compiledRuntimeDriveFileIDs joins the Producer's runtime transport manifest
// to immutable V2 plan identities. The plan SHA remains untouched; the
// runtime manifest supplies only the Drive file locator used to fetch it.
func compiledRuntimeDriveFileIDs(raw interface{}) map[string]string {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return nil
	}
	var assets []struct {
		AssetID     string `json:"asset_id"`
		AssetKey    string `json:"asset_key"`
		DriveFileID string `json:"drive_file_id"`
		URL         string `json:"url"`
		SourceURI   string `json:"source_uri"`
	}
	if err := json.Unmarshal(encoded, &assets); err != nil {
		return nil
	}
	byIdentity := make(map[string]string, len(assets)*2)
	for _, asset := range assets {
		fileID := strings.TrimSpace(asset.DriveFileID)
		if fileID == "" {
			for _, locator := range []string{asset.URL, asset.SourceURI} {
				u, parseErr := url.Parse(strings.TrimSpace(locator))
				if parseErr != nil || !strings.EqualFold(u.Scheme, "velox-drive") {
					continue
				}
				fileID = strings.TrimSpace(u.Host)
				if fileID == "" {
					fileID = strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
				}
				if fileID != "" {
					break
				}
			}
		}
		if fileID == "" {
			continue
		}
		for _, identity := range []string{asset.AssetID, asset.AssetKey} {
			if identity = strings.TrimSpace(identity); identity != "" {
				byIdentity[identity] = fileID
			}
		}
	}
	return byIdentity
}

// compiledRuntimeDriveSourceURIs joins the runtime transport manifest to the
// canonical plan identities without changing those identities. The
// downloader applies its HTTPS host allowlist and redirect checks when the
// locator is consumed.
func compiledRuntimeDriveSourceURIs(raw interface{}) map[string]string {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return nil
	}
	var assets []struct {
		AssetID     string `json:"asset_id"`
		AssetKey    string `json:"asset_key"`
		DriveFileID string `json:"drive_file_id"`
		SourceURI   string `json:"source_uri"`
	}
	if err := json.Unmarshal(encoded, &assets); err != nil {
		return nil
	}
	byIdentity := make(map[string]string, len(assets)*3)
	for _, asset := range assets {
		sourceURI := strings.TrimSpace(asset.SourceURI)
		if sourceURI == "" {
			continue
		}
		for _, identity := range []string{asset.AssetID, asset.AssetKey, asset.DriveFileID} {
			if identity = strings.TrimSpace(identity); identity != "" {
				byIdentity[identity] = sourceURI
			}
		}
	}
	return byIdentity
}

// compiledAssetIdentity is the one V2-to-cache identity rule. AssetKey is
// optional for backwards-compatible plans; when omitted, AssetID is the
// canonical cache key as well as the runtime binding key.
func compiledAssetIdentity(asset contract.AssetRefV2) string {
	identity := strings.TrimSpace(asset.AssetKey)
	if identity == "" {
		identity = strings.TrimSpace(asset.AssetID)
	}
	if wireID, ok := assetref.WireAssetID(identity); ok {
		return wireID
	}
	return identity
}

func compiledAssetReference(asset contract.AssetRefV2) string {
	raw := strings.TrimSpace(asset.AssetKey)
	if raw == "" {
		raw = strings.TrimSpace(asset.AssetID)
	}
	if _, ok := assetref.WireAssetID(raw); ok {
		return raw
	}
	return assetref.SchemeVeloxAsset + "://" + compiledAssetIdentity(asset)
}
