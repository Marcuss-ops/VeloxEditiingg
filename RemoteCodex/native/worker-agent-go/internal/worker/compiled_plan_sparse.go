package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/internal/mediarange"
	"velox-worker-agent/internal/runtimeassets"
)

// Prefer a complete warm source. Only a cold HTTP source with authenticated
// block evidence can become a private sparse assembly input.
func (w *Worker) resolveCertifiedSparseAsset(ctx context.Context, plan *contract.CompiledRenderPlanV2, asset contract.AssetRefV2, sourceURL string) (runtimeassets.Binding, bool, error) {
	certificate := asset.SourceCertificate
	if asset.Kind != "video" || certificate == nil || certificate.Chunks == nil || certificate.Validate() != nil || asset.SizeBytes <= 2*contract.VideoSourceChunkSize || strings.TrimSpace(sourceURL) == "" {
		return runtimeassets.Binding{}, false, nil
	}
	// Use the same direct-source host allowlist and redirect policy as the
	// complete asset transferer. Sparse ranges must not create a weaker network
	// boundary than the full verified download path.
	transferer := &masterAssetTransferer{w: w}
	fetchURL, _, client, err := transferer.assetTransferRequestForSource(asset.AssetID, sourceURL)
	if err != nil {
		return runtimeassets.Binding{}, false, nil
	}
	if path, err := cachedAssetPath(w.assetCacheDir(), asset.SHA256, asset.SizeBytes); err == nil && path != "" {
		return runtimeassets.Binding{}, false, nil
	}
	var windows []mediarange.Window
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if segment.AssetID != asset.AssetID {
				continue
			}
			if !contract.PacketCopySourceWindowCertified(plan, segment, asset) {
				return runtimeassets.Binding{}, false, nil
			}
			windows = append(windows, mediarange.Window{StartUS: segment.SourceInUS, DurationUS: segment.SourceDurationUS})
		}
	}
	if len(windows) == 0 {
		return runtimeassets.Binding{}, false, nil
	}
	root := filepath.Join(w.assetCacheDir(), "sparse-inputs")
	if err := os.MkdirAll(root, 0750); err != nil {
		return runtimeassets.Binding{}, false, nil
	}
	directory, err := os.MkdirTemp(root, "source-*")
	if err != nil {
		return runtimeassets.Binding{}, false, nil
	}
	path := filepath.Join(directory, "source.mp4")
	_, err = mediarange.FetchWindow(ctx, client, fetchURL, mediarange.Options{Size: asset.SizeBytes, Windows: windows, Dest: path, Integrity: certificate.Chunks})
	if err != nil {
		os.RemoveAll(directory)
		if ctx.Err() != nil {
			return runtimeassets.Binding{}, false, ctx.Err()
		}
		// The canonical resolver remains authoritative on unsupported layouts,
		// ignored ranges or changed responses: its full SHA check must succeed.
		return runtimeassets.Binding{}, false, nil
	}
	return runtimeassets.Binding{AssetID: asset.AssetID, Path: path, SHA256: asset.SHA256, Size: asset.SizeBytes, Verified: true, Sparse: true, VerifiedRanges: true}, true, nil
}

func cleanupSparsePlanBindings(bindings runtimeassets.Bindings) {
	for _, binding := range bindings {
		if binding.Sparse && binding.VerifiedRanges {
			os.RemoveAll(filepath.Dir(binding.Path))
		}
	}
}
