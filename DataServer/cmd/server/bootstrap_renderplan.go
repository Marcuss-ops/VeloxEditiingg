package main

// bootstrap_renderplan.go — Fase D composition root: wires the canonical
// master-side RenderPlanCompiler onto the gRPC placement pipeline.
//
// The adapter (assetMetadataResolver) bridges the voiceover assets registry
// to the renderplan.MetadataResolver seam. It is READ-ONLY by contract:
// only Get (asset row) and GetMediaMetadata (verified probe) are consulted,
// never blob transfer, so compile performs no prefetch/download.

import (
	"context"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/grpcserver"
	"velox-server/internal/logging"
	"velox-server/internal/renderplan"
)

// assetMetadataResolver adapts *voiceoverassets.AssetService to
// renderplan.MetadataResolver. Best-effort semantics: a missing asset or a
// missing metadata row returns partial/empty metadata (never an error that
// fails the compile); only genuine read errors propagate.
type assetMetadataResolver struct {
	svc *voiceoverassets.AssetService
}

// ResolveAssetMetadata returns the registry description of a local asset:
// sha256 + kind/mime/size from the asset row, media description from the
// verified asset_media_metadata row when present.
func (r *assetMetadataResolver) ResolveAssetMetadata(ctx context.Context, assetID string) (renderplan.AssetMetadata, error) {
	meta := renderplan.AssetMetadata{AssetID: assetID}
	if r == nil || r.svc == nil {
		return meta, nil
	}
	asset, err := r.svc.Get(ctx, assetID)
	if err != nil {
		return meta, err
	}
	if asset == nil {
		return meta, nil
	}
	meta.SHA256 = asset.SHA256
	meta.Kind = asset.Kind
	meta.MimeType = asset.MimeType
	meta.SizeBytes = asset.SizeBytes
	if media, mediaErr := r.svc.GetMediaMetadata(ctx, assetID); mediaErr == nil && media != nil {
		meta.DurationMs = media.DurationMs
		meta.Width = media.Width
		meta.Height = media.Height
	}
	return meta, nil
}

// wireRenderPlanCompiler installs the canonical compiler on the gRPC handler
// when the asset registry is available. Asset metadata enrichment is
// best-effort by design; a missing AssetService disables the stamp entirely.
func wireRenderPlanCompiler(grpcHandler *grpcserver.Handler, c *appComponents) {
	if grpcHandler == nil || c == nil || c.modules == nil || c.modules.AssetService == nil {
		return
	}
	grpcHandler.SetRenderPlanCompiler(renderplan.NewCompiler(renderplan.Options{
		MetadataResolver: &assetMetadataResolver{svc: c.modules.AssetService},
	}))
	logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] wired RenderPlanCompiler (Fase D) on gRPC handler (plan_sha256 stamped at claim)")
}
