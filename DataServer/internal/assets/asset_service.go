// asset_service.go is intentionally a shell. The original
// asset_service.go (mixing two domains — asset registration/staging
// and payload rewrite collectors/applicators) has been redistributed
// across these focused files, each with a single concern:
//
//	service.go                 - entry point: AssetRepository and
//	                            BlobStore interfaces, AssetService
//	                            struct + NewAssetService, plus the
//	                            read-side methods (Get, LinkToJob)
//	                            and the AssetRecord→Asset conversion
//	                            helper.
//	registration.go            - ResolveAndRegister: the single
//	                            registration pipeline (resolve via
//	                            the registry → stage bytes → SHA-256
//	                            → dedup → promote → insert → insert
//	                            source).
//	payload_rewrite.go         - the shared applyRewrite orchestrator
//	                            plus RewriteVoiceoverPayload and
//	                            RewriteSceneImagePayload entry points.
//	rewrite_voiceover.go       - voiceover-specific pure collector
//	                            and applicator (no resolver, no
//	                            DB, no BlobStore).
//	rewrite_scene_images.go    - scene-image-specific pure collector
//	                            and applicator (no resolver, no
//	                            DB, no BlobStore).
//	media_extension.go         - extensionFromName: media-extension
//	                            inference used by ResolveAndRegister.
//
// Single-instance contract enforced by this split:
//   - ResolverRegistry remains defined only in registry.go.
//   - ResolveAndRegister exists only on *AssetService, only in
//     registration.go.
//   - applyRewrite exists only on *AssetService, only in
//     payload_rewrite.go.
//   - The voiceover and scene-image collectors/applicators each
//     appear exactly once and live next to each other (no
//     duplicated resolver logic between the two rewrite files).
//
// Pure structural refactor: zero behaviour, schema, API, or test
// surface change. Every error-wrap message is preserved verbatim;
// the public surface (AssetRepository, BlobStore, AssetService,
// NewAssetService, ResolveAndRegister, Get, LinkToJob,
// RewriteVoiceoverPayload, RewriteSceneImagePayload) is unchanged.
package assets
