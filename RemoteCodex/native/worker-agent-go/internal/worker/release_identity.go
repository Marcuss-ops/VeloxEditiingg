package worker

// release_identity.go — worker-side construction of the canonical
// ReleaseIdentity certificate.
//
// The certificate is generated once in CI (worker-image.yml baseline
// manifest), baked into the image (BUILD_INFO.json + video-engine.sha256 +
// BUNDLE_HASH.txt), and reconstructed here at runtime so the worker can
// advertise it at register/hello time and keep it fresh on heartbeat.
//
// Sources (highest precedence first):
//   - ImageDigest      ← VELOX_WORKER_IMAGE env (published @sha256 ref)
//   - SourceCommit     ← BUILD_INFO.json "git_commit"
//   - SourceHash       ← BUILD_INFO.json "source_hash"
//   - BundleHash       ← cfg.BundleHash (VELOX_BUNDLE_HASH / BUNDLE_HASH.txt)
//   - EngineSHA256     ← VELOX_VIDEO_ENGINE_SHA_FILE (video-engine.sha256)
//   - SoftwareVersion  ← resolved version (w.version / VERSION.txt)
//   - ProtocolVersion  ← cfg.ProtocolVersion
//   - CapabilitySchema ← controltransport.CapabilitySchemaVersion
//
// Missing optional files degrade to empty fields (certificate stays valid);
// the master fails closed only when it must compare hashes for a rollout.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/controltransport"
)

// buildInfoFile is the canonical on-disk build certificate inside the image.
const buildInfoFile = "BUILD_INFO.json"

// engineSHAFileEnv overrides the default engine SHA-256 metadata path.
const engineSHAFileEnv = "VELOX_VIDEO_ENGINE_SHA_FILE"

// defaultEngineSHAFile is the path baked by the canonical Dockerfile.
const defaultEngineSHAFile = "/usr/local/share/velox/video-engine.sha256"

// loadReleaseIdentity assembles the release certificate from the worker
// configuration, build metadata on disk, and the environment. It never
// fails: unreadable optional sources leave the matching field empty so the
// worker can still register and advertise what it does know.
//
// The certificate is immutable for the process lifetime (image-baked files
// + config + env never change), so it is assembled once and cached; every
// capabilitiesMap call (hello + every heartbeat) reads the cached value.
func (w *Worker) loadReleaseIdentity() controltransport.ReleaseIdentity {
	w.releaseIdentityOnce.Do(func() {
		w.releaseIdentity = w.buildReleaseIdentity()
	})
	return w.releaseIdentity
}

func (w *Worker) buildReleaseIdentity() controltransport.ReleaseIdentity {
	ri := controltransport.ReleaseIdentity{
		BundleHash:       strings.TrimSpace(w.config.BundleHash),
		SoftwareVersion:  strings.TrimSpace(w.version),
		ProtocolVersion:  strings.TrimSpace(w.config.ProtocolVersion),
		CapabilitySchema: controltransport.CapabilitySchemaVersion,
	}

	if info := readBuildInfo(w.config.WorkDir); info != nil {
		ri.SourceCommit = strings.TrimSpace(info.GitCommit)
		ri.SourceHash = strings.TrimSpace(info.SourceHash)
		if ri.SoftwareVersion == "" {
			ri.SoftwareVersion = strings.TrimSpace(info.Version)
		}
		if ri.ProtocolVersion == "" {
			ri.ProtocolVersion = strings.TrimSpace(info.ProtocolVersion)
		}
		// BUILD_INFO.json is the single certificate source generated in CI;
		// it wins over the package constant when the build stamped it.
		if info.CapabilitySchema > 0 {
			ri.CapabilitySchema = info.CapabilitySchema
		}
	}

	ri.EngineSHA256 = readEngineSHA()

	if image := strings.TrimSpace(os.Getenv("VELOX_WORKER_IMAGE")); image != "" {
		if at := strings.LastIndex(image, "@"); at >= 0 {
			digest := strings.TrimPrefix(image[at+1:], "sha256:")
			ri.ImageDigest = digest
		}
	}
	return ri
}

// buildInfo is the JSON shape of RemoteCodex/BUILD_INFO.json.
type buildInfo struct {
	Version          string `json:"version"`
	GitCommit        string `json:"git_commit"`
	SourceHash       string `json:"source_hash"`
	ProtocolVersion  string `json:"protocol_version"`
	CapabilitySchema int    `json:"capability_schema"`
}

// readBuildInfo loads BUILD_INFO.json from the canonical candidate paths,
// mirroring the version-file search the bootstrap package already uses.
func readBuildInfo(workDir string) *buildInfo {
	candidates := []string{
		filepath.Join(workDir, "RemoteCodex", buildInfoFile),
		filepath.Join(workDir, buildInfoFile),
		"/app/RemoteCodex/" + buildInfoFile,
		"/opt/velox/RemoteCodex/" + buildInfoFile,
		filepath.Join(workDir, "..", "RemoteCodex", buildInfoFile),
	}
	seen := make(map[string]bool)
	for _, path := range candidates {
		abs, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		data, err := os.ReadFile(abs)
		if err != nil {
			continue
		}
		var info buildInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		return &info
	}
	return nil
}

// readEngineSHA reads the first 64-hex token from the engine metadata file.
// The canonical image writes video-engine.sha256 at build time and verifies
// it on entrypoint; this is a read-only projection of the same evidence.
func readEngineSHA() string {
	path := strings.TrimSpace(os.Getenv(engineSHAFileEnv))
	if path == "" {
		path = defaultEngineSHAFile
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, field := range strings.Fields(string(data)) {
		field = strings.ToLower(strings.TrimSpace(field))
		if len(field) == 64 && isLowerHex(field) {
			return field
		}
	}
	return ""
}

func isLowerHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
