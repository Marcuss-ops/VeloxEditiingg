package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"velox-shared/controltransport"
	"velox-worker-agent/pkg/config"
)

func testWorkerCfg(t *testing.T) *config.WorkerConfig {
	t.Helper()
	cfg := newInsecureDevCfg(t)
	// StateDir must be writable by the test user: the /tmp/velox-worker
	// default may be owned by another account in shared dev environments.
	cfg.StateDir = t.TempDir()
	return cfg
}

func TestLoadReleaseIdentity_FromBuildInfoAndEnv(t *testing.T) {
	workDir := t.TempDir()
	remoteCodexDir := filepath.Join(workDir, "RemoteCodex")
	require.NoError(t, os.MkdirAll(remoteCodexDir, 0o755))

	// Byte-match the shape scripts/generate-build-info.sh emits: capability_schema
	// is a JSON NUMBER (unquoted). A quoted string here would make json.Unmarshal
	// into buildInfo.CapabilitySchema (int) fail, silently dropping the whole
	// certificate — this fixture is the regression guard for that drift.
	data, err := json.MarshalIndent(map[string]interface{}{
		"version":           "v1.2.20",
		"git_commit":        "fbbf7c1",
		"source_hash":       strings.Repeat("b", 64),
		"protocol_version":  "v3",
		"capability_schema": controltransport.CapabilitySchemaVersion,
		"engine_version":    "v1.2.20",
		"platform":          "linux",
		"arch":              "x86_64",
	}, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(remoteCodexDir, "BUILD_INFO.json"), data, 0o644))

	engineSHA := strings.Repeat("d", 64)
	engineFile := filepath.Join(t.TempDir(), "video-engine.sha256")
	require.NoError(t, os.WriteFile(engineFile, []byte(engineSHA+"\n"), 0o644))

	digest := strings.Repeat("a", 64)
	t.Setenv("VELOX_WORKER_IMAGE", "ghcr.io/marcuss-ops/velox-worker@sha256:"+digest)
	t.Setenv(engineSHAFileEnv, engineFile)

	cfg := testWorkerCfg(t)
	cfg.WorkDir = workDir
	cfg.BundleHash = strings.Repeat("c", 64)
	cfg.ProtocolVersion = "v3"

	w, err := New(cfg, "v1.2.20")
	require.NoError(t, err)

	ri := w.loadReleaseIdentity()
	require.NoError(t, ri.Validate())
	require.Equal(t, digest, ri.ImageDigest)
	require.Equal(t, "fbbf7c1", ri.SourceCommit)
	require.Equal(t, strings.Repeat("b", 64), ri.SourceHash)
	require.Equal(t, strings.Repeat("c", 64), ri.BundleHash)
	require.Equal(t, engineSHA, ri.EngineSHA256)
	require.Equal(t, "v1.2.20", ri.SoftwareVersion)
	require.Equal(t, "v3", ri.ProtocolVersion)
	require.Equal(t, controltransport.CapabilitySchemaVersion, ri.CapabilitySchema)
}

// TestLoadReleaseIdentity_QuotedCapabilitySchemaFailsClosed pins the
// string-vs-int JSON drift: a quoted "capability_schema" makes the whole
// BUILD_INFO.json unparseable, so the certificate must NOT silently lose its
// source hash/commit — the file read fails closed and buildReleaseIdentity
// still publishes the config-sourced fields.
func TestLoadReleaseIdentity_QuotedCapabilitySchemaFailsClosed(t *testing.T) {
	workDir := t.TempDir()
	remoteCodexDir := filepath.Join(workDir, "RemoteCodex")
	require.NoError(t, os.MkdirAll(remoteCodexDir, 0o755))

	// Deliberately WRONG shape: capability_schema as a JSON string.
	data := []byte(`{
  "version": "v1.2.20",
  "git_commit": "fbbf7c1",
  "source_hash": "` + strings.Repeat("b", 64) + `",
  "protocol_version": "v3",
  "capability_schema": "1"
}`)
	require.NoError(t, os.WriteFile(filepath.Join(remoteCodexDir, "BUILD_INFO.json"), data, 0o644))

	cfg := testWorkerCfg(t)
	cfg.WorkDir = workDir
	cfg.BundleHash = strings.Repeat("c", 64)
	cfg.ProtocolVersion = "v3"
	w, err := New(cfg, "v1.2.20")
	require.NoError(t, err)

	ri := w.loadReleaseIdentity()
	// The corrupt file must not poison the certificate: config-sourced fields
	// survive, but the build-info-sourced commit/hash are absent.
	require.Equal(t, "", ri.SourceCommit, "corrupt BUILD_INFO.json must not supply fields")
	require.Equal(t, "", ri.SourceHash)
	require.Equal(t, strings.Repeat("c", 64), ri.BundleHash)
	require.Equal(t, "v1.2.20", ri.SoftwareVersion)
}

func TestCapabilitiesMap_PublishesReleaseIdentityBlock(t *testing.T) {
	workDir := t.TempDir()
	cfg := testWorkerCfg(t)
	cfg.WorkDir = workDir
	cfg.BundleHash = strings.Repeat("c", 64)
	cfg.ProtocolVersion = "v3"

	w, err := New(cfg, "v1.2.20")
	require.NoError(t, err)

	caps := w.capabilitiesMap("test-host")
	_, ok := caps[controltransport.CapabilityReleaseIdentityKey].(map[string]interface{})
	require.True(t, ok, "release_identity block must be published")

	ri, ok := controltransport.ReleaseIdentityFromCapabilities(caps)
	require.True(t, ok)
	require.NoError(t, ri.Validate())
	require.Equal(t, strings.Repeat("c", 64), ri.BundleHash)
	require.Equal(t, "v1.2.20", ri.SoftwareVersion)
	require.Equal(t, "v3", ri.ProtocolVersion)
	require.Equal(t, controltransport.CapabilitySchemaVersion, ri.CapabilitySchema)

	// Flat legacy keys must be present for the master snapshot columns.
	require.Equal(t, "", caps[controltransport.CapabilityKeyGitSHA], "no build info on disk -> empty git_sha")

	// The canonical block must carry bundle_hash (the top-level Hello
	// BundleHash field is a separate, pre-existing wire field).
	require.Equal(t, strings.Repeat("c", 64), blockMap(caps)["bundle_hash"], "canonical block bundle_hash")
}

func blockMap(caps map[string]interface{}) map[string]interface{} {
	m, _ := caps[controltransport.CapabilityReleaseIdentityKey].(map[string]interface{})
	return m
}

func TestCapabilitiesMap_NoBuildInfoStaysValid(t *testing.T) {
	cfg := testWorkerCfg(t) // empty temp workdir, no BUILD_INFO.json
	w, err := New(cfg, "test")
	require.NoError(t, err)

	caps := w.capabilitiesMap("test-host")
	// Certificate with only config-sourced fields must still be published.
	ri, ok := controltransport.ReleaseIdentityFromCapabilities(caps)
	require.True(t, ok, "certificate must be published even without build info")
	require.NoError(t, ri.Validate())
	require.Equal(t, "test", ri.SoftwareVersion)
}

func TestReadEngineSHA_IgnoresNonHexTokens(t *testing.T) {
	engineFile := filepath.Join(t.TempDir(), "video-engine.sha256")
	content := "some path 0xNOTHEX " + strings.Repeat("e", 64) + "\n"
	require.NoError(t, os.WriteFile(engineFile, []byte(content), 0o644))

	t.Setenv(engineSHAFileEnv, engineFile)
	got := readEngineSHA()
	require.Equal(t, strings.Repeat("e", 64), got)
}

func TestReadBuildInfo_UnreadableFileReturnsNil(t *testing.T) {
	require.Nil(t, readBuildInfo(filepath.Join(t.TempDir(), "does-not-exist")))
}
