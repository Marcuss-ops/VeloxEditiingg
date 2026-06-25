package doctor

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"velox-worker-agent/pkg/config"
)

// ── Test helpers ───────────────────────────────────────────────────────────

func testCfg() *config.WorkerConfig {
	w := config.DefaultConfig("/tmp/velox-test")
	w.Environment = "dev"
	w.ControlGRPCURL = "localhost:9999"
	w.MasterURL = "http://localhost:9999"
	w.OutputDir = ""
	w.TempDir = ""
	w.HealthPort = 0
	w.PrometheusPort = 0
	w.MinDiskFreeMB = 1
	w.VideoEngineCppBin = ""
	w.AllowInsecureGRPC = true
	return w
}

// ── EnvironmentValidator ────────────────────────────────────────────────────

func TestEnvironment_DevOK(t *testing.T) {
	cfg := testCfg()
	cfg.Environment = "dev"
	cfg.AllowInsecureGRPC = true
	r := (&EnvironmentValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status)
}

func TestEnvironment_ProductionNoTLS_ShouldFail(t *testing.T) {
	cfg := testCfg()
	cfg.Environment = "production"
	cfg.AllowInsecureGRPC = false
	r := (&EnvironmentValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "requires full TLS triple")
}

func TestEnvironment_ProductionWithInsecure_ShouldFail(t *testing.T) {
	cfg := testCfg()
	cfg.Environment = "production"
	cfg.AllowInsecureGRPC = true
	r := (&EnvironmentValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "forbidden")
}

func TestEnvironment_ProductionWithTLS_ShouldPass(t *testing.T) {
	cfg := testCfg()
	cfg.Environment = "production"
	cfg.AllowInsecureGRPC = false
	cfg.TLSCertFile = "/fake/cert.pem"
	cfg.TLSKeyFile = "/fake/key.pem"
	cfg.TLSCAFile = "/fake/ca.pem"
	r := (&EnvironmentValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status)
}

// ── TransportTLSValidator ───────────────────────────────────────────────────

func TestTransportTLS_ValidDevInsecure(t *testing.T) {
	cfg := testCfg()
	cfg.Environment = "dev"
	cfg.AllowInsecureGRPC = true
	r := (&TransportTLSValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status)
}

func TestTransportTLS_MissingControlURL_ShouldFail(t *testing.T) {
	cfg := testCfg()
	cfg.ControlGRPCURL = ""
	cfg.MasterURL = ""
	cfg.AllowInsecureGRPC = true
	r := (&TransportTLSValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
}

// ── DirsValidator ───────────────────────────────────────────────────────────

func TestDirs_AllWritable(t *testing.T) {
	cfg := testCfg()
	tmpDir := t.TempDir()
	cfg.WorkDir = tmpDir + "/work"
	cfg.OutputDir = tmpDir + "/output"
	cfg.TempDir = tmpDir + "/temp"
	t.Setenv("VELOX_WORKER_CACHE_DIR", tmpDir+"/cache")
	t.Setenv("VELOX_WORKER_BLOB_DIR", tmpDir+"/blobs")
	r := (&DirsValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status, "detail: %s", r.Detail)
}

func TestDirs_ReadOnlyParent_ShouldFailOne(t *testing.T) {
	cfg := testCfg()
	tmpDir := t.TempDir()
	readOnlyDir := tmpDir + "/readonly"
	require.NoError(t, os.MkdirAll(readOnlyDir, 0555))
	cfg.WorkDir = readOnlyDir + "/work" // parent is read-only, MkdirAll fails
	cfg.OutputDir = tmpDir + "/output"
	cfg.TempDir = tmpDir + "/temp"
	r := (&DirsValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "work_dir")
}

// ── DiskFreeValidator ──────────────────────────────────────────────────────

func TestDiskFree_PassesWithThreshold(t *testing.T) {
	cfg := testCfg()
	tmpDir := t.TempDir()
	cfg.OutputDir = tmpDir
	cfg.MinDiskFreeMB = 1 // very low threshold, should pass
	r := (&DiskFreeValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status, "detail: %s", r.Detail)
}

func TestDiskFree_ImpossibleThreshold_ShouldFail(t *testing.T) {
	cfg := testCfg()
	tmpDir := t.TempDir()
	cfg.OutputDir = tmpDir
	cfg.MinDiskFreeMB = 100_000_000 // 100 TB, impossible
	r := (&DiskFreeValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "below threshold")
}

// ── PortsValidator ──────────────────────────────────────────────────────────

func TestPorts_FreePorts(t *testing.T) {
	cfg := testCfg()
	cfg.HealthPort = 0
	cfg.PrometheusPort = 0
	r := (&PortsValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status)
}

func TestPorts_OccupiedPort_ShouldFail(t *testing.T) {
	// Grab a port first.
	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	cfg := testCfg()
	cfg.HealthPort = port
	r := (&PortsValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "health_port")
}

// ── EngineBinaryValidator ───────────────────────────────────────────────────

func TestEngineBinary_Missing_ShouldFail(t *testing.T) {
	cfg := testCfg()
	cfg.VideoEngineCppBin = "/nonexistent/path/nosuchbinary"
	r := (&EngineBinaryValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
}

func TestEngineBinary_FoundInPath(t *testing.T) {
	cfg := testCfg()
	cfg.VideoEngineCppBin = "" // should fall back to exec.LookPath
	// If we can find any binary in PATH, use it.
	if _, err := exec.LookPath("ls"); err == nil {
		cfg.VideoEngineCppBin = "ls"
		r := (&EngineBinaryValidator{}).Run(context.Background(), cfg)
		assert.Equal(t, StatusPass, r.Status, "detail: %s", r.Detail)
	}
}

// ── FFmpegValidator ─────────────────────────────────────────────────────────

func TestFFmpeg_Missing_ShouldFail(t *testing.T) {
	// This test is environment-dependent: skip if ffmpeg is installed.
	if _, err := exec.LookPath("ffmpeg"); err == nil {
		t.Skip("ffmpeg installed, skipping failure test")
	}
	r := (&FFmpegValidator{}).Run(context.Background(), config.DefaultConfig("/tmp"))
	assert.Equal(t, StatusFail, r.Status)
}

func TestFFmpeg_Found_SmokeOK(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not installed")
	}
	r := (&FFmpegValidator{}).Run(context.Background(), config.DefaultConfig("/tmp"))
	// May pass or fail depending on ffmpeg version — just test it doesn't panic.
	t.Logf("ffmpeg validator: %s — %s", r.Status, r.Detail)
}

// ── RegistryValidator ──────────────────────────────────────────────────────

type stubRegistry struct{ descs []DescriptorView }

func (s *stubRegistry) Descriptors() []DescriptorView { return s.descs }

func TestRegistry_Empty_ShouldFail(t *testing.T) {
	v := &RegistryValidator{Registry: &stubRegistry{descs: nil}}
	r := v.Run(context.Background(), testCfg())
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "empty")
}

func TestRegistry_OneExecutor_SceneCompositePresent(t *testing.T) {
	v := &RegistryValidator{Registry: &stubRegistry{
		descs: []DescriptorView{{ID: "scene.composite.v1", Version: 1}},
	}}
	r := v.Run(context.Background(), testCfg())
	assert.Equal(t, StatusPass, r.Status)
}

func TestRegistry_OneExecutor_SceneCompositeMissing(t *testing.T) {
	v := &RegistryValidator{Registry: &stubRegistry{
		descs: []DescriptorView{{ID: "other.executor", Version: 2}},
	}}
	r := v.Run(context.Background(), testCfg())
	assert.Equal(t, StatusFail, r.Status)
	assert.Contains(t, r.Detail, "missing")
}

// ── CertExpiryValidator ─────────────────────────────────────────────────────

func TestCertExpiry_NoCert(t *testing.T) {
	cfg := testCfg()
	r := (&CertExpiryValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusPass, r.Status, "should skip when no cert configured")
}

func TestCertExpiry_InvalidCertPath(t *testing.T) {
	cfg := testCfg()
	cfg.TLSCertFile = "/nonexistent/cert.pem"
	cfg.TLSKeyFile = "/nonexistent/key.pem"
	r := (&CertExpiryValidator{}).Run(context.Background(), cfg)
	assert.Equal(t, StatusFail, r.Status)
}

// ── Report / Run integration ────────────────────────────────────────────────

func TestRun_AllPass(t *testing.T) {
	cfg := testCfg()
	tmpDir := t.TempDir()
	cfg.WorkDir = tmpDir + "/w"
	cfg.OutputDir = tmpDir + "/o"
	cfg.TempDir = tmpDir + "/t"
	cfg.HealthPort = 0
	cfg.PrometheusPort = 0
	cfg.MinDiskFreeMB = 1
	t.Setenv("VELOX_WORKER_CACHE_DIR", tmpDir+"/cache")
	t.Setenv("VELOX_WORKER_BLOB_DIR", tmpDir+"/blob")

	var buf strings.Builder
	validators := []Validator{
		&EnvironmentValidator{},
		&DirsValidator{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := Run(ctx, cfg, validators, &buf)
	assert.NoError(t, err)

	var report Report
	require.NoError(t, json.Unmarshal([]byte(buf.String()), &report))
	assert.Equal(t, VerdictReady, report.Verdict)
	assert.Len(t, report.Checks, 2)
	for _, c := range report.Checks {
		assert.Equal(t, StatusPass, c.Status, "check %s: %s", c.ID, c.Detail)
	}
}

func TestRun_WithFail(t *testing.T) {
	cfg := testCfg()
	cfg.VideoEngineCppBin = "/nonexistent/velox-render-cpp"

	var buf strings.Builder
	validators := []Validator{
		&EnvironmentValidator{},
		&EngineBinaryValidator{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := Run(ctx, cfg, validators, &buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_READY")

	var report Report
	require.NoError(t, json.Unmarshal([]byte(buf.String()), &report))
	assert.Equal(t, VerdictNotReady, report.Verdict)
	assert.Len(t, report.Checks, 2)
}

// ── DefaultValidators / DefaultValidatorsWithRegistry ───────────────────────

func TestDefaultValidators_Length(t *testing.T) {
	vals := DefaultValidators()
	assert.Len(t, vals, 9, "should return 9 validators (all except registry)")
}

func TestDefaultValidatorsWithRegistry(t *testing.T) {
	r := &stubRegistry{descs: []DescriptorView{{ID: "scene.composite.v1", Version: 1}}}
	vals := DefaultValidatorsWithRegistry(r)
	assert.Len(t, vals, 10, "should return 10 validators (including registry)")
}

func TestDefaultValidatorsWithRegistry_Nil(t *testing.T) {
	vals := DefaultValidatorsWithRegistry(nil)
	assert.Len(t, vals, 9, "nil registry → 9 validators")
}
