package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const defaultTestThreshold = 64 * 1024 * 1024 // 64 MiB

func testConfig(tmpfs bool) Config {
	cfg := Config{
		CacheDir:            "/nvme/cache",
		TempDir:             "/nvme/temp",
		ArtifactDir:         "/nvme/artifact",
		TmpfsThresholdBytes: defaultTestThreshold,
	}
	if tmpfs {
		cfg.TmpfsDir = "/dev/shm/velox-worker"
	}
	return cfg
}

func mustResolver(t *testing.T, cfg Config) *Resolver {
	t.Helper()
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	return r
}

// ── construction / fail-closed validation ─────────────────────────────────

func TestNew_RequiresAllNvmeBackings(t *testing.T) {
	base := testConfig(false)
	cases := map[string]Config{
		"missing cache":      {TempDir: base.TempDir, ArtifactDir: base.ArtifactDir, TmpfsThresholdBytes: base.TmpfsThresholdBytes},
		"missing temp":       {CacheDir: base.CacheDir, ArtifactDir: base.ArtifactDir, TmpfsThresholdBytes: base.TmpfsThresholdBytes},
		"missing artifact":   {CacheDir: base.CacheDir, TempDir: base.TempDir, TmpfsThresholdBytes: base.TmpfsThresholdBytes},
		"tmpfs without gate": {CacheDir: base.CacheDir, TempDir: base.TempDir, ArtifactDir: base.ArtifactDir, TmpfsDir: "/dev/shm/x"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg); err == nil {
				t.Fatalf("New(%s) should fail closed, got nil error", name)
			}
		})
	}
}

func TestNew_ValidConfigs(t *testing.T) {
	if _, err := New(testConfig(false)); err != nil {
		t.Fatalf("New without tmpfs: %v", err)
	}
	if _, err := New(testConfig(true)); err != nil {
		t.Fatalf("New with tmpfs: %v", err)
	}
}

// ── class roots ───────────────────────────────────────────────────────────

func TestClass_Roots(t *testing.T) {
	r := mustResolver(t, testConfig(true))
	cases := map[Class]string{
		CachePersistent: "/nvme/cache",
		AttemptTemp:     "/nvme/temp",
		ArtifactFinal:   "/nvme/artifact",
	}
	for class, want := range cases {
		got, err := r.Class(class)
		if err != nil {
			t.Fatalf("Class(%s): %v", class, err)
		}
		if got != want {
			t.Errorf("Class(%s) = %q, want %q", class, got, want)
		}
	}
	if _, err := r.Class(Class("BOGUS")); err == nil {
		t.Error("Class(BOGUS) should error")
	}
}

// ── CACHE_PERSISTENT / ARTIFACT_FINAL are always NVMe ─────────────────────

func TestPlace_PersistentAndFinal_NeverTmpfs(t *testing.T) {
	r := mustResolver(t, testConfig(true))
	// Even a "small" cached asset or final artifact must NEVER route to
	// tmpfs: cached blobs survive jobs, and the final is the durable
	// deliverable.
	for _, class := range []Class{CachePersistent, ArtifactFinal} {
		p, err := r.Place(class, "rel/path.bin", 16) // tiny, tmpfs-eligible size
		if err != nil {
			t.Fatalf("Place(%s): %v", class, err)
		}
		if p.Backing != BackingNvme {
			t.Errorf("Place(%s, tiny) backing = %s, want nvme", class, p.Backing)
		}
		if !strings.HasPrefix(p.Path, "/nvme/") {
			t.Errorf("Place(%s) path %q must stay on NVMe", class, p.Path)
		}
	}
}

func TestPlace_PersistentAndFinal_JoinRel(t *testing.T) {
	r := mustResolver(t, testConfig(false))
	p, err := r.Place(CachePersistent, "assets/video/abc.mp4", -1)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join("/nvme/cache", "assets/video/abc.mp4"); p.Path != want {
		t.Errorf("cache path = %q, want %q", p.Path, want)
	}
	p, err = r.Place(ArtifactFinal, "job-1/final.mp4", -1)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join("/nvme/artifact", "job-1/final.mp4"); p.Path != want {
		t.Errorf("artifact path = %q, want %q", p.Path, want)
	}
}

// ── ATTEMPT_TEMP tmpfs threshold gate ─────────────────────────────────────

func TestPlace_AttemptTemp_TmpfsGate(t *testing.T) {
	r := mustResolver(t, testConfig(true))
	cases := []struct {
		name      string
		rel       string
		sizeBytes int64
		want      Backing
		wantPath  string
	}{
		{"below threshold", "seg.mp4", 64*1024*1024 - 1, BackingTmpfs, filepath.Join("/dev/shm/velox-worker", "seg.mp4")},
		{"zero bytes", "manifest.json", 0, BackingTmpfs, filepath.Join("/dev/shm/velox-worker", "manifest.json")},
		{"exactly threshold → NVMe", "seg.mp4", 64 * 1024 * 1024, BackingNvme, filepath.Join("/nvme/temp", "seg.mp4")},
		{"above threshold → NVMe", "seg.mp4", 64*1024*1024 + 1, BackingNvme, filepath.Join("/nvme/temp", "seg.mp4")},
		{"346MB intermediate → NVMe", "video_temp.mp4", 346 * 1024 * 1024, BackingNvme, filepath.Join("/nvme/temp", "video_temp.mp4")},
		{"unknown size → NVMe", "unknown.bin", -1, BackingNvme, filepath.Join("/nvme/temp", "unknown.bin")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := r.Place(AttemptTemp, tc.rel, tc.sizeBytes)
			if err != nil {
				t.Fatal(err)
			}
			if p.Backing != tc.want {
				t.Errorf("backing = %s, want %s (size=%d)", p.Backing, tc.want, tc.sizeBytes)
			}
			if p.Path != tc.wantPath {
				t.Errorf("path = %q, want %q", p.Path, tc.wantPath)
			}
			if p.Class != AttemptTemp {
				t.Errorf("class = %s, want %s", p.Class, AttemptTemp)
			}
		})
	}
}

func TestPlace_AttemptTemp_NoTmpfsConfigured(t *testing.T) {
	r := mustResolver(t, testConfig(false)) // TmpfsDir empty
	p, err := r.Place(AttemptTemp, "manifest.json", 16)
	if err != nil {
		t.Fatal(err)
	}
	if p.Backing != BackingNvme {
		t.Errorf("backing = %s, want nvme (tmpfs disabled)", p.Backing)
	}
	if want := filepath.Join("/nvme/temp", "manifest.json"); p.Path != want {
		t.Errorf("path = %q, want %q", p.Path, want)
	}
}

func TestTmpfsEligible(t *testing.T) {
	r := mustResolver(t, testConfig(true))
	if !r.TmpfsEligible(0) || !r.TmpfsEligible(defaultTestThreshold-1) {
		t.Error("small sizes should be tmpfs-eligible")
	}
	if r.TmpfsEligible(defaultTestThreshold) || r.TmpfsEligible(defaultTestThreshold+1) {
		t.Error("at/above threshold must never be tmpfs-eligible")
	}
	if r.TmpfsEligible(-1) {
		t.Error("unknown size must never be tmpfs-eligible")
	}
	noTmpfs := mustResolver(t, testConfig(false))
	if noTmpfs.TmpfsEligible(16) {
		t.Error("tmpfs-eligibility requires a configured TmpfsDir")
	}
}

// ── unknown class ─────────────────────────────────────────────────────────

func TestPlace_UnknownClass(t *testing.T) {
	r := mustResolver(t, testConfig(false))
	if _, err := r.Place(Class("BOGUS"), "x.bin", 16); err == nil {
		t.Error("Place with unknown class should error")
	}
}

// ── EnsureDirs ────────────────────────────────────────────────────────────

func TestEnsureDirs_CreatesAllBackings(t *testing.T) {
	root := t.TempDir()
	cfg := Config{
		CacheDir:            filepath.Join(root, "cache"),
		TempDir:             filepath.Join(root, "temp"),
		TmpfsDir:            filepath.Join(root, "shm"),
		ArtifactDir:         filepath.Join(root, "artifact"),
		TmpfsThresholdBytes: defaultTestThreshold,
	}
	r := mustResolver(t, cfg)
	if err := r.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}
	for _, dir := range []string{cfg.CacheDir, cfg.TempDir, cfg.TmpfsDir, cfg.ArtifactDir} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", dir)
		}
	}
	// Idempotent second pass.
	if err := r.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs second pass: %v", err)
	}
}

// ── diagnostics ───────────────────────────────────────────────────────────

func TestString_IncludesThresholdAndBackings(t *testing.T) {
	r := mustResolver(t, testConfig(true))
	s := r.String()
	for _, want := range []string{"cache=/nvme/cache", "temp=/nvme/temp", "tmpfs=/dev/shm/velox-worker", "tmpfs_threshold_bytes=67108864", "artifact=/nvme/artifact"} {
		if !strings.Contains(s, want) {
			t.Errorf("String() %q missing %q", s, want)
		}
	}
}
