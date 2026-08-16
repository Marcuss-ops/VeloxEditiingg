package storage

import (
	"errors"
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

// testStagingConfig returns a config with ARTIFACT_STAGING tmpfs enabled.
func testStagingConfig() Config {
	cfg := testConfig(false)
	cfg.ArtifactStaging = ArtifactStagingConfig{
		Enabled:      true,
		Dir:          "/dev/shm/velox-artifacts",
		MaxPercent:   65,
		ReserveBytes: 512 * 1024 * 1024,
	}
	return cfg
}

// setStagingStatfs injects a deterministic tmpfs view into the resolver's
// staging manager. Requires staging to be enabled.
func setStagingStatfs(t *testing.T, r *Resolver, total, avail int64) {
	t.Helper()
	if r.staging == nil {
		t.Fatal("staging manager not constructed; enable artifact staging in config")
	}
	r.staging.statfs = func(string) (int64, int64, error) { return total, avail, nil }
}

// recordingStagingMetrics captures the ARTIFACT_STAGING observability calls
// for deterministic assertions.
type recordingStagingMetrics struct {
	fallbacks []string
	reserved  []int64
}

func (r *recordingStagingMetrics) RecordArtifactNvmeFallback(reason string) {
	r.fallbacks = append(r.fallbacks, reason)
}
func (r *recordingStagingMetrics) SetArtifactTmpfsReservedBytes(reserved int64) {
	r.reserved = append(r.reserved, reserved)
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
		ArtifactStaging: "/nvme/artifact",
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

// ── ARTIFACT_STAGING: tmpfs-with-reservation, NVMe fallback ─────────────

func TestNew_ArtifactStagingFailClosed(t *testing.T) {
	base := testConfig(false)
	cases := map[string]ArtifactStagingConfig{
		"missing dir":      {Enabled: true, MaxPercent: 65, ReserveBytes: 512 * 1024 * 1024},
		"zero max percent": {Enabled: true, Dir: "/dev/shm/x", MaxPercent: 0, ReserveBytes: 512 * 1024 * 1024},
		"max percent > 99": {Enabled: true, Dir: "/dev/shm/x", MaxPercent: 100, ReserveBytes: 512 * 1024 * 1024},
		"zero reserve":     {Enabled: true, Dir: "/dev/shm/x", MaxPercent: 65, ReserveBytes: 0},
	}
	for name, sc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.ArtifactStaging = sc
			if _, err := New(cfg); err == nil {
				t.Fatal("New with inconsistent artifact staging should fail closed")
			}
		})
	}
}

func TestPlace_ArtifactStaging_DisabledFallsBackNvme(t *testing.T) {
	r := mustResolver(t, testConfig(false)) // staging disabled
	p, err := r.Place(ArtifactStaging, "job-1/final.mp4", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if p.Class != ArtifactStaging {
		t.Errorf("class = %s, want %s", p.Class, ArtifactStaging)
	}
	if p.Backing != BackingNvme {
		t.Errorf("backing = %s, want nvme (staging disabled)", p.Backing)
	}
	if want := filepath.Join("/nvme/artifact", "job-1/final.mp4"); p.Path != want {
		t.Errorf("path = %q, want %q", p.Path, want)
	}
	if p.ReservedBytes != 0 {
		t.Errorf("disabled staging must not reserve, got %d", p.ReservedBytes)
	}
}

func TestPlace_ArtifactStaging_TmpfsReserveAndRelease(t *testing.T) {
	r := mustResolver(t, testStagingConfig())
	setStagingStatfs(t, r, 4<<30, 4<<30) // 4 GiB tmpfs, 4 GiB free

	p, err := r.Place(ArtifactStaging, "job-1/final.mp4", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if p.Backing != BackingTmpfs {
		t.Fatalf("backing = %s, want tmpfs (1 GiB fits in 65%% of 4 GiB)", p.Backing)
	}
	if want := filepath.Join("/dev/shm/velox-artifacts", "job-1/final.mp4"); p.Path != want {
		t.Errorf("path = %q, want %q", p.Path, want)
	}
	if p.ReservedBytes != 1<<30 {
		t.Errorf("reserved = %d, want %d", p.ReservedBytes, int64(1<<30))
	}
	if got := r.ReservedTmpfsBytes(); got != 1<<30 {
		t.Errorf("ReservedTmpfsBytes = %d, want %d", got, int64(1<<30))
	}

	r.ReleaseArtifact(p)
	if got := r.ReservedTmpfsBytes(); got != 0 {
		t.Errorf("after release, ReservedTmpfsBytes = %d, want 0", got)
	}
	// Double-release is a safe no-op.
	r.ReleaseArtifact(p)
	if got := r.ReservedTmpfsBytes(); got != 0 {
		t.Errorf("double release should stay 0, got %d", got)
	}
}

func TestPlace_ArtifactStaging_SecondOversizedReservationFallsBack(t *testing.T) {
	r := mustResolver(t, testStagingConfig())
	setStagingStatfs(t, r, 4<<30, 4<<30)

	p1, err := r.Place(ArtifactStaging, "job-1/final.mp4", 2<<30)
	if err != nil {
		t.Fatal(err)
	}
	if p1.Backing != BackingTmpfs {
		t.Fatalf("first placement = %s, want tmpfs", p1.Backing)
	}

	// 65% of 4 GiB ≈ 2.6 GiB; 2 GiB already reserved, so a further 1 GiB
	// does not fit → durable NVMe fallback.
	p2, err := r.Place(ArtifactStaging, "job-2/final.mp4", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Backing != BackingNvme {
		t.Fatalf("second placement = %s, want nvme (reservation budget exceeded)", p2.Backing)
	}
	if want := filepath.Join("/nvme/artifact", "job-2/final.mp4"); p2.Path != want {
		t.Errorf("path = %q, want %q", p2.Path, want)
	}
	if p2.ReservedBytes != 0 {
		t.Errorf("nvme fallback must not reserve, got %d", p2.ReservedBytes)
	}
}

// TestPlace_ArtifactStaging_InsufficientRAMFallsBackNvme is the acceptance
// case for the RAM-staging decision: when the tmpfs cannot satisfy the
// estimated reservation (free bytes minus reserve headroom are smaller than
// the estimate), the placement lands on durable NVMe and reserves nothing.
func TestPlace_ArtifactStaging_InsufficientRAMFallsBackNvme(t *testing.T) {
	r := mustResolver(t, testStagingConfig())
	// 4 GiB tmpfs, 1 GiB free, 512 MiB reserve → ~512 MiB budget. A 1 GiB
	// estimate does not fit → NVMe fallback.
	setStagingStatfs(t, r, 4<<30, 1<<30)

	p, err := r.Place(ArtifactStaging, "job-1/final.mp4", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if p.Backing != BackingNvme {
		t.Errorf("backing = %s, want nvme (insufficient RAM for the estimate)", p.Backing)
	}
	if want := filepath.Join("/nvme/artifact", "job-1/final.mp4"); p.Path != want {
		t.Errorf("path = %q, want %q", p.Path, want)
	}
	if p.ReservedBytes != 0 {
		t.Errorf("nvme fallback must not reserve, got %d", p.ReservedBytes)
	}
	if p.FallbackReason != FallbackNoSpace {
		t.Errorf("FallbackReason = %q, want %q", p.FallbackReason, FallbackNoSpace)
	}
	if got := r.ReservedTmpfsBytes(); got != 0 {
		t.Errorf("ReservedTmpfsBytes = %d, want 0", got)
	}
}

func TestPlace_ArtifactStaging_ReportsFallbackAndReservedToMetrics(t *testing.T) {
	cfg := testStagingConfig()
	m := &recordingStagingMetrics{}
	cfg.StagingMetrics = m
	r := mustResolver(t, cfg)
	// 4 GiB tmpfs, 1 GiB free, 512 MiB reserve → ~512 MiB budget.
	setStagingStatfs(t, r, 4<<30, 1<<30)

	// 1 GiB estimate does not fit → NVMe fallback reported with no_space.
	p, err := r.Place(ArtifactStaging, "job-1/final.mp4", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if p.Backing != BackingNvme || p.FallbackReason != FallbackNoSpace {
		t.Fatalf("placement = %+v, want NVMe/no_space", p)
	}
	if len(m.fallbacks) != 1 || m.fallbacks[0] != string(FallbackNoSpace) {
		t.Fatalf("fallbacks = %v, want [no_space]", m.fallbacks)
	}

	// A tiny estimate fits → tmpfs, no fallback, and the reserved ledger is
	// published.
	p2, err := r.Place(ArtifactStaging, "job-2/final.mp4", 64)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Backing != BackingTmpfs || p2.FallbackReason != FallbackNone {
		t.Fatalf("placement = %+v, want tmpfs/no fallback", p2)
	}
	if len(m.reserved) != 1 || m.reserved[0] != 64 {
		t.Fatalf("reserved = %v, want [64]", m.reserved)
	}

	// Release publishes the ledger back to zero.
	r.ReleaseArtifact(p2)
	if len(m.reserved) != 2 || m.reserved[1] != 0 {
		t.Fatalf("reserved after release = %v, want [64 0]", m.reserved)
	}
}

func TestPlace_ArtifactStaging_UnknownSizeFallsBackNvme(t *testing.T) {
	r := mustResolver(t, testStagingConfig())
	setStagingStatfs(t, r, 4<<30, 4<<30)

	p, err := r.Place(ArtifactStaging, "job-1/final.mp4", -1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Backing != BackingNvme {
		t.Errorf("backing = %s, want nvme (unknown size never reserves)", p.Backing)
	}
	if got := r.ReservedTmpfsBytes(); got != 0 {
		t.Errorf("unknown size must not reserve, got %d", got)
	}
}

func TestPlace_ArtifactStaging_StatfsErrorFallsBackNvme(t *testing.T) {
	r := mustResolver(t, testStagingConfig())
	r.staging.statfs = func(string) (int64, int64, error) {
		return 0, 0, errors.New("statfs boom")
	}

	p, err := r.Place(ArtifactStaging, "job-1/final.mp4", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if p.Backing != BackingNvme {
		t.Errorf("backing = %s, want nvme on statfs error", p.Backing)
	}
}

func TestEnsureDirs_CreatesArtifactStagingDir(t *testing.T) {
	root := t.TempDir()
	cfg := Config{
		CacheDir:            filepath.Join(root, "cache"),
		TempDir:             filepath.Join(root, "temp"),
		ArtifactDir:         filepath.Join(root, "artifact"),
		TmpfsThresholdBytes: defaultTestThreshold,
		ArtifactStaging: ArtifactStagingConfig{
			Enabled:      true,
			Dir:          filepath.Join(root, "shm-artifacts"),
			MaxPercent:   65,
			ReserveBytes: 512 * 1024 * 1024,
		},
	}
	r := mustResolver(t, cfg)
	if err := r.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}
	info, err := os.Stat(cfg.ArtifactStaging.Dir)
	if err != nil {
		t.Fatalf("stat artifact staging dir: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("%s is not a directory", cfg.ArtifactStaging.Dir)
	}
}

func TestString_IncludesArtifactStaging(t *testing.T) {
	r := mustResolver(t, testStagingConfig())
	s := r.String()
	for _, want := range []string{"artifact_staging", "tmpfs=/dev/shm/velox-artifacts", "max_percent=65"} {
		if !strings.Contains(s, want) {
			t.Errorf("String() %q missing %q", s, want)
		}
	}
}
