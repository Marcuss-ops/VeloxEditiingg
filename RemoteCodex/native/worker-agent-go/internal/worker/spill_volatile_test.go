package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"velox-worker-agent/internal/spool"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"
)

// spillTestResolver builds a resolver with ARTIFACT_STAGING enabled over
// temp dirs (real filesystem hosts the "tmpfs" backing so the statfs probe
// reports a large budget without a mounted /dev/shm).
func spillTestResolver(t *testing.T, stagingDir, artifactDir string) *storage.Resolver {
	t.Helper()
	r, err := storage.New(storage.Config{
		CacheDir:            filepath.Join(t.TempDir(), "cache"),
		TempDir:             filepath.Join(t.TempDir(), "temp"),
		ArtifactDir:         artifactDir,
		TmpfsThresholdBytes: 64 * 1024 * 1024,
		ArtifactStaging: storage.ArtifactStagingConfig{
			Enabled:      true,
			Dir:          stagingDir,
			MaxPercent:   99,
			ReserveBytes: 1,
		},
	})
	if err != nil {
		t.Fatalf("build resolver: %v", err)
	}
	if err := r.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	return r
}

func TestOutputStorageTier(t *testing.T) {
	stagingDir := t.TempDir()
	resolver := spillTestResolver(t, stagingDir, filepath.Join(t.TempDir(), "artifact"))
	w := &Worker{storageResolver: resolver}

	if got := w.outputStorageTier(filepath.Join(stagingDir, "job.mp4")); got != spool.StorageTierTmpfsVolatile {
		t.Errorf("staging path tier = %q; want TMPFS_VOLATILE", got)
	}
	if got := w.outputStorageTier("/var/lib/velox/artifact/job.mp4"); got != spool.StorageTierNvmeDurable {
		t.Errorf("nvme path tier = %q; want NVME_DURABLE", got)
	}
	if got := w.outputStorageTier(""); got != spool.StorageTierNvmeDurable {
		t.Errorf("empty path tier = %q; want NVME_DURABLE", got)
	}
	// No resolver wired → durable default.
	if got := (&Worker{}).outputStorageTier(filepath.Join(stagingDir, "job.mp4")); got != spool.StorageTierNvmeDurable {
		t.Errorf("nil-resolver tier = %q; want NVME_DURABLE", got)
	}
}

func TestSpillVolatileToNVMe_MovesFileAndRepointsSpool(t *testing.T) {
	stagingDir := t.TempDir()
	artifactDir := t.TempDir()
	resolver := spillTestResolver(t, stagingDir, artifactDir)
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer store.Close()

	// Hold a real reservation via the canonical placement path.
	p, err := resolver.Place(storage.ArtifactStaging, "job.mp4", 1024)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if p.Backing != storage.BackingTmpfs {
		t.Fatalf("backing = %s; want tmpfs", p.Backing)
	}
	if err := os.WriteFile(p.Path, []byte("rendered-bytes"), 0o640); err != nil {
		t.Fatalf("write tmpfs artifact: %v", err)
	}

	entry, err := store.Insert(context.Background(), spool.SpoolEntry{
		TaskID: "task-spill", AttemptID: "attempt-spill", WorkerSpoolKey: "task-spill:output:0",
		LocalPath: p.Path, StorageTier: spool.StorageTierTmpfsVolatile,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	_ = store.MarkReady(context.Background(), entry.SpoolID, string64('a'), 14)
	_ = store.MarkUploadPending(context.Background(), entry.SpoolID, "up-spill")
	_ = store.MarkUploading(context.Background(), entry.SpoolID, 0)

	w := &Worker{
		config:          &config.WorkerConfig{WorkerID: "worker-spill", OutputDir: artifactDir},
		storageResolver: resolver,
		outputSpool:     store,
		logger:          logger.New(logger.InfoLevel, io.Discard),
	}
	if !w.spillVolatileToNVMe(context.Background(), *entry) {
		t.Fatal("spillVolatileToNVMe returned false")
	}

	// The tmpfs copy is gone and the reservation is freed.
	if _, err := os.Stat(p.Path); !os.IsNotExist(err) {
		t.Fatalf("tmpfs artifact still exists (stat err=%v)", err)
	}
	if got := resolver.ReservedTmpfsBytes(); got != 0 {
		t.Errorf("ReservedTmpfsBytes = %d; want 0 after spill", got)
	}
	// The spool row is repointed to a durable path.
	got, err := store.Get(context.Background(), entry.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.StorageTier != spool.StorageTierNvmeDurable {
		t.Errorf("spool tier = %q; want NVME_DURABLE", got.StorageTier)
	}
	wantPath := filepath.Join(artifactDir, entry.SpoolID+"_job.mp4")
	if got.LocalPath != wantPath {
		t.Errorf("spool path = %q; want %q", got.LocalPath, wantPath)
	}
	if data, err := os.ReadFile(got.LocalPath); err != nil || string(data) != "rendered-bytes" {
		t.Errorf("durable copy read err=%v data=%q; want rendered-bytes", err, string(data))
	}
}

func TestSpillVolatileUncommitted_OnlyMovesNonTerminalVolatile(t *testing.T) {
	stagingDir := t.TempDir()
	artifactDir := t.TempDir()
	resolver := spillTestResolver(t, stagingDir, artifactDir)
	store, err := spool.Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer store.Close()

	volatilePath := filepath.Join(stagingDir, "job.mp4")
	if err := os.WriteFile(volatilePath, []byte("volatile"), 0o640); err != nil {
		t.Fatalf("write volatile: %v", err)
	}
	volEntry, err := store.Insert(context.Background(), spool.SpoolEntry{
		TaskID: "task-vol", AttemptID: "attempt-vol", WorkerSpoolKey: "task-vol:output:0",
		LocalPath: volatilePath, StorageTier: spool.StorageTierTmpfsVolatile,
	})
	if err != nil {
		t.Fatalf("insert volatile: %v", err)
	}
	_ = store.MarkReady(context.Background(), volEntry.SpoolID, string64('v'), 8)
	_ = store.MarkUploadPending(context.Background(), volEntry.SpoolID, "up-vol")
	_ = store.MarkUploading(context.Background(), volEntry.SpoolID, 0)

	// A durable row is never touched by the shutdown spill.
	durablePath := filepath.Join(artifactDir, "dur.mp4")
	if err := os.WriteFile(durablePath, []byte("durable"), 0o640); err != nil {
		t.Fatalf("write durable: %v", err)
	}
	_, err = store.Insert(context.Background(), spool.SpoolEntry{
		TaskID: "task-dur", AttemptID: "attempt-dur", WorkerSpoolKey: "task-dur:output:0",
		LocalPath: durablePath, StorageTier: spool.StorageTierNvmeDurable,
	})
	if err != nil {
		t.Fatalf("insert durable: %v", err)
	}

	w := &Worker{
		config:          &config.WorkerConfig{WorkerID: "worker-shutdown", OutputDir: artifactDir},
		storageResolver: resolver,
		outputSpool:     store,
		logger:          logger.New(logger.InfoLevel, io.Discard),
	}
	w.spillVolatileUncommitted()

	if _, err := os.Stat(volatilePath); !os.IsNotExist(err) {
		t.Errorf("volatile tmpfs file still exists after shutdown spill (err=%v)", err)
	}
	if _, err := os.Stat(durablePath); err != nil {
		t.Errorf("durable file must be untouched (err=%v)", err)
	}
	got, err := store.Get(context.Background(), volEntry.SpoolID)
	if err != nil {
		t.Fatalf("Get volatile: %v", err)
	}
	if got.StorageTier != spool.StorageTierNvmeDurable {
		t.Errorf("volatile tier after spill = %q; want NVME_DURABLE", got.StorageTier)
	}
}

func TestPathWithinRoot(t *testing.T) {
	root := t.TempDir()
	inside := filepath.Join(root, "a", "b.mp4")
	inside2 := filepath.Join(root, "c.mp4")
	outside := filepath.Join(t.TempDir(), "x.mp4")
	cases := []struct {
		path string
		want bool
	}{
		{inside, true},
		{inside2, true},
		{root, false},               // the root itself is not "inside"
		{filepath.Dir(root), false}, // parent is not inside
		{outside, false},
		{"", false},
	}
	for _, tc := range cases {
		if got := pathWithinRoot(root, tc.path); got != tc.want {
			t.Errorf("pathWithinRoot(%q, %q) = %v; want %v", root, tc.path, got, tc.want)
		}
	}
}

func string64(b byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = b
	}
	return string(out)
}
