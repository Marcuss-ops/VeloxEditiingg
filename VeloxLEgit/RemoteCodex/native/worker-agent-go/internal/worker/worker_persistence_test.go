// P0.4: lock down the "worker state lives under cfg.StateDir" contract.
// worker_persistence.go::saveLocalState and ::loadLocalState must
// resolve worker_state.json against cfg.StateDir (the canonical
// runtime dir, env VELOX_STATE_DIR), NOT cfg.WorkDir (the release
// checkout /opt/velox which is mounted :ro inside the container at
// /app). The audit hit was: the previous code used w.config.WorkDir,
// which routed state writes into a subdirectory of the runtime dir
// (work/worker_state.json) rather than the runtime root, AND in the
// case of a bare-default config routed writes to /opt/velox which is
// read-only. This test pins the move to StateDir.
package worker

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// TestStateFilePath_ResolvesToStateDirNotWorkDir is the headline
// assertion: stateFilePath must use the stateDir argument, full stop.
// We don't need a Worker for this — it's a pure function — but we
// keep the test in this file so the contract lives next to the code.
func TestStateFilePath_ResolvesToStateDirNotWorkDir(t *testing.T) {
	stateDir := "/var/lib/velox/workers/velox-worker-1"
	workDir := "/opt/velox"

	got := stateFilePath(stateDir)
	want := filepath.Join(stateDir, "worker_state.json")
	if got != want {
		t.Fatalf("stateFilePath(stateDir)=%q; want %q (must resolve to StateDir, not WorkDir %q)", got, want, workDir)
	}
	if filepath.Dir(got) == workDir {
		t.Fatalf("stateFilePath landed under workDir %q — would write to read-only /app/RemoteCodex", workDir)
	}
}

// TestSaveLoadStateFilePath_HonorsStateDir is the integration half:
// a Worker{} with cfg.StateDir set to a temp dir and cfg.WorkDir
// set to a non-existent path must produce worker_state.json under
// the temp dir, not under WorkDir. The non-existent WorkDir is
// deliberate — if the test ever regresses to writing there, the
// os.MkdirAll at the top of saveLocalState would fail loudly
// instead of silently creating a stray directory.
func TestSaveLoadStateFilePath_HonorsStateDir(t *testing.T) {
	stateDir := t.TempDir()
	workDir := filepath.Join(t.TempDir(), "never-created-readonly-workdir")

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID: "velox-worker-1",
			WorkDir:  workDir,
			StateDir: stateDir,
		},
		logger:       logger.New(logger.InfoLevel, os.Stderr),
		commandMu:    sync.Mutex{},
		seenCommands: make(map[string]time.Time),
	}

	// Seed one seen command so the file is non-empty (exercises the
	// JSON marshalling path too).
	w.seenCommands["cmd-test-1"] = time.Now().UTC()

	// SAVE → assert worker_state.json lands under stateDir, NOT workDir.
	if err := w.saveLocalState(); err != nil {
		t.Fatalf("saveLocalState failed: %v", err)
	}

	wantPath := filepath.Join(stateDir, "worker_state.json")
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("worker_state.json missing at canonical runtime path %q: %v", wantPath, err)
	}

	badPath := filepath.Join(workDir, "worker_state.json")
	if _, err := os.Stat(badPath); err == nil {
		t.Fatalf("worker_state.json leaked under WorkDir %q — P0.4 contract violated", badPath)
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected stat error on WorkDir path %q: %v", badPath, err)
	}

	// Also assert no stray .tmp survived in either location — the
	// atomic-write contract is .tmp + fsync + rename, so the .tmp
	// file must be gone after saveLocalState returns.
	if _, err := os.Stat(wantPath + ".tmp"); err == nil {
		t.Fatalf("worker_state.json.tmp survived at %q after atomic rename; atomic-write contract violated", wantPath+".tmp")
	}
	if _, err := os.Stat(badPath + ".tmp"); err == nil {
		t.Fatalf("worker_state.json.tmp leaked under WorkDir %q", badPath+".tmp")
	}

	// LOAD → mutate in-memory state, then re-load and assert the
	// seen command survived. This proves the load path also resolves
	// the file from StateDir.
	w.seenCommands = make(map[string]time.Time)
	w.loadLocalState()
	if _, ok := w.seenCommands["cmd-test-1"]; !ok {
		t.Fatalf("loadLocalState did not restore cmd-test-1 from %q — load path also uses WorkDir? stateDir=%q", wantPath, stateDir)
	}
}
