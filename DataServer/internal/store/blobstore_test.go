package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPromoteDurableEnforceRenamesOnSameFilesystem(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging")
	final := filepath.Join(root, "final")
	bs, err := NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.WithPromoteMode(PromoteModeEnforce); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(staging, "upload.part")
	if err := os.WriteFile(source, []byte("durable payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	witness := filepath.Join(staging, "witness")
	if err := os.Link(source, witness); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(final, "artifact.bin")
	got, err := bs.PromoteDurable(source, target)
	if err != nil {
		t.Fatal(err)
	}
	if got != target {
		t.Fatalf("PromoteDurable path = %q, want %q", got, target)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("staging source remains: %v", err)
	}
	finalInfo, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	witnessInfo, err := os.Stat(witness)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(finalInfo, witnessInfo) {
		t.Fatal("enforce mode copied bytes instead of renaming the staged inode")
	}
}

func TestPromoteDurableShadowCopiesAndKeepsStagingSemantics(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging")
	final := filepath.Join(root, "final")
	bs, err := NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.WithPromoteMode(PromoteModeShadow); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(staging, "upload.part")
	if err := os.WriteFile(source, []byte("shadow payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	witness := filepath.Join(staging, "witness")
	if err := os.Link(source, witness); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(final, "artifact.bin")
	if _, err := bs.PromoteDurable(source, target); err != nil {
		t.Fatal(err)
	}
	finalInfo, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	witnessInfo, err := os.Stat(witness)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(finalInfo, witnessInfo) {
		t.Fatal("shadow mode unexpectedly moved the staged inode")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "shadow payload" {
		t.Fatalf("target content = %q", got)
	}
}
