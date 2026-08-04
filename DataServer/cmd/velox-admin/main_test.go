package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunDriveDuplicateCleanupRequiresExplicitMode(t *testing.T) {
	for _, args := range [][]string{
		{"cleanup-drive-duplicates", "--db", "/tmp/velox.db", "--manifest", "/tmp/manifest.json"},
		{"cleanup-drive-duplicates", "--db", "/tmp/velox.db", "--manifest", "/tmp/manifest.json", "--dry-run", "--apply"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "exactly one") {
			t.Fatalf("args=%v err=%v stderr=%q", args, err, stderr.String())
		}
	}
}

func TestRunHelpListsDuplicateCleanupCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"--help"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "cleanup-drive-duplicates") {
		t.Fatalf("help=%q", stderr.String())
	}
}
