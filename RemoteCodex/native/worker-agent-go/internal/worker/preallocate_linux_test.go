//go:build linux

package worker

import (
	"os"
	"testing"
)

func TestPreallocateFileReservesSize(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "prealloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const size = 1 << 20 // 1 MiB
	if err := preallocateFile(f, size); err != nil {
		t.Fatalf("preallocateFile: %v", err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != size {
		t.Fatalf("size = %d, want %d", info.Size(), size)
	}
}
