package darkeditor

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePathComponentRejectsTraversalAndAbsoluteReferences(t *testing.T) {
	t.Parallel()

	cases := []string{
		"../secret.txt",
		"../../secret.txt",
		`..\secret.txt`,
		"%2e%2e%2fsecret.txt",
		"%252e%252e%252fsecret.txt",
		"/etc/passwd",
		`C:\Windows\win.ini`,
		"C:relative.txt",
		`\\server\share\secret.txt`,
		"foo/bar.txt",
		"foo\\bar.txt",
		"bad\x00name.txt",
		"%ZZ",
	}

	for _, input := range cases {
		input := input
		t.Run(input, func(t *testing.T) {
			if _, err := validatePathComponent(input); !errors.Is(err, ErrUnsafePath) {
				t.Fatalf("validatePathComponent(%q) error = %v, want ErrUnsafePath", input, err)
			}
		})
	}
}

func TestValidatePathComponentAcceptsSafeUnicodeAndSpaces(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"image with spaces.png", "café-日本.png", "frame_01.webp"} {
		got, err := validatePathComponent(input)
		if err != nil {
			t.Fatalf("validatePathComponent(%q): %v", input, err)
		}
		if got != input {
			t.Fatalf("validatePathComponent(%q) = %q", input, got)
		}
	}
}

func TestConfinedReadWriteRejectsSymlinkAndKeepsDataContained(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := confinedWriteFile(base, "image with spaces.png", []byte("safe"), 0644); err != nil {
		t.Fatalf("confinedWriteFile(valid): %v", err)
	}
	data, err := confinedReadFile(base, "image with spaces.png")
	if err != nil || string(data) != "safe" {
		t.Fatalf("confinedReadFile(valid) = %q, %v", data, err)
	}

	link := filepath.Join(base, "link.txt")
	if err := os.Symlink(outsideFile, link); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("symlinks are unavailable in this environment")
		}
		t.Fatal(err)
	}
	if _, err := confinedReadFile(base, "link.txt"); err == nil {
		t.Fatal("confinedReadFile(symlink) unexpectedly succeeded")
	}
	if err := confinedWriteFile(base, "link.txt", []byte("must not escape"), 0644); err == nil {
		t.Fatal("confinedWriteFile(symlink) unexpectedly succeeded")
	}
	got, err := os.ReadFile(outsideFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "secret" {
		t.Fatalf("outside file was modified: %q", got)
	}
}

func TestConfinedProjectOperationsRejectTraversalAndSymlink(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	if err := ensureProjectDir(base, "project-1"); err != nil {
		t.Fatalf("ensureProjectDir(valid): %v", err)
	}
	if err := confinedProjectWriteFile(base, "project-1", "canvas.json", []byte(`{"ok":true}`), 0644); err != nil {
		t.Fatalf("confinedProjectWriteFile(valid): %v", err)
	}
	if _, err := confinedProjectReadFile(base, "../outside", "canvas.json"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("project traversal error = %v, want ErrUnsafePath", err)
	}

	outside := t.TempDir()
	link := filepath.Join(base, "project-link")
	if err := os.Symlink(outside, link); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("symlinks are unavailable in this environment")
		}
		t.Fatal(err)
	}
	if err := ensureProjectDir(base, "project-link"); err == nil {
		t.Fatal("ensureProjectDir(symlink) unexpectedly succeeded")
	}
	if err := confinedProjectWriteFile(base, "project-link", "canvas.json", []byte("escape"), 0644); err == nil {
		t.Fatal("confinedProjectWriteFile(symlink) unexpectedly succeeded")
	}
}

func TestConfinedProjectRemoveDoesNotCreateMissingDirectory(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	if err := projectDirExists(base, "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("projectDirExists(missing) = %v, want not-exist", err)
	}
	if err := confinedProjectRemoveAll(base, "missing"); err != nil {
		t.Fatalf("confinedProjectRemoveAll(missing) = %v, want idempotent success", err)
	}
	if _, err := os.Stat(filepath.Join(base, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remove of missing project created a directory: %v", err)
	}
}
