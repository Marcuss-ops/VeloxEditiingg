package darkeditor

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

// ErrUnsafePath is returned when an external path reference cannot be safely
// confined to its configured Darkeditor directory.
var ErrUnsafePath = errors.New("unsafe path reference")

const maxPathDecodingPasses = 8

func decodePathReference(raw string) (string, error) {
	decoded := raw
	for pass := 0; pass < maxPathDecodingPasses; pass++ {
		next, err := url.PathUnescape(decoded)
		if err != nil {
			return "", fmt.Errorf("%w: invalid URL escaping", ErrUnsafePath)
		}
		if next == decoded {
			if strings.Contains(decoded, "%") {
				return "", fmt.Errorf("%w: residual percent escape", ErrUnsafePath)
			}
			return decoded, nil
		}
		decoded = next
	}
	return "", fmt.Errorf("%w: excessive URL encoding", ErrUnsafePath)
}

// validatePathComponent accepts only a single flat filename or project ID.
// Darkeditor has no supported nested user path, so separators are rejected
// rather than sanitized into a different resource.
func validatePathComponent(raw string) (string, error) {
	decoded, err := decodePathReference(raw)
	if err != nil {
		return "", err
	}
	if decoded == "" || strings.IndexByte(decoded, 0) >= 0 {
		return "", fmt.Errorf("%w: empty or NUL-containing reference", ErrUnsafePath)
	}
	if decoded == "." || decoded == ".." || filepath.IsAbs(decoded) {
		return "", fmt.Errorf("%w: absolute or dot path", ErrUnsafePath)
	}
	if strings.ContainsAny(decoded, `/\\`) {
		return "", fmt.Errorf("%w: path separator is not allowed", ErrUnsafePath)
	}
	// Reject Windows drive-qualified and UNC forms even on Linux.
	if len(decoded) >= 2 && unicode.IsLetter(rune(decoded[0])) && decoded[1] == ':' {
		return "", fmt.Errorf("%w: Windows drive path", ErrUnsafePath)
	}
	if strings.HasPrefix(decoded, `\\`) {
		return "", fmt.Errorf("%w: UNC path", ErrUnsafePath)
	}
	return decoded, nil
}

func canonicalBaseDir(baseDir string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("%w: empty base directory", ErrUnsafePath)
	}
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("%w: absolute base: %v", ErrUnsafePath, err)
	}
	if err := os.MkdirAll(abs, 0755); err != nil {
		return "", fmt.Errorf("%w: create base: %v", ErrUnsafePath, err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("%w: resolve base: %v", ErrUnsafePath, err)
	}
	return resolved, nil
}

func openConfinedRoot(baseDir string) (*os.Root, error) {
	base, err := canonicalBaseDir(baseDir)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return nil, fmt.Errorf("%w: open root: %v", ErrUnsafePath, err)
	}
	return root, nil
}

// confinedReadFile performs the read while the os.Root containment boundary
// is active; it does not return a path that can later be redirected by a
// symlink race.
func confinedReadFile(baseDir, reference string) ([]byte, error) {
	name, err := validatePathComponent(reference)
	if err != nil {
		return nil, err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.ReadFile(name)
}

func confinedWriteFile(baseDir, reference string, data []byte, perm os.FileMode) error {
	name, err := validatePathComponent(reference)
	if err != nil {
		return err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.WriteFile(name, data, perm)
}

// confinedProjectReadFile/read/write/remove use a validated flat project ID
// and a fixed server-owned filename. The complete nested operation remains
// inside one os.Root, so project-directory symlink replacement cannot escape.
func confinedProjectReadFile(baseDir, projectID, filename string) ([]byte, error) {
	id, err := validatePathComponent(projectID)
	if err != nil {
		return nil, err
	}
	name, err := validatePathComponent(filename)
	if err != nil {
		return nil, err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.ReadFile(filepath.Join(id, name))
}

func confinedProjectWriteFile(baseDir, projectID, filename string, data []byte, perm os.FileMode) error {
	id, err := validatePathComponent(projectID)
	if err != nil {
		return err
	}
	name, err := validatePathComponent(filename)
	if err != nil {
		return err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.WriteFile(filepath.Join(id, name), data, perm)
}

func confinedProjectRemoveAll(baseDir, projectID string) error {
	id, err := validatePathComponent(projectID)
	if err != nil {
		return err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveAll(id)
}

// ensureProjectDir creates only the validated project directory beneath the
// configured root. It never creates user-controlled intermediate components.
func ensureProjectDir(baseDir, projectID string) error {
	id, err := validatePathComponent(projectID)
	if err != nil {
		return err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return err
	}
	defer root.Close()
	if _, err := root.Lstat(id); os.IsNotExist(err) {
		return root.Mkdir(id, 0755)
	} else if err != nil {
		return err
	}
	info, err := root.Lstat(id)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: project directory is not a real directory", ErrUnsafePath)
	}
	return nil
}

func projectDirExists(baseDir, projectID string) error {
	id, err := validatePathComponent(projectID)
	if err != nil {
		return err
	}
	root, err := openConfinedRoot(baseDir)
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Lstat(id)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: project directory is not a real directory", ErrUnsafePath)
	}
	return nil
}
