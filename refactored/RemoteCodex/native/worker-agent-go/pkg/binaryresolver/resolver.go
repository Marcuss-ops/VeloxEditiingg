// Package binaryresolver locates executables on disk using a deterministic
// strategy: env override → absolute candidates → relative offsets from the
// call-site of Resolve(). The same Resolver pattern fits the video engine,
// ffmpeg/ffprobe, ansible-playbook, and any other sidecar invoked through
// exec.Command.
package binaryresolver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Resolver describes the lookup strategy for a single binary.
//
// The resolution order is:
//
//  1. Resolve() honors EnvVar if set and points at an existing file.
//  2. Each entry in AbsCandidates is checked verbatim (must exist).
//  3. Each RelOffset is appended to the call-site directory given by
//     runtime.Caller (the file invoking Resolve()). Useful during local
//     development when the binary is built next to the Go source.
type Resolver struct {
	// Name is a friendly human-readable identifier used in error messages.
	Name string
	// EnvVar, if set, is the environment variable that overrides resolution.
	EnvVar string
	// AbsCandidates is tried in order. The first existing regular file wins.
	AbsCandidates []string
	// RelOffsets is a list of paths relative to the call-site directory.
	// Resolved by joining filepath.Dir(runtime.Caller) + each offset. Use
	// ".." freely to walk up the source tree.
	RelOffsets []string
}

// Resolve returns the absolute path of the binary, or an error describing
// what was tried if the binary cannot be located.
//
// Passed skip is forwarded to runtime.Caller: 0 = call-site of Resolve(),
// 1 = caller's caller, etc. The default is 0 which is appropriate when
// Resolve() is invoked directly from the package needing the path.
func (r Resolver) Resolve(skip int) (string, error) {
	name := r.Name
	if name == "" {
		name = "binary"
	}

	if r.EnvVar != "" {
		if override := strings.TrimSpace(os.Getenv(r.EnvVar)); override != "" {
			if stat, err := os.Stat(override); err == nil && !stat.IsDir() {
				return override, nil
			}
		}
	}

	for _, candidate := range r.AbsCandidates {
		cleaned := filepath.Clean(strings.TrimSpace(candidate))
		if cleaned == "" {
			continue
		}
		if stat, err := os.Stat(cleaned); err == nil && !stat.IsDir() {
			return cleaned, nil
		}
	}

	if len(r.RelOffsets) > 0 {
		_, sourceFile, _, ok := runtime.Caller(skip + 1)
		if !ok {
			return "", fmt.Errorf("%s: unable to locate call-site via runtime.Caller", name)
		}
		base := filepath.Dir(sourceFile)
		for _, offset := range r.RelOffsets {
			candidate := filepath.Join(base, offset)
			if stat, err := os.Stat(candidate); err == nil && !stat.IsDir() {
				return candidate, nil
			}
		}
	}

	return "", r.notFoundError()
}

// ErrNotFound is returned (wrapped) when none of the candidates resolve.
//
// Use errors.Is to detect this case at the call site.
var ErrNotFound = errors.New("binary not found")

func (r Resolver) notFoundError() error {
	tried := append([]string{r.EnvVar}, r.AbsCandidates...)
	tried = append(tried, r.RelOffsets...)
	return fmt.Errorf("%w: %s (set a valid override in env %q, build/install %s, or place it in one of the searched paths); tried=%v",
		ErrNotFound, r.Name, r.EnvVar, r.Name, tried)
}
