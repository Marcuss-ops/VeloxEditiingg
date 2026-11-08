package assets

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"velox-shared/iopool"
)

// preparedStageOutcome records how a prepared fragment reached the staging
// area. It exists so callers and tests can assert that the cheap path
// (clone/link) was used instead of a full byte copy.
type preparedStageOutcome string

const (
	preparedStageClone preparedStageOutcome = "clone"
	preparedStageLink  preparedStageOutcome = "hardlink"
	preparedStageCopy  preparedStageOutcome = "copy"
)

// stagePreparedFragment materializes src at dst using the cheapest safe
// mechanism available: a filesystem clone (reflink), a hard link, then a
// streaming copy. The source is treated as immutable for the duration of the
// call.
//
// When computeHash is true the SHA-256 of the staged bytes is returned. A
// clone or link still needs one read to prove identity, but never the second
// write that the previous copy-into-staging implementation performed for
// every prepared fragment.
func stagePreparedFragment(src, dst string, maxBytes int64, computeHash bool) (int64, string, preparedStageOutcome, error) {
	return stagePreparedFragmentWithOptions(src, dst, maxBytes, computeHash, true, true)
}

func stagePreparedFragmentWithOptions(src, dst string, maxBytes int64, computeHash, allowClone, allowLink bool) (int64, string, preparedStageOutcome, error) {
	info, err := os.Stat(src)
	if err != nil {
		return 0, "", "", fmt.Errorf("open prepared segment: %w", err)
	}
	if !info.Mode().IsRegular() {
		return 0, "", "", fmt.Errorf("prepared segment is not a regular file")
	}
	if info.Size() <= 0 {
		return 0, "", "", fmt.Errorf("prepared video segment is empty")
	}
	if maxBytes > 0 && info.Size() > maxBytes {
		return 0, "", "", fmt.Errorf("prepared video segment exceeds the input byte limit")
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return 0, "", "", fmt.Errorf("prepare segment staging directory: %w", err)
	}

	outcome := preparedStageCopy
	staged := false
	if allowClone && clonePreparedFile(src, dst) == nil {
		outcome, staged = preparedStageClone, true
	}
	if !staged && allowLink && os.Link(src, dst) == nil {
		outcome, staged = preparedStageLink, true
	}
	if !staged {
		if err := copyPreparedFile(src, dst); err != nil {
			return 0, "", "", err
		}
	}

	stagedInfo, err := os.Stat(dst)
	if err != nil {
		_ = os.Remove(dst)
		return 0, "", "", fmt.Errorf("stat staged prepared segment: %w", err)
	}
	if stagedInfo.Size() != info.Size() {
		_ = os.Remove(dst)
		return 0, "", "", fmt.Errorf("staged prepared segment size differs from source")
	}

	sha256Hex := ""
	if computeHash {
		hashedSHA, hashedSize, hashErr := hashFile(dst)
		if hashErr != nil {
			_ = os.Remove(dst)
			return 0, "", "", fmt.Errorf("hash staged prepared segment: %w", hashErr)
		}
		if hashedSize != stagedInfo.Size() {
			_ = os.Remove(dst)
			return 0, "", "", fmt.Errorf("hashed prepared segment size differs from staged file")
		}
		sha256Hex = hashedSHA
	}
	return stagedInfo.Size(), sha256Hex, outcome, nil
}

func copyPreparedFile(src, dst string) error {
	input, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open prepared segment: %w", err)
	}
	defer input.Close()
	staging, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create segment staging file: %w", err)
	}
	pooled := iopool.Get()
	defer iopool.Put(pooled)
	if _, err := io.CopyBuffer(staging, input, pooled[:]); err != nil {
		_ = staging.Close()
		_ = os.Remove(dst)
		return fmt.Errorf("stage prepared segment: %w", err)
	}
	if err := staging.Sync(); err != nil {
		_ = staging.Close()
		_ = os.Remove(dst)
		return fmt.Errorf("sync segment staging file: %w", err)
	}
	if err := staging.Close(); err != nil {
		_ = os.Remove(dst)
		return fmt.Errorf("close segment staging file: %w", err)
	}
	return nil
}
