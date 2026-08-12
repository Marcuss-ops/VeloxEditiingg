// Package drive / tokens.go — Drive token file listing sub-domain.
// Extracted from service.go: token file enumeration.
package drive

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ListDriveTokens lists token files
func (s *Service) ListDriveTokens() ([]TokenFile, error) {
	if s == nil {
		return nil, fmt.Errorf("drive: service not configured")
	}
	if s.tokensDir == "" {
		return nil, fmt.Errorf("drive: tokens directory not configured")
	}

	entries, err := os.ReadDir(s.tokensDir)
	if err != nil {
		return nil, fmt.Errorf("drive: list tokens directory: %w", err)
	}

	var files []TokenFile
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, TokenFile{
				Name: entry.Name(),
				Path: filepath.Join(s.tokensDir, entry.Name()),
			})
		}
	}
	return files, nil
}
