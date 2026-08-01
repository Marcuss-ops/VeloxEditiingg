// Package drive / tokens.go — Drive token file listing sub-domain.
// Extracted from service.go: token file enumeration.
package drive

import (
	"os"
	"path/filepath"
	"strings"
)

// ListDriveTokens lists token files
func (s *Service) ListDriveTokens() ([]TokenFile, error) {
	if s.tokensDir == "" {
		return []TokenFile{}, nil
	}

	entries, err := os.ReadDir(s.tokensDir)
	if err != nil {
		return []TokenFile{}, nil
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
