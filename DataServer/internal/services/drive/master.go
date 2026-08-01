// Package drive / master.go — master folders sub-domain of the Drive service.
// Extracted from service.go: master folder reads and upserts.
package drive

import "fmt"

// GetMasterFolders returns master folders
func (s *Service) GetMasterFolders() (map[string]interface{}, error) {
	masters := make(map[string]interface{})
	if s.store != nil {
		dbMasters, err := s.store.ListMasterFolders()
		if err == nil && len(dbMasters) > 0 {
			for _, m := range dbMasters {
				language, _ := m["language"].(string)
				if language == "" {
					continue
				}
				masters[language] = m
			}
		}
	}
	return masters, nil
}

// UpsertMasterFolder upserts a master folder
func (s *Service) UpsertMasterFolder(req UpsertMasterFolderRequest) error {
	if s.store == nil {
		return fmt.Errorf("drive store not initialized")
	}
	return s.store.UpsertMasterFolder(req.ID, req.Name, req.URL, req.Language, req.SubfoldersCount)
}

// ListOutroFolders retrieves all detailed master folder entries from the database.
func (s *Service) ListOutroFolders() ([]map[string]any, error) {
	if s.store == nil {
		return nil, fmt.Errorf("drive store not configured")
	}
	return s.store.ListMasterFoldersDetailed()
}

// FindMasterFolderByLanguage retrieves a master folder by its language tag.
func (s *Service) FindMasterFolderByLanguage(language string) (map[string]any, error) {
	if s.store == nil {
		return nil, fmt.Errorf("drive store not configured")
	}
	return s.store.FindMasterFolderByLanguage(language)
}
