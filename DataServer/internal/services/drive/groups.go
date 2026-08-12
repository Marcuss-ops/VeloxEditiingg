// Package drive / groups.go — group mappings and group folder resolution.
// Extracted from service.go: group→folder maps and lookups.
package drive

import "fmt"

// Group mappings
var groupToClipFolder = map[string]string{
	"wwe":    "WWE",
	"hiphop": "Hip Hop",
	"news":   "News",
	"tech":   "Tech",
}

var groupToVoiceoverFolder = map[string]string{
	"wwe":    "WWE Voice",
	"hiphop": "Hip Hop Voice",
	"news":   "News Voice",
	"tech":   "Tech Voice",
}

var stockFolderAliases = map[string]string{
	"wwe":    "WWE Stock",
	"hiphop": "Hip Hop Stock",
	"news":   "News Stock",
	"tech":   "Tech Stock",
}

// GetDriveGroups builds group structures
func (s *Service) GetDriveGroups() (map[string]interface{}, error) {
	folders, err := s.getLinks()
	if err != nil {
		return nil, err
	}
	groups := make(map[string]interface{})

	for group, clipName := range groupToClipFolder {
		clipID := findMasterIDByName(folders, []string{clipName, group})
		voiceoverID := findMasterIDByName(folders, []string{groupToVoiceoverFolder[group], group + " Voice"})
		stockID := findMasterIDByName(folders, []string{stockFolderAliases[group], group + " Stock"})

		if clipID != "" || voiceoverID != "" || stockID != "" {
			groups[group] = map[string]interface{}{
				"clip":      clipID,
				"voiceover": voiceoverID,
				"stock":     stockID,
			}
		}
	}
	return groups, nil
}

// GroupFolders resolves group folder mappings
func (s *Service) GroupFolders(groupName string) (map[string]interface{}, error) {
	folders, err := s.getLinks()
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})

	if clipName, ok := groupToClipFolder[groupName]; ok {
		result["clip"] = findMasterIDByName(folders, []string{clipName, groupName})
	}
	if stockName, ok := stockFolderAliases[groupName]; ok {
		result["stock"] = findMasterIDByName(folders, []string{stockName, groupName + " Stock"})
	}
	if voiceoverName, ok := groupToVoiceoverFolder[groupName]; ok {
		result["voiceover"] = findMasterIDByName(folders, []string{voiceoverName, groupName + " Voice"})
	}
	return result, nil
}

// ClipFolderID finds folder ID by name or group
func (s *Service) ClipFolderID(folderName, group string) (map[string]interface{}, error) {
	folders, err := s.getLinks()
	if err != nil {
		return nil, err
	}

	if folderName != "" {
		for _, f := range folders {
			if normalizeName(f.Name) == normalizeName(folderName) {
				return map[string]interface{}{
					"id":   f.ID,
					"name": f.Name,
				}, nil
			}
		}
	}

	if group != "" {
		if clipName, ok := groupToClipFolder[group]; ok {
			clipID := findMasterIDByName(folders, []string{clipName, group})
			if clipID != "" {
				return map[string]interface{}{
					"id":    clipID,
					"group": group,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("%w", ErrFolderNotFound)
}

// Helpers
func findMasterIDByName(folders []DriveFolder, names []string) string {
	for _, name := range names {
		normName := normalizeName(name)
		for _, f := range folders {
			if f.ParentID == "" && normalizeName(f.Name) == normName {
				return f.ID
			}
		}
	}
	return ""
}
