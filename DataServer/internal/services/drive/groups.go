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
	masterByNormName := indexMasterFoldersByNormalizedName(folders)
	groups := make(map[string]interface{})

	for group, clipName := range groupToClipFolder {
		clipID := findMasterIDInIndex(masterByNormName, clipName, group)
		voiceoverID := findMasterIDInIndex(masterByNormName, groupToVoiceoverFolder[group], group+" Voice")
		stockID := findMasterIDInIndex(masterByNormName, stockFolderAliases[group], group+" Stock")

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
	masterByNormName := indexMasterFoldersByNormalizedName(folders)
	result := make(map[string]interface{})

	if clipName, ok := groupToClipFolder[groupName]; ok {
		result["clip"] = findMasterIDInIndex(masterByNormName, clipName, groupName)
	}
	if stockName, ok := stockFolderAliases[groupName]; ok {
		result["stock"] = findMasterIDInIndex(masterByNormName, stockName, groupName+" Stock")
	}
	if voiceoverName, ok := groupToVoiceoverFolder[groupName]; ok {
		result["voiceover"] = findMasterIDInIndex(masterByNormName, voiceoverName, groupName+" Voice")
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

// findMasterIDByName keeps the original alias-order linear scan for the
// single-lookup callers (ClipFolderID) where building an index would cost
// more than the one scan it saves.
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

// indexMasterFoldersByNormalizedName precomputes a normalized-name → ID
// lookup over the root (parent-less) folders so multi-alias group resolution
// performs O(1) map lookups instead of re-scanning and re-normalizing the
// whole folder list for every alias (the old findMasterIDByName loop was
// O(names × folders) per call). The first folder in slice order wins, which
// preserves findMasterIDByName's first-match semantics exactly.
func indexMasterFoldersByNormalizedName(folders []DriveFolder) map[string]string {
	index := make(map[string]string, len(folders))
	for _, f := range folders {
		if f.ParentID != "" {
			continue
		}
		// An empty normalized key is deliberately indexed so a name that
		// normalizes to "" keeps the exact first-match behavior of
		// findMasterIDByName (both strip down to the same empty key).
		key := normalizeName(f.Name)
		if _, exists := index[key]; !exists {
			index[key] = f.ID
		}
	}
	return index
}

// findMasterIDInIndex resolves the first alias that hits the precomputed
// index, mirroring findMasterIDByName's alias-order + first-match behavior.
func findMasterIDInIndex(index map[string]string, names ...string) string {
	for _, name := range names {
		if id := index[normalizeName(name)]; id != "" {
			return id
		}
	}
	return ""
}
