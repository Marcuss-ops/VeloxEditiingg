package jobs

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/integrations/drive"
	jobsservice "velox-server/internal/services/jobs"
	"velox-server/internal/store"
	"velox-shared/paths"
)

// =============================================================================
// Drive token resolution
// =============================================================================

func loadAllDriveTokens(tm *drive.TokenManager) ([]*drive.Token, error) {
	names, err := tm.ListTokens()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, os.ErrNotExist
	}
	sort.Strings(names)
	out := make([]*drive.Token, 0, len(names))
	for _, name := range names {
		tok, tokErr := tm.LoadToken(name)
		if tokErr != nil || tok == nil {
			continue
		}
		out = append(out, tok)
	}
	if len(out) == 0 {
		return nil, os.ErrNotExist
	}
	return out, nil
}

func resolveWorkingDriveToken(ctx context.Context, cfg *config.Config, service *drive.Service) (*drive.Token, error) {
	if service == nil || cfg == nil {
		return nil, fmt.Errorf("drive service/config unavailable")
	}

	tokenDirs := []string{}
	if dir := strings.TrimSpace(cfg.DriveTokensDir); dir != "" {
		tokenDirs = append(tokenDirs, dir)
	}
	if dataDir := strings.TrimSpace(cfg.DataDir); dataDir != "" {
		tokenDirs = append(tokenDirs,
			filepath.Join(dataDir, "drive", "tokens"),
			filepath.Join(dataDir, "drive", "Token_Backup_Invalid"),
		)
	}

	seen := map[string]bool{}
	uniqueDirs := make([]string, 0, len(tokenDirs))
	for _, d := range tokenDirs {
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		uniqueDirs = append(uniqueDirs, d)
	}

	var lastErr error = fmt.Errorf("no configured Drive token/account")
	for _, tokenDir := range uniqueDirs {
		tm, tmErr := drive.NewTokenManager(tokenDir)
		if tmErr != nil {
			lastErr = tmErr
			continue
		}
		tokens, tokErr := loadAllDriveTokens(tm)
		if tokErr != nil {
			lastErr = tokErr
			continue
		}
		for _, tok := range tokens {
			service.SetToken(tok)
			if _, aboutErr := service.GetAbout(ctx); aboutErr == nil {
				log.Printf("[OK] Drive fallback: token validated from %s", tokenDir)
				return tok, nil
			} else {
				lastErr = aboutErr
			}
		}
	}
	return nil, lastErr
}

// =============================================================================
// Group / project / variant name resolution
// =============================================================================

func resolveGroupName(job map[string]interface{}) string {
	if s := strings.TrimSpace(asJobString(job["youtube_group"])); s != "" {
		return s
	}
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		if s := strings.TrimSpace(asJobString(slot["youtube_group"])); s != "" {
			return s
		}
	}
	return "Ungrouped"
}

func resolveProjectName(job map[string]interface{}, videoPath string) string {
	for _, key := range []string{"project_name", "video_name", "title", "youtube_title"} {
		if s := strings.TrimSpace(asJobString(job[key])); s != "" {
			return sanitizeDriveFolderName(s)
		}
	}
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		for _, key := range []string{"project_name", "video_name", "title", "youtube_title"} {
			if s := strings.TrimSpace(asJobString(slot[key])); s != "" {
				return sanitizeDriveFolderName(s)
			}
		}
	}
	if base := strings.TrimSpace(strings.TrimSuffix(filepath.Base(videoPath), filepath.Ext(videoPath))); base != "" {
		return sanitizeDriveFolderName(base)
	}
	return "Project"
}

func resolveLanguageVariantFolderName(job map[string]interface{}) string {
	candidates := []string{
		asJobString(job["audio_language_for_srt"]),
		asJobString(job["target_language"]),
		asJobString(job["language"]),
		asJobString(job["voice_language"]),
		asJobString(job["lang"]),
	}
	for _, candidate := range candidates {
		if s := sanitizeDriveFolderName(strings.TrimSpace(candidate)); s != "" && !strings.EqualFold(s, "Project") {
			return s
		}
	}
	return ""
}

func sanitizeDriveFolderName(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "Project"
	}
	invalid := regexp.MustCompile(`[\\/:*?"<>|]+`)
	s = invalid.ReplaceAllString(s, " ")
	s = strings.Join(strings.Fields(s), " ")
	if s == "" {
		return "Project"
	}
	if len(s) > 120 {
		s = strings.TrimSpace(s[:120])
	}
	return s
}

func normalizeDriveFolderName(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var result strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// =============================================================================
// VideoYoutube group target resolution (SQLite drive_links)
// =============================================================================

func resolveVideoYoutubeGroupTarget(dbStore *store.SQLiteStore, preferredGroup string) (groupDriveID string, resolvedGroup string, err error) {
	rows, rootDriveID, rootLocalID := loadVideoYoutubeRowsFromDB(dbStore)
	if len(rows) == 0 || rootDriveID == "" {
		return "", "", fmt.Errorf("VideoYoutube root not found in drive_links")
	}

	if g := strings.TrimSpace(preferredGroup); g != "" && !strings.EqualFold(g, "Ungrouped") {
		if id := findVideoYoutubeChildDriveID(rows, rootLocalID, g); id != "" {
			return id, g, nil
		}
		return "", "", fmt.Errorf("group '%s' has no mapped child folder under VideoYoutube", g)
	}
	return "", "", fmt.Errorf("youtube group missing or invalid")
}

func loadVideoYoutubeRowsFromDB(dbStore *store.SQLiteStore) (rows []driveLinkRow, rootDriveID string, rootLocalID string) {
	if dbStore == nil {
		return nil, "", ""
	}
	dbLinks, err := dbStore.ListDriveLinks()
	if err != nil || len(dbLinks) == 0 {
		return nil, "", ""
	}

	rows = make([]driveLinkRow, 0, len(dbLinks))
	for _, link := range dbLinks {
		row := driveLinkRow{
			ID:       asJobString(link["id"]),
			Name:     asJobString(link["name"]),
			Link:     asJobString(link["link"]),
			ParentID: asJobString(link["parentId"]),
			Language: asJobString(link["language"]),
		}
		rows = append(rows, row)
	}

	for _, r := range rows {
		if strings.EqualFold(strings.TrimSpace(r.Name), "VideoYoutube") {
			rootLocalID = strings.TrimSpace(r.ID)
			if id := extractDriveIDFromLink(r.Link); id != "" {
				return rows, id, rootLocalID
			}
			if id := strings.TrimSpace(r.ID); id != "" && !strings.HasPrefix(id, "folder-") {
				return rows, id, rootLocalID
			}
		}
	}
	return rows, "", rootLocalID
}

func findVideoYoutubeChildDriveID(rows []driveLinkRow, rootLocalID string, childName string) string {
	name := strings.TrimSpace(childName)
	if name == "" || rootLocalID == "" {
		return ""
	}
	normChild := normalizeDriveFolderName(name)

	// Phase 1: exact normalized match (highest confidence)
	for _, r := range rows {
		if !strings.EqualFold(strings.TrimSpace(r.ParentID), rootLocalID) {
			continue
		}
		if normalizeDriveFolderName(r.Name) == normChild {
			if id := extractDriveIDFromLink(r.Link); id != "" {
				return id
			}
			if id := strings.TrimSpace(r.ID); id != "" && !strings.HasPrefix(id, "folder-") {
				return id
			}
		}
	}

	// Phase 2: prefix/contains match with minimum length threshold
	minFuzzyLen := 4
	if len(normChild) < minFuzzyLen {
		return ""
	}
	for _, r := range rows {
		if !strings.EqualFold(strings.TrimSpace(r.ParentID), rootLocalID) {
			continue
		}
		normRow := normalizeDriveFolderName(r.Name)
		if len(normRow) < minFuzzyLen {
			continue
		}
		if normRow == normChild {
			continue
		}
		if strings.HasPrefix(normRow, normChild) || strings.HasPrefix(normChild, normRow) ||
			strings.Contains(normRow, normChild) || strings.Contains(normChild, normRow) {
			if id := extractDriveIDFromLink(r.Link); id != "" {
				return id
			}
			if id := strings.TrimSpace(r.ID); id != "" && !strings.HasPrefix(id, "folder-") {
				return id
			}
		}
	}
	return ""
}

// =============================================================================
// Video path resolution
// =============================================================================

func resolveVideoPath(videosDir, jobID string, job map[string]interface{}) string {
	candidates := []string{
		strings.TrimSpace(asJobString(job["master_video_path"])),
		strings.TrimSpace(asJobString(job["output_path"])),
		strings.TrimSpace(asJobString(job["result_path_worker"])),
	}
	if base := strings.TrimSpace(asJobString(job["video_name"])); base != "" {
		jobRunID := strings.TrimSpace(asJobString(job["job_run_id"]))
		if jobRunID == "" {
			jobRunID = strings.TrimSpace(asJobString(job["run_id"]))
		}
		outputVideoID := strings.TrimSpace(asJobString(job["output_video_id"]))
		if outputVideoID == "" {
			outputVideoID = jobID
		}
		if jobRunID != "" && strings.TrimSpace(videosDir) != "" {
			slug := paths.SanitizeVideoName(base)
			if slug != "" {
				candidates = append(candidates,
					filepath.Join(videosDir, fmt.Sprintf("%s_%s_%s.mp4", slug, outputVideoID, jobRunID)),
					filepath.Join(videosDir, fmt.Sprintf("%s_%s_%s.mov", slug, outputVideoID, jobRunID)),
				)
			}
		}
	}
	if out, ok := job["worker_output"].(map[string]interface{}); ok {
		candidates = append(candidates, jobsservice.ExtractOutputVideoPath(out))
	}

	checkPath := func(path string) string {
		path = strings.TrimSpace(path)
		if path == "" {
			return ""
		}
		if st, err := os.Stat(path); err == nil && !st.IsDir() {
			return path
		}
		if !filepath.IsAbs(path) && strings.TrimSpace(videosDir) != "" {
			if joined := filepath.Join(videosDir, path); joined != path {
				if st, err := os.Stat(joined); err == nil && !st.IsDir() {
					return joined
				}
			}
			if joined := filepath.Join(videosDir, filepath.Base(path)); joined != path {
				if st, err := os.Stat(joined); err == nil && !st.IsDir() {
					return joined
				}
			}
		}
		if abs, err := filepath.Abs(path); err == nil {
			if st, err := os.Stat(abs); err == nil && !st.IsDir() {
				return abs
			}
		}
		return ""
	}

	for _, c := range candidates {
		if resolved := checkPath(c); resolved != "" {
			return resolved
		}
	}

	if strings.TrimSpace(videosDir) != "" {
		patterns := []string{
			filepath.Join(videosDir, "*"+jobID+"*.mp4"),
			filepath.Join(videosDir, "*"+jobID+"*.mov"),
		}
		for _, pattern := range patterns {
			matches, _ := filepath.Glob(pattern)
			for _, m := range matches {
				if st, err := os.Stat(m); err == nil && !st.IsDir() {
					return m
				}
			}
		}
	}
	return ""
}

// =============================================================================
// Drive ID extraction helpers
// =============================================================================

func extractDriveFolderIDFromLink(link string) string {
	link = strings.TrimSpace(link)
	if link == "" {
		return ""
	}
	parts := []string{"/folders/", "id="}
	for _, marker := range parts {
		if idx := strings.Index(link, marker); idx >= 0 {
			rest := link[idx+len(marker):]
			if end := strings.IndexAny(rest, "/?&"); end > 0 {
				rest = rest[:end]
			}
			if rest != "" {
				return strings.TrimSpace(rest)
			}
		}
	}
	if strings.Contains(link, "drive.google.com") {
		return ""
	}
	return link
}

func extractDriveIDFromLink(link string) string {
	link = strings.TrimSpace(link)
	if link == "" {
		return ""
	}
	parts := strings.Split(link, "/folders/")
	if len(parts) < 2 {
		return ""
	}
	id := parts[1]
	if idx := strings.Index(id, "?"); idx >= 0 {
		id = id[:idx]
	}
	return strings.TrimSpace(id)
}

func hasDriveSuccess(v interface{}) bool {
	m, ok := v.(map[string]interface{})
	if !ok || m == nil {
		return false
	}
	b, ok := m["success"].(bool)
	return ok && b
}


