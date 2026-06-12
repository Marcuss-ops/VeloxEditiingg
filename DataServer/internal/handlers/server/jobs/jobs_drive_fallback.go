package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"velox-server/internal/config"
	"velox-server/internal/integrations/drive"
	jobsservice "velox-server/internal/services/jobs"
)

type driveLinkRow struct {
	ID       string `json:"id" yaml:"id"`
	Name     string `json:"name" yaml:"name"`
	Link     string `json:"link" yaml:"link"`
	ParentID string `json:"parentId" yaml:"parentId"`
	Language string `json:"language" yaml:"language"`
}



func (api *JobAPI) tryDriveFallbackUpload(jobID string) {
	log.Printf("[CLOUD] Drive fallback triggered for job %s", jobID)

	if api == nil || api.fileQ == nil || api.cfg == nil {
		log.Printf("[CLOUD] Drive fallback skipped: api=%v, fileQ=%v, cfg=%v", api != nil, api.fileQ != nil, api.cfg != nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	job, err := api.fileQ.GetJobAsMap(ctx, jobID)
	if err != nil || job == nil {
		log.Printf("[CLOUD] Drive fallback skipped: job not found or error: %v", err)
		return
	}
	if strings.ToUpper(strings.TrimSpace(toString(job["status"]))) != "COMPLETED" {
		log.Printf("[CLOUD] Drive fallback skipped: status=%s", job["status"])
		return
	}
	if strings.TrimSpace(toString(job["drive_url"])) != "" {
		log.Printf("[CLOUD] Drive fallback skipped: drive_url already set")
		return
	}
	if hasDriveSuccess(job["last_drive_upload_result"]) {
		log.Printf("[CLOUD] Drive fallback skipped: drive upload already successful")
		return
	}

	videoPath := resolveVideoPath(api.cfg.VideosDir, jobID, job)
	if videoPath == "" {
		log.Printf("[CLOUD] Drive fallback skipped: video file not found")
		if err := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   "fallback drive skipped: master video file not found",
			},
		}); err != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, err)
		}
		return
	}
	log.Printf("[CLOUD] Drive fallback: video found at %s", videoPath)

	service, err := drive.NewService(&drive.ServiceConfig{
		ClientID:     api.cfg.DriveClientID,
		ClientSecret: api.cfg.DriveClientSecret,
		RedirectURI:  api.cfg.DriveRedirectURI,
		TokensDir:    api.cfg.DriveTokensDir,
	})
	if err != nil {
		log.Printf("[WARN] Drive fallback disabled for job %s: %v", jobID, err)
		return
	}

	token, err := resolveWorkingDriveToken(ctx, api.cfg, service)
	if err != nil {
		if updErr := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("fallback drive failed: %v", err),
			},
		}); updErr != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, updErr)
		}
		return
	}
	service.SetToken(token)

	groupName := resolveGroupName(job)
	projectName := resolveProjectName(job, videoPath)
	targetParentID, resolvedGroup, resolveErr := resolveVideoYoutubeGroupTarget(api.cfg.DataDir, groupName)
	if resolveErr != nil {
		if updErr := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("drive group mapping required: %v", resolveErr),
			},
		}); updErr != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, updErr)
		}
		return
	}

	// Upload hierarchy (master-side):
	// VideoYoutube / <Group> / <ProjectName> / <LanguageVariant> / final.mp4
	projectFolder, err := service.GetOrCreateFolder(ctx, projectName, targetParentID)
	if err != nil || projectFolder == nil || strings.TrimSpace(projectFolder.ID) == "" {
		msg := "failed to create project folder in mapped group"
		if err != nil {
			msg = err.Error()
		}
		if updErr := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   msg,
			},
		}); updErr != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, updErr)
		}
		return
	}

	uploadParentID := projectFolder.ID
	if variantFolderName := resolveLanguageVariantFolderName(job); variantFolderName != "" {
		variantFolder, vErr := service.GetOrCreateFolder(ctx, variantFolderName, projectFolder.ID)
		if vErr != nil || variantFolder == nil || strings.TrimSpace(variantFolder.ID) == "" {
			msg := "failed to create language variant folder in project folder"
			if vErr != nil {
				msg = vErr.Error()
			}
			if updErr := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
				"last_drive_upload_result": map[string]interface{}{
					"success": false,
					"error":   msg,
				},
			}); updErr != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, updErr)
		}
		return
	}
		uploadParentID = variantFolder.ID
	}

	result, err := service.UploadFile(ctx, videoPath, uploadParentID)
	if err != nil || result == nil || !result.Success {
		msg := "upload failed"
		if err != nil {
			msg = err.Error()
		} else if result != nil && strings.TrimSpace(result.Error) != "" {
			msg = result.Error
		}
		if updErr := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   msg,
			},
		}); updErr != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, updErr)
		}
		return
	}

	if err := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
		"drive_url": result.WebViewLink,
		"last_drive_upload_result": map[string]interface{}{
			"success":     true,
			"link":        result.WebViewLink,
			"file_id":     result.FileID,
			"folder_link": result.FolderLink,
			"group":       resolvedGroup,
			"project":     projectName,
			"variant":     resolveLanguageVariantFolderName(job),
			"uploaded_at": time.Now().UTC().Format(time.RFC3339),
			"source":      "master_fallback_youtube_not_attempted",
			"uploaded_by": "master",
		},
	}); err != nil {
		log.Printf("drive_fallback: final UpdateJobFields failed for %s: %v", jobID, err)
	}
	log.Printf("[CLOUD] Drive fallback completed for job %s: %s", jobID, result.WebViewLink)
}

func loadFirstDriveToken(tm *drive.TokenManager) (*drive.Token, error) {
	names, err := tm.ListTokens()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, os.ErrNotExist
	}
	sort.Strings(names)
	return tm.LoadToken(names[0])
}

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

func resolveGroupName(job map[string]interface{}) string {
	if s := strings.TrimSpace(toString(job["youtube_group"])); s != "" {
		return s
	}
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		if s := strings.TrimSpace(toString(slot["youtube_group"])); s != "" {
			return s
		}
	}
	return "Ungrouped"
}

func resolveProjectName(job map[string]interface{}, videoPath string) string {
	for _, key := range []string{"project_name", "video_name", "title", "youtube_title"} {
		if s := strings.TrimSpace(toString(job[key])); s != "" {
			return sanitizeDriveFolderName(s)
		}
	}
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		for _, key := range []string{"project_name", "video_name", "title", "youtube_title"} {
			if s := strings.TrimSpace(toString(slot[key])); s != "" {
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
		toString(job["audio_language_for_srt"]),
		toString(job["target_language"]),
		toString(job["language"]),
		toString(job["voice_language"]),
		toString(job["lang"]),
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
	// Keep folder names manageable.
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

func resolveVideoYoutubeGroupTarget(dataDir string, preferredGroup string) (groupDriveID string, resolvedGroup string, err error) {
	rows, rootDriveID, rootLocalID := loadVideoYoutubeRows(dataDir)
	if len(rows) == 0 || rootDriveID == "" {
		return "", "", fmt.Errorf("VideoYoutube root not found in drive_links.yaml/json")
	}

	if g := strings.TrimSpace(preferredGroup); g != "" && !strings.EqualFold(g, "Ungrouped") {
		if id := findVideoYoutubeChildDriveID(rows, rootLocalID, g); id != "" {
			return id, g, nil
		}
		return "", "", fmt.Errorf("group '%s' has no mapped child folder under VideoYoutube", g)
	}
	return "", "", fmt.Errorf("youtube group missing or invalid")
}

func loadVideoYoutubeRows(dataDir string) (rows []driveLinkRow, rootDriveID string, rootLocalID string) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, "", ""
	}
	candidates := []string{
		filepath.Join(dataDir, "drive", "drive_links.yaml"),
		filepath.Join(dataDir, "drive", "drive_links.yml"),
		filepath.Join(dataDir, "drive", "drive_links.json"),
	}
	for _, path := range candidates {
		raw, err := os.ReadFile(path)
		if err != nil || len(raw) == 0 {
			continue
		}
		rows = []driveLinkRow{}
		if strings.HasSuffix(strings.ToLower(path), ".json") {
			if err := json.Unmarshal(raw, &rows); err != nil {
				continue
			}
		} else {
			if err := yaml.Unmarshal(raw, &rows); err != nil {
				continue
			}
		}
		if len(rows) == 0 {
			continue
		}
		break
	}
	if len(rows) == 0 {
		return nil, "", ""
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

	// Phase 2: prefix/contains match with minimum length threshold to avoid
	// false positives on short names (e.g. "ai" matching "train").
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
			continue // already tried in phase 1
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

func resolveVideoPath(videosDir, jobID string, job map[string]interface{}) string {
	candidates := []string{
		strings.TrimSpace(toString(job["master_video_path"])),
	}
	if out, ok := job["worker_output"].(map[string]interface{}); ok {
		candidates = append(candidates, jobsservice.ExtractOutputVideoPath(out))
	}
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			return c
		}
	}

	if strings.TrimSpace(videosDir) == "" {
		return ""
	}
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
	return ""
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

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return ""
	}
}
