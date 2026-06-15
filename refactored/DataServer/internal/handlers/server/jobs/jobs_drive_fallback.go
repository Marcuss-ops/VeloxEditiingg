package jobs

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/integrations/drive"
)

type driveLinkRow struct {
	ID       string `json:"id" yaml:"id"`
	Name     string `json:"name" yaml:"name"`
	Link     string `json:"link" yaml:"link"`
	ParentID string `json:"parentId" yaml:"parentId"`
	Language string `json:"language" yaml:"language"`
}

func (api *JobAPI) tryDriveFallbackUpload(jobID string) {
	// Recover from panics so a bug in the drive upload path never silently
	// orphans a job without drive_url.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[CLOUD] Drive fallback PANIC for job %s: %v", jobID, r)
			if api != nil && api.fileQ != nil && api.cfg != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
					"last_drive_upload_result": map[string]interface{}{
						"success": false,
						"error":   fmt.Sprintf("drive fallback panic: %v", r),
					},
				}); err != nil {
					log.Printf("drive_fallback: panic UpdateJobFields failed for %s: %v", jobID, err)
				}
			}
		}
	}()

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
		if err := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("job not found: %v", err),
			},
		}); err != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, err)
		}
		return
	}
	if strings.ToUpper(strings.TrimSpace(asJobString(job["status"]))) != "COMPLETED" {
		log.Printf("[CLOUD] Drive fallback skipped: status=%s", job["status"])
		return
	}
	if strings.TrimSpace(asJobString(job["drive_url"])) != "" {
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
		if updErr := api.fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
			"last_drive_upload_result": map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("drive service unavailable: %v", err),
			},
		}); updErr != nil {
			log.Printf("drive_fallback: UpdateJobFields failed for %s: %v", jobID, updErr)
		}
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
	resolvedGroup := groupName
	targetParentID := extractDriveFolderIDFromLink(asJobString(job["drive_output_folder"]))
	if targetParentID == "" {
		targetParentID = extractDriveFolderIDFromLink(asJobString(job["output_directory"]))
	}
	if targetParentID == "" {
		var resolveErr error
		targetParentID, resolvedGroup, resolveErr = resolveVideoYoutubeGroupTarget(api.dbStore, groupName)
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
	}

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
