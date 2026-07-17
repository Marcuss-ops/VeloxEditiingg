// Package youtube provides YouTube API integration for the Velox server.
// This file contains video upload functionality.
package youtube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/youtube/v3"
)

const (
	uploadMaxAttempts    = 3
	uploadRetryBaseDelay = 2 * time.Second
)

// Uploader handles video uploads to YouTube
type Uploader struct {
	service *Service
}

// NewUploader creates a new Uploader
func NewUploader(s *Service) *Uploader {
	return &Uploader{
		service: s,
	}
}

// UploadVideo uploads a video to YouTube
func (u *Uploader) UploadVideo(ctx context.Context, channelID string, videoPath string, config UploadConfig) (*UploadResult, error) {
	// 1. Quota Check
	if !u.service.quotaManager.CheckQuota() {
		return nil, fmt.Errorf("YouTube API quota exceeded or threshold reached (90%%). Upload blocked for safety.")
	}

	service, err := u.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get YouTube service: %w", err)
	}

	// Open the video file
	file, err := os.Open(videoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open video file: %w", err)
	}
	defer file.Close()

	log.Printf("[UPLOAD] YouTube upload started: channel=%s file=%s title=%q privacy=%s", channelID, filepath.Base(videoPath), config.Title, config.PrivacyStatus)

	// Prepare video metadata
	video := &youtube.Video{
		Snippet: &youtube.VideoSnippet{
			Title:       config.Title,
			Description: config.Description,
			Tags:        config.Tags,
			CategoryId:  config.CategoryID,
		},
		Status: &youtube.VideoStatus{
			PrivacyStatus:           config.PrivacyStatus,
			PublishAt:               config.PublishAt,
			SelfDeclaredMadeForKids: false,
		},
	}

	// Stamp the idempotency token as a tag so retries of the same delivery
	// are traceable to the canonical delivery_id.
	if config.IdempotencyToken != "" {
		video.Snippet.Tags = append(video.Snippet.Tags, "velox-delivery:"+config.IdempotencyToken)
	}

	// Set default category if not specified
	if video.Snippet.CategoryId == "" {
		video.Snippet.CategoryId = "22" // People & Blogs
	}

	var response *youtube.Video
	var uploadErr error

	// Upload video with retry for transient errors only.
	for attempt := 1; attempt <= uploadMaxAttempts; attempt++ {
		if attempt > 1 {
			delay := uploadRetryDelay(attempt - 1)
			log.Printf("[UPLOAD] YouTube upload retry %d/%d in %s: %v", attempt, uploadMaxAttempts, delay, uploadErr)
			if err := sleepWithContext(ctx, delay); err != nil {
				return nil, fmt.Errorf("upload retry interrupted: %w", err)
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return nil, fmt.Errorf("failed to rewind video file for retry: %w", err)
			}
		}

		call := service.Videos.Insert([]string{"snippet", "status"}, video)
		call.Media(file)

		response, uploadErr = call.Do()
		if uploadErr == nil {
			break
		}

		if !isRetryableUploadError(uploadErr) || attempt == uploadMaxAttempts {
			return nil, fmt.Errorf("failed to upload video: %w", uploadErr)
		}
	}

	result := &UploadResult{
		ID:         response.Id,
		VideoID:    response.Id,
		Status:     string(response.Status.UploadStatus),
		YouTubeURL: fmt.Sprintf("https://www.youtube.com/watch?v=%s", response.Id),
	}

	// Upload thumbnail if provided
	if config.ThumbnailPath != "" {
		thumbResult, err := u.SetThumbnail(ctx, channelID, response.Id, config.ThumbnailPath)
		if err != nil {
			log.Printf("[WARN] Failed to upload thumbnail: %v", err)
		} else {
			result.ThumbnailURL = thumbResult
		}
	}

	log.Printf("[OK] YouTube upload completed: channel=%s video_id=%s url=%s", channelID, result.VideoID, result.YouTubeURL)
	u.service.quotaManager.TrackUsage(CostUpload)
	return result, nil
}

// SetThumbnail sets the thumbnail for a YouTube video
func (u *Uploader) SetThumbnail(ctx context.Context, channelID string, videoID string, thumbnailPath string) (string, error) {
	service, err := u.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return "", fmt.Errorf("failed to get YouTube service: %w", err)
	}

	file, err := os.Open(thumbnailPath)
	if err != nil {
		// Fallback to dark_editor temp directory if not found directly
		fallbackPath := filepath.Join("data", "dark_editor", "temp", filepath.Base(thumbnailPath))
		var fallbackErr error
		file, fallbackErr = os.Open(fallbackPath)
		if fallbackErr != nil {
			return "", fmt.Errorf("failed to open thumbnail file: %w (fallback: %v)", err, fallbackErr)
		}
	}
	defer file.Close()

	call := service.Thumbnails.Set(videoID)
	call.Media(file)

	response, err := call.Do()
	if err != nil {
		return "", fmt.Errorf("failed to set thumbnail: %w", err)
	}

	return response.Etag, nil
}

func uploadRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return uploadRetryBaseDelay * (1 << (attempt - 1))
}

func isRetryableUploadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}

		for _, item := range apiErr.Errors {
			reason := strings.ToLower(item.Reason)
			if strings.Contains(reason, "quota") || strings.Contains(reason, "ratelimit") || strings.Contains(reason, "rate_limit") {
				return true
			}
		}

		message := strings.ToLower(apiErr.Message)
		if strings.Contains(message, "quota") || strings.Contains(message, "rate limit") || strings.Contains(message, "backend error") {
			return true
		}
	}

	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "quota"):
		return true
	case strings.Contains(message, "rate limit"):
		return true
	case strings.Contains(message, "timeout"):
		return true
	case strings.Contains(message, "temporary"):
		return true
	case strings.Contains(message, "connection reset"):
		return true
	case strings.Contains(message, "broken pipe"):
		return true
	case strings.Contains(message, "eof"):
		return true
	}

	return false
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
