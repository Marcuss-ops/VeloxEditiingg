// Package livestream provides livestream management handlers.
package livestream

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"velox-server/internal/integrations/youtube"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	yt "google.golang.org/api/youtube/v3"
)

type LiveStreamConfig struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	Platform            string    `json:"platform"`
	StreamKey           string    `json:"stream_key"`
	StreamURL           string    `json:"stream_url"`
	Description         string    `json:"description"`
	IsForKids           bool      `json:"is_for_kids"`
	VideoBitrate        int       `json:"video_bitrate"`
	AudioBitrate        int       `json:"audio_bitrate"`
	Status              string    `json:"status"` // "created", "testing", "live", "complete", "revoked"
	VideoOrder          string    `json:"video_order"`
	Protocol            string    `json:"protocol"`
	AutoStart           bool      `json:"auto_start"`
	AutoStop            bool      `json:"auto_stop"`
	ScheduledStartTime  string    `json:"scheduled_start_time,omitempty"`
	ScheduledEndTime    string    `json:"scheduled_end_time,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	Duration            int       `json:"duration"`
	MaxViewers          int       `json:"max_viewers"`
	LatencyPreference   string    `json:"latency_preference"`
	ChannelID           string    `json:"channel_id"`
	BroadcastID         string    `json:"broadcast_id,omitempty"`
	YouTubeStreamID     string    `json:"youtube_stream_id,omitempty"`
}

type LivestreamHandlers struct {
	ytService *youtube.Service
	filePath  string
	mu        sync.RWMutex
}

// NewLivestreamHandlers creates a new LivestreamHandlers instance.
func NewLivestreamHandlers(ytService *youtube.Service, dataDir string) *LivestreamHandlers {
	h := &LivestreamHandlers{
		ytService: ytService,
		filePath:  filepath.Join(dataDir, "livestreams.json"),
	}
	_ = os.MkdirAll(filepath.Dir(h.filePath), 0755)
	return h
}

func (h *LivestreamHandlers) loadStreams() ([]LiveStreamConfig, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if _, err := os.Stat(h.filePath); os.IsNotExist(err) {
		return []LiveStreamConfig{}, nil
	}

	data, err := ioutil.ReadFile(h.filePath)
	if err != nil {
		return nil, err
	}

	var streams []LiveStreamConfig
	if err := json.Unmarshal(data, &streams); err != nil {
		return nil, err
	}
	return streams, nil
}

func (h *LivestreamHandlers) saveStreams(streams []LiveStreamConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	data, err := json.MarshalIndent(streams, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(h.filePath, data, 0644)
}

// ListStreams returns a list of all livestreams.
func (h *LivestreamHandlers) ListStreams(c *gin.Context) {
	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"streams": streams})
}

// CreateStream creates a new livestream.
func (h *LivestreamHandlers) CreateStream(c *gin.Context) {
	var req LiveStreamConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	req.ID = uuid.New().String()
	req.Status = "created"
	req.CreatedAt = time.Now()
	req.LatencyPreference = "normal"

	if req.Platform == "youtube" && h.ytService != nil {
		channelID := req.ChannelID
		if channelID == "" {
			channels := h.ytService.GetChannels()
			if len(channels) > 0 {
				channelID = channels[0].ID
				req.ChannelID = channelID
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "No authenticated YouTube channels found"})
				return
			}
		}

		ctx := c.Request.Context()
		ytClient, err := h.ytService.GetYouTubeService(ctx, channelID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("Failed to get YouTube service: %v", err)})
			return
		}

		// 1. Create LiveBroadcast
		broadcast := &yt.LiveBroadcast{
			Snippet: &yt.LiveBroadcastSnippet{
				Title:       req.Name,
				Description: req.Description,
			},
			Status: &yt.LiveBroadcastStatus{
				PrivacyStatus:           "private", // Default to private for safety
				SelfDeclaredMadeForKids: req.IsForKids,
			},
		}

		if req.ScheduledStartTime != "" {
			broadcast.Snippet.ScheduledStartTime = req.ScheduledStartTime
		} else {
			broadcast.Snippet.ScheduledStartTime = time.Now().Add(10 * time.Minute).Format(time.RFC3339)
		}

		if req.ScheduledEndTime != "" {
			broadcast.Snippet.ScheduledEndTime = req.ScheduledEndTime
		}

		bCall := ytClient.LiveBroadcasts.Insert([]string{"snippet", "status"}, broadcast)
		createdBroadcast, err := bCall.Do()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("Failed to create YouTube LiveBroadcast: %v", err)})
			return
		}
		req.BroadcastID = createdBroadcast.Id

		// 2. Create LiveStream Ingest
		stream := &yt.LiveStream{
			Snippet: &yt.LiveStreamSnippet{
				Title: req.Name + " Ingest",
			},
			Cdn: &yt.CdnSettings{
				IngestionType: "rtmp",
				FrameRate:     "variable",
				Resolution:    "variable",
			},
		}
		sCall := ytClient.LiveStreams.Insert([]string{"snippet", "cdn"}, stream)
		createdStream, err := sCall.Do()
		if err != nil {
			_ = ytClient.LiveBroadcasts.Delete(createdBroadcast.Id).Do()
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("Failed to create YouTube LiveStream: %v", err)})
			return
		}
		req.YouTubeStreamID = createdStream.Id
		req.StreamKey = createdStream.Cdn.IngestionInfo.StreamName
		req.StreamURL = createdStream.Cdn.IngestionInfo.IngestionAddress

		// 3. Bind Broadcast and Stream
		bindCall := ytClient.LiveBroadcasts.Bind(createdBroadcast.Id, []string{"id"}).StreamId(createdStream.Id)
		_, err = bindCall.Do()
		if err != nil {
			_ = ytClient.LiveStreams.Delete(createdStream.Id).Do()
			_ = ytClient.LiveBroadcasts.Delete(createdBroadcast.Id).Do()
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("Failed to bind YouTube Broadcast and Stream: %v", err)})
			return
		}
	}

	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	streams = append(streams, req)
	if err := h.saveStreams(streams); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, req)
}

// GetStream returns a specific livestream by ID.
func (h *LivestreamHandlers) GetStream(c *gin.Context) {
	id := c.Param("id")
	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	for _, s := range streams {
		if s.ID == id {
			c.JSON(http.StatusOK, s)
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
}

// UpdateStream updates a livestream.
func (h *LivestreamHandlers) UpdateStream(c *gin.Context) {
	id := c.Param("id")
	var req LiveStreamConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	idx := -1
	for i, s := range streams {
		if s.ID == id {
			idx = i
			break
		}
	}

	if idx == -1 {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	streams[idx].Name = req.Name
	streams[idx].Description = req.Description
	streams[idx].VideoOrder = req.VideoOrder
	streams[idx].AutoStart = req.AutoStart
	streams[idx].AutoStop = req.AutoStop
	streams[idx].ScheduledStartTime = req.ScheduledStartTime
	streams[idx].ScheduledEndTime = req.ScheduledEndTime

	if err := h.saveStreams(streams); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, streams[idx])
}

// DeleteStream deletes a livestream.
func (h *LivestreamHandlers) DeleteStream(c *gin.Context) {
	id := c.Param("id")
	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	idx := -1
	for i, s := range streams {
		if s.ID == id {
			idx = i
			break
		}
	}

	if idx == -1 {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	stream := streams[idx]

	if stream.Platform == "youtube" && stream.BroadcastID != "" && h.ytService != nil {
		ctx := c.Request.Context()
		ytClient, err := h.ytService.GetYouTubeService(ctx, stream.ChannelID)
		if err == nil {
			_ = ytClient.LiveBroadcasts.Delete(stream.BroadcastID).Do()
			if stream.YouTubeStreamID != "" {
				_ = ytClient.LiveStreams.Delete(stream.YouTubeStreamID).Do()
			}
		}
	}

	streams = append(streams[:idx], streams[idx+1:]...)
	if err := h.saveStreams(streams); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true, "id": id, "deleted": true})
}

// GetStatus returns the status of livestream services.
func (h *LivestreamHandlers) GetStatus(c *gin.Context) {
	streamID := c.Query("stream_id")
	if streamID == "" {
		c.JSON(http.StatusOK, gin.H{"status": "operational"})
		return
	}

	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	for _, s := range streams {
		if s.ID == streamID {
			health := "good"
			bitrate := 4500.0
			fps := 30
			res := "1080p"

			if s.Platform == "youtube" && s.YouTubeStreamID != "" && h.ytService != nil {
				ctx := c.Request.Context()
				ytClient, err := h.ytService.GetYouTubeService(ctx, s.ChannelID)
				if err == nil {
					call := ytClient.LiveStreams.List([]string{"status"}).Id(s.YouTubeStreamID)
					resp, err := call.Do()
					if err == nil && len(resp.Items) > 0 {
						ytStatus := resp.Items[0].Status
						if ytStatus.StreamStatus == "inactive" {
							health = "error"
						} else if ytStatus.HealthStatus != nil {
							health = ytStatus.HealthStatus.Status
						}
					}
				}
			}

			c.JSON(http.StatusOK, gin.H{
				"ok":     true,
				"id":     s.ID,
				"status": s.Status,
				"health": gin.H{
					"status":      health,
					"bitrate":     bitrate,
					"framerate":   fps,
					"resolution":  res,
					"packetsLost": 0,
				},
			})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
}

func (h *LivestreamHandlers) transitionStream(c *gin.Context, transitionState string) {
	id := c.Param("id")
	streams, err := h.loadStreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	idx := -1
	for i, s := range streams {
		if s.ID == id {
			idx = i
			break
		}
	}

	if idx == -1 {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	stream := streams[idx]

	if stream.Platform == "youtube" && stream.BroadcastID != "" && h.ytService != nil {
		ctx := c.Request.Context()
		ytClient, err := h.ytService.GetYouTubeService(ctx, stream.ChannelID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		call := ytClient.LiveBroadcasts.Transition(transitionState, stream.BroadcastID, []string{"id", "status"})
		_, err = call.Do()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("YouTube transition to %s failed: %v", transitionState, err)})
			return
		}
	}

	streams[idx].Status = transitionState
	if err := h.saveStreams(streams); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true, "id": id, "status": transitionState})
}

// StartTesting starts testing mode for a livestream.
func (h *LivestreamHandlers) StartTesting(c *gin.Context) {
	h.transitionStream(c, "testing")
}

// GoLive starts a livestream.
func (h *LivestreamHandlers) GoLive(c *gin.Context) {
	h.transitionStream(c, "live")
}

// EndStream ends a livestream.
func (h *LivestreamHandlers) EndStream(c *gin.Context) {
	h.transitionStream(c, "complete")
}
