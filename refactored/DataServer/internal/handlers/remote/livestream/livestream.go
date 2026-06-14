// Package livestream provides livestream management handlers backed by SQLite.
package livestream

import (
	"fmt"
	"net/http"
	"time"

	"velox-server/internal/integrations/youtube"
	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	yt "google.golang.org/api/youtube/v3"
)

type LiveStreamConfig struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	Platform           string    `json:"platform"`
	StreamKey          string    `json:"stream_key"`
	StreamURL          string    `json:"stream_url"`
	Description        string    `json:"description"`
	IsForKids          bool      `json:"is_for_kids"`
	VideoBitrate       int       `json:"video_bitrate"`
	AudioBitrate       int       `json:"audio_bitrate"`
	Status             string    `json:"status"` // "created", "testing", "live", "complete", "revoked"
	VideoOrder         string    `json:"video_order"`
	Protocol           string    `json:"protocol"`
	AutoStart          bool      `json:"auto_start"`
	AutoStop           bool      `json:"auto_stop"`
	ScheduledStartTime string    `json:"scheduled_start_time,omitempty"`
	ScheduledEndTime   string    `json:"scheduled_end_time,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	Duration           int       `json:"duration"`
	MaxViewers         int       `json:"max_viewers"`
	LatencyPreference  string    `json:"latency_preference"`
	ChannelID          string    `json:"channel_id"`
	BroadcastID        string    `json:"broadcast_id,omitempty"`
	YouTubeStreamID    string    `json:"youtube_stream_id,omitempty"`
}

type LivestreamHandlers struct {
	ytService *youtube.Service
	dbStore   *store.SQLiteStore
}

// NewLivestreamHandlers creates a new LivestreamHandlers instance.
func NewLivestreamHandlers(ytService *youtube.Service, dbStore *store.SQLiteStore) *LivestreamHandlers {
	return &LivestreamHandlers{
		ytService: ytService,
		dbStore:   dbStore,
	}
}

// ListStreams returns a list of all livestreams.
func (h *LivestreamHandlers) ListStreams(c *gin.Context) {
	rows, err := h.dbStore.ListLivestreams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"streams": store.ToLivestreamConfigs(rows)})
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
				PrivacyStatus:           "private",
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

	// Persist to SQLite
	cfg := map[string]interface{}{
		"id": req.ID, "name": req.Name, "platform": req.Platform,
		"stream_key": req.StreamKey, "stream_url": req.StreamURL,
		"description": req.Description, "is_for_kids": req.IsForKids,
		"video_bitrate": req.VideoBitrate, "audio_bitrate": req.AudioBitrate,
		"status": req.Status, "video_order": req.VideoOrder,
		"protocol": req.Protocol, "auto_start": req.AutoStart,
		"auto_stop": req.AutoStop, "scheduled_start_time": req.ScheduledStartTime,
		"scheduled_end_time": req.ScheduledEndTime,
		"created_at": req.CreatedAt.Format(time.RFC3339),
		"duration": req.Duration, "max_viewers": req.MaxViewers,
		"latency_preference": req.LatencyPreference,
		"channel_id": req.ChannelID, "broadcast_id": req.BroadcastID,
		"youtube_stream_id": req.YouTubeStreamID,
	}
	if err := h.dbStore.UpsertLivestream(store.ConfigToRow(cfg)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, req)
}

// GetStream returns a specific livestream by ID.
func (h *LivestreamHandlers) GetStream(c *gin.Context) {
	id := c.Param("id")
	row, err := h.dbStore.GetLivestream(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}
	if row == nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}
	c.JSON(http.StatusOK, store.ToLivestreamConfigs([]*store.LivestreamRow{row})[0])
}

// UpdateStream updates a livestream.
func (h *LivestreamHandlers) UpdateStream(c *gin.Context) {
	id := c.Param("id")
	var req LiveStreamConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	row, err := h.dbStore.GetLivestream(id)
	if err != nil || row == nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	row.Name = req.Name
	row.Description = req.Description
	row.VideoOrder = req.VideoOrder
	row.AutoStart = req.AutoStart
	row.AutoStop = req.AutoStop
	row.SchedStart = req.ScheduledStartTime
	row.SchedEnd = req.ScheduledEndTime

	if err := h.dbStore.UpsertLivestream(row); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, store.ToLivestreamConfigs([]*store.LivestreamRow{row})[0])
}

// DeleteStream deletes a livestream.
func (h *LivestreamHandlers) DeleteStream(c *gin.Context) {
	id := c.Param("id")
	row, err := h.dbStore.GetLivestream(id)
	if err != nil || row == nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	// Delete YouTube resources if present
	if row.Platform == "youtube" && row.BroadcastID != "" && h.ytService != nil {
		ctx := c.Request.Context()
		ytClient, err := h.ytService.GetYouTubeService(ctx, row.ChannelID)
		if err == nil {
			_ = ytClient.LiveBroadcasts.Delete(row.BroadcastID).Do()
			if row.YTStreamID != "" {
				_ = ytClient.LiveStreams.Delete(row.YTStreamID).Do()
			}
		}
	}

	if err := h.dbStore.DeleteLivestream(id); err != nil {
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

	row, err := h.dbStore.GetLivestream(streamID)
	if err != nil || row == nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	health := "good"
	status := row.Status

	if row.Platform == "youtube" && row.YTStreamID != "" && h.ytService != nil {
		ctx := c.Request.Context()
		ytClient, err := h.ytService.GetYouTubeService(ctx, row.ChannelID)
		if err == nil {
			call := ytClient.LiveStreams.List([]string{"status"}).Id(row.YTStreamID)
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
		"id":     row.ID,
		"status": status,
		"health": gin.H{
			"status":  health,
			"bitrate": row.VideoBitrate,
			"framerate": 30,
			"resolution": "1080p",
			"packetsLost": 0,
		},
	})
}

func (h *LivestreamHandlers) transitionStream(c *gin.Context, transitionState string) {
	id := c.Param("id")
	row, err := h.dbStore.GetLivestream(id)
	if err != nil || row == nil {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Stream not found"})
		return
	}

	if row.Platform == "youtube" && row.BroadcastID != "" && h.ytService != nil {
		ctx := c.Request.Context()
		ytClient, err := h.ytService.GetYouTubeService(ctx, row.ChannelID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		call := ytClient.LiveBroadcasts.Transition(transitionState, row.BroadcastID, []string{"id", "status"})
		_, err = call.Do()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("YouTube transition to %s failed: %v", transitionState, err)})
			return
		}
	}

	row.Status = transitionState
	if err := h.dbStore.UpsertLivestream(row); err != nil {
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