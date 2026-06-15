package store

import (
	"fmt"
	"time"
)

// CreateLivestreamTable creates the livestreams table if it doesn't exist.
func (s *SQLiteStore) CreateLivestreamTable() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS livestreams (
			id               TEXT PRIMARY KEY,
			name             TEXT NOT NULL DEFAULT '',
			platform         TEXT NOT NULL DEFAULT '',
			stream_key       TEXT NOT NULL DEFAULT '',
			stream_url       TEXT NOT NULL DEFAULT '',
			description      TEXT NOT NULL DEFAULT '',
			is_for_kids      INTEGER NOT NULL DEFAULT 0,
			video_bitrate    INTEGER NOT NULL DEFAULT 0,
			audio_bitrate    INTEGER NOT NULL DEFAULT 0,
			status           TEXT NOT NULL DEFAULT 'created',
			video_order      TEXT NOT NULL DEFAULT '',
			protocol         TEXT NOT NULL DEFAULT '',
			auto_start       INTEGER NOT NULL DEFAULT 0,
			auto_stop        INTEGER NOT NULL DEFAULT 0,
			scheduled_start  TEXT NOT NULL DEFAULT '',
			scheduled_end    TEXT NOT NULL DEFAULT '',
			created_at       TEXT NOT NULL DEFAULT '',
			duration         INTEGER NOT NULL DEFAULT 0,
			max_viewers      INTEGER NOT NULL DEFAULT 0,
			latency_pref     TEXT NOT NULL DEFAULT 'normal',
			channel_id       TEXT NOT NULL DEFAULT '',
			broadcast_id     TEXT NOT NULL DEFAULT '',
			yt_stream_id     TEXT NOT NULL DEFAULT ''
		)
	`)
	return err
}

// LivestreamRow is the SQLite-compatible representation of a livestream.
type LivestreamRow struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Platform     string `json:"platform"`
	StreamKey    string `json:"stream_key"`
	StreamURL    string `json:"stream_url"`
	Description  string `json:"description"`
	IsForKids    bool   `json:"is_for_kids"`
	VideoBitrate int    `json:"video_bitrate"`
	AudioBitrate int    `json:"audio_bitrate"`
	Status       string `json:"status"`
	VideoOrder   string `json:"video_order"`
	Protocol     string `json:"protocol"`
	AutoStart    bool   `json:"auto_start"`
	AutoStop     bool   `json:"auto_stop"`
	SchedStart   string `json:"scheduled_start"`
	SchedEnd     string `json:"scheduled_end"`
	CreatedAt    string `json:"created_at"`
	Duration     int    `json:"duration"`
	MaxViewers   int    `json:"max_viewers"`
	LatencyPref  string `json:"latency_pref"`
	ChannelID    string `json:"channel_id"`
	BroadcastID  string `json:"broadcast_id"`
	YTStreamID   string `json:"yt_stream_id"`
}

// UpsertLivestream inserts or replaces a livestream record.
func (s *SQLiteStore) UpsertLivestream(row *LivestreamRow) error {
	if s == nil || s.db == nil {
		return nil
	}
	_ = s.CreateLivestreamTable()
	isForKids := 0
	if row.IsForKids {
		isForKids = 1
	}
	autoStart := 0
	if row.AutoStart {
		autoStart = 1
	}
	autoStop := 0
	if row.AutoStop {
		autoStop = 1
	}
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO livestreams
			(id, name, platform, stream_key, stream_url, description, is_for_kids,
			 video_bitrate, audio_bitrate, status, video_order, protocol,
			 auto_start, auto_stop, scheduled_start, scheduled_end, created_at,
			 duration, max_viewers, latency_pref, channel_id, broadcast_id, yt_stream_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		row.ID, row.Name, row.Platform, row.StreamKey, row.StreamURL,
		row.Description, isForKids, row.VideoBitrate, row.AudioBitrate,
		row.Status, row.VideoOrder, row.Protocol, autoStart, autoStop,
		row.SchedStart, row.SchedEnd, row.CreatedAt, row.Duration,
		row.MaxViewers, row.LatencyPref, row.ChannelID, row.BroadcastID, row.YTStreamID,
	)
	return err
}

// ListLivestreams returns all livestream records, most recent first.
func (s *SQLiteStore) ListLivestreams() ([]*LivestreamRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	_ = s.CreateLivestreamTable()
	rows, err := s.db.Query(`
		SELECT id, name, platform, stream_key, stream_url, description, is_for_kids,
		       video_bitrate, audio_bitrate, status, video_order, protocol,
		       auto_start, auto_stop, scheduled_start, scheduled_end, created_at,
		       duration, max_viewers, latency_pref, channel_id, broadcast_id, yt_stream_id
		FROM livestreams
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list livestreams: %w", err)
	}
	defer rows.Close()

	var result []*LivestreamRow
	for rows.Next() {
		var r LivestreamRow
		var isForKids, autoStart, autoStop int
		if err := rows.Scan(
			&r.ID, &r.Name, &r.Platform, &r.StreamKey, &r.StreamURL,
			&r.Description, &isForKids, &r.VideoBitrate, &r.AudioBitrate,
			&r.Status, &r.VideoOrder, &r.Protocol, &autoStart, &autoStop,
			&r.SchedStart, &r.SchedEnd, &r.CreatedAt, &r.Duration,
			&r.MaxViewers, &r.LatencyPref, &r.ChannelID, &r.BroadcastID, &r.YTStreamID,
		); err != nil {
			return nil, fmt.Errorf("scan livestream: %w", err)
		}
		r.IsForKids = isForKids == 1
		r.AutoStart = autoStart == 1
		r.AutoStop = autoStop == 1
		result = append(result, &r)
	}
	return result, rows.Err()
}

// GetLivestream returns a single livestream by ID.
func (s *SQLiteStore) GetLivestream(id string) (*LivestreamRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	_ = s.CreateLivestreamTable()
	var r LivestreamRow
	var isForKids, autoStart, autoStop int
	err := s.db.QueryRow(`
		SELECT id, name, platform, stream_key, stream_url, description, is_for_kids,
		       video_bitrate, audio_bitrate, status, video_order, protocol,
		       auto_start, auto_stop, scheduled_start, scheduled_end, created_at,
		       duration, max_viewers, latency_pref, channel_id, broadcast_id, yt_stream_id
		FROM livestreams WHERE id = ?
	`, id).Scan(
		&r.ID, &r.Name, &r.Platform, &r.StreamKey, &r.StreamURL,
		&r.Description, &isForKids, &r.VideoBitrate, &r.AudioBitrate,
		&r.Status, &r.VideoOrder, &r.Protocol, &autoStart, &autoStop,
		&r.SchedStart, &r.SchedEnd, &r.CreatedAt, &r.Duration,
		&r.MaxViewers, &r.LatencyPref, &r.ChannelID, &r.BroadcastID, &r.YTStreamID,
	)
	if err != nil {
		return nil, err
	}
	r.IsForKids = isForKids == 1
	r.AutoStart = autoStart == 1
	r.AutoStop = autoStop == 1
	return &r, nil
}

// DeleteLivestream removes a livestream by ID.
func (s *SQLiteStore) DeleteLivestream(id string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM livestreams WHERE id = ?`, id)
	return err
}

// ToLivestreamConfigs converts SQLite rows to the livestream handler's config type.
func ToLivestreamConfigs(rows []*LivestreamRow) []map[string]interface{} {
	configs := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		createdAt, _ := time.Parse(time.RFC3339, r.CreatedAt)
		configs[i] = map[string]interface{}{
			"id":                   r.ID,
			"name":                 r.Name,
			"platform":             r.Platform,
			"stream_key":           r.StreamKey,
			"stream_url":           r.StreamURL,
			"description":          r.Description,
			"is_for_kids":          r.IsForKids,
			"video_bitrate":        r.VideoBitrate,
			"audio_bitrate":        r.AudioBitrate,
			"status":               r.Status,
			"video_order":          r.VideoOrder,
			"protocol":             r.Protocol,
			"auto_start":           r.AutoStart,
			"auto_stop":            r.AutoStop,
			"scheduled_start_time": r.SchedStart,
			"scheduled_end_time":   r.SchedEnd,
			"created_at":           createdAt,
			"duration":             r.Duration,
			"max_viewers":          r.MaxViewers,
			"latency_preference":   r.LatencyPref,
			"channel_id":           r.ChannelID,
			"broadcast_id":         r.BroadcastID,
			"youtube_stream_id":    r.YTStreamID,
		}
	}
	return configs
}

// ConfigToRow converts a livestream config map to a SQLite row.
func ConfigToRow(cfg map[string]interface{}) *LivestreamRow {
	r := &LivestreamRow{}
	if v, ok := cfg["id"].(string); ok {
		r.ID = v
	}
	if v, ok := cfg["name"].(string); ok {
		r.Name = v
	}
	if v, ok := cfg["platform"].(string); ok {
		r.Platform = v
	}
	if v, ok := cfg["stream_key"].(string); ok {
		r.StreamKey = v
	}
	if v, ok := cfg["stream_url"].(string); ok {
		r.StreamURL = v
	}
	if v, ok := cfg["description"].(string); ok {
		r.Description = v
	}
	if v, ok := cfg["is_for_kids"].(bool); ok {
		r.IsForKids = v
	}
	if v, ok := cfg["video_bitrate"].(float64); ok {
		r.VideoBitrate = int(v)
	}
	if v, ok := cfg["audio_bitrate"].(float64); ok {
		r.AudioBitrate = int(v)
	}
	if v, ok := cfg["status"].(string); ok {
		r.Status = v
	}
	if v, ok := cfg["video_order"].(string); ok {
		r.VideoOrder = v
	}
	if v, ok := cfg["protocol"].(string); ok {
		r.Protocol = v
	}
	if v, ok := cfg["auto_start"].(bool); ok {
		r.AutoStart = v
	}
	if v, ok := cfg["auto_stop"].(bool); ok {
		r.AutoStop = v
	}
	if v, ok := cfg["scheduled_start_time"].(string); ok {
		r.SchedStart = v
	}
	if v, ok := cfg["scheduled_end_time"].(string); ok {
		r.SchedEnd = v
	}
	if v, ok := cfg["created_at"].(string); ok {
		r.CreatedAt = v
	}
	if v, ok := cfg["duration"].(float64); ok {
		r.Duration = int(v)
	}
	if v, ok := cfg["max_viewers"].(float64); ok {
		r.MaxViewers = int(v)
	}
	if v, ok := cfg["latency_preference"].(string); ok {
		r.LatencyPref = v
	}
	if v, ok := cfg["channel_id"].(string); ok {
		r.ChannelID = v
	}
	if v, ok := cfg["broadcast_id"].(string); ok {
		r.BroadcastID = v
	}
	if v, ok := cfg["youtube_stream_id"].(string); ok {
		r.YTStreamID = v
	}
	return r
}
