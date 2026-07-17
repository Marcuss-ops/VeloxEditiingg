package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CalendarEvent represents a video production calendar event
type CalendarEvent struct {
	ID                string      `json:"id"`
	ExternalID        string      `json:"externalId,omitempty"`
	Source            string      `json:"source,omitempty"`
	Title             string      `json:"title"`
	Date              int         `json:"date"`
	Month             int         `json:"month"`
	Year              int         `json:"year"`
	Status            string      `json:"status,omitempty"`
	StockFootage      []VideoClip `json:"stockFootage"`
	InitialClips      []VideoClip `json:"initialClips"`
	IntermediateClips []VideoClip `json:"intermediateClips"`
	FinalClips        []VideoClip `json:"finalClips"`
	VoiceoverPaths    []string    `json:"voiceoverPaths,omitempty"`
	Titles            []string    `json:"titles,omitempty"`
	ScriptText        string      `json:"scriptText,omitempty"`
	Category          string      `json:"category,omitempty"`
	JobID             string      `json:"jobId,omitempty"`
	JobStatus         string      `json:"jobStatus,omitempty"`
	QueuedAt          string      `json:"queuedAt,omitempty"`
	QueueError        string      `json:"queueError,omitempty"`
	OutputVideoPath   string      `json:"outputVideoPath,omitempty"`
	OutputVideoURL    string      `json:"outputVideoUrl,omitempty"`
	PublishStatus     string      `json:"publishStatus,omitempty"`
	CreatedAt         time.Time   `json:"createdAt"`
	UpdatedAt         time.Time   `json:"updatedAt"`
}

// VideoClip represents a video clip attached to a calendar event
type VideoClip struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	DriveID   string `json:"driveId"`
	Path      string `json:"path,omitempty"`
	URL       string `json:"url,omitempty"`
	WebView   string `json:"webViewLink,omitempty"`
	Thumbnail string `json:"thumbnail,omitempty"`
	Duration  int    `json:"duration,omitempty"`
	Type      string `json:"type"`
}

// CalendarEventFilter represents filter options for calendar events
type CalendarEventFilter struct {
	Search   string `json:"search"`
	Month    *int   `json:"month"`
	Year     *int   `json:"year"`
	HasClips bool   `json:"hasClips"`
	Limit    int    `json:"limit"`
	Offset   int    `json:"offset"`
}

// initCalendarSchema has been migrated to migrations/001_initial.sql.
// Post-migration column adjustments are handled in postMigrationAdjustments().
// The embedded migrations/sqlite/002_calendar.sql can be removed once confirmed.

// CreateCalendarEvent creates a new calendar event
func (s *SQLiteStore) CreateCalendarEvent(ctx context.Context, event *CalendarEvent) error {
	if event.ID == "" {
		event.ID = fmt.Sprintf("evt_%d", time.Now().UnixNano())
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}
	event.UpdatedAt = time.Now()

	stockFootage, _ := json.Marshal(event.StockFootage)
	initialClips, _ := json.Marshal(event.InitialClips)
	intermediateClips, _ := json.Marshal(event.IntermediateClips)
	finalClips, _ := json.Marshal(event.FinalClips)
	voiceoverPaths, _ := json.Marshal(event.VoiceoverPaths)
	titles, _ := json.Marshal(event.Titles)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO calendar_events (
			id, external_id, source, title, date, month, year, status,
			stock_footage, initial_clips, intermediate_clips, final_clips,
			voiceover_paths_json, titles_json, script_text,
			category, job_id, job_status, queued_at, queue_error,
			created_at, updated_at
		)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, event.ExternalID, event.Source, event.Title, event.Date, event.Month, event.Year, event.Status,
		string(stockFootage), string(initialClips), string(intermediateClips), string(finalClips),
		string(voiceoverPaths), string(titles), event.ScriptText,
		event.Category, event.JobID, event.JobStatus, event.QueuedAt, event.QueueError,
		event.CreatedAt.Format(time.RFC3339), event.UpdatedAt.Format(time.RFC3339),
	)
	return err
}

// GetCalendarEvent retrieves a calendar event by ID
func (s *SQLiteStore) GetCalendarEvent(ctx context.Context, id string) (*CalendarEvent, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, external_id, source, title, date, month, year, status, stock_footage, initial_clips, intermediate_clips, final_clips,
		        voiceover_paths_json, titles_json, script_text, category, job_id, job_status, queued_at, queue_error,
		        created_at, updated_at
		 FROM calendar_events WHERE id = ?`, id)

	event := &CalendarEvent{}
	var stockFootage, initialClips, intermediateClips, finalClips []byte
	var voiceoverPaths, titles []byte
	var createdAt, updatedAt sql.NullString
	var queuedAt sql.NullString

	err := row.Scan(&event.ID, &event.ExternalID, &event.Source, &event.Title, &event.Date, &event.Month, &event.Year,
		&event.Status, &stockFootage, &initialClips, &intermediateClips, &finalClips,
		&voiceoverPaths, &titles, &event.ScriptText, &event.Category, &event.JobID, &event.JobStatus, &queuedAt, &event.QueueError,
		&createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if len(stockFootage) > 0 {
		json.Unmarshal(stockFootage, &event.StockFootage)
	}
	if len(initialClips) > 0 {
		json.Unmarshal(initialClips, &event.InitialClips)
	}
	if len(intermediateClips) > 0 {
		json.Unmarshal(intermediateClips, &event.IntermediateClips)
	}
	if len(finalClips) > 0 {
		json.Unmarshal(finalClips, &event.FinalClips)
	}
	if len(voiceoverPaths) > 0 {
		json.Unmarshal(voiceoverPaths, &event.VoiceoverPaths)
	}
	if len(titles) > 0 {
		json.Unmarshal(titles, &event.Titles)
	}
	if createdAt.Valid {
		event.CreatedAt, _ = time.Parse(time.RFC3339, createdAt.String)
	}
	if updatedAt.Valid {
		event.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt.String)
	}
	if queuedAt.Valid {
		event.QueuedAt = queuedAt.String
	}

	return event, nil
}

// GetCalendarEventByExternalID retrieves a calendar event by external ID
func (s *SQLiteStore) GetCalendarEventByExternalID(ctx context.Context, externalID string) (*CalendarEvent, error) {
	if strings.TrimSpace(externalID) == "" {
		return nil, nil
	}

	row := s.db.QueryRowContext(ctx,
		`SELECT id, external_id, source, title, date, month, year, status, stock_footage, initial_clips, intermediate_clips, final_clips,
		        voiceover_paths_json, titles_json, script_text, category, job_id, job_status, queued_at, queue_error,
		        created_at, updated_at
		 FROM calendar_events WHERE external_id = ?`, strings.TrimSpace(externalID))

	event := &CalendarEvent{}
	var stockFootage, initialClips, intermediateClips, finalClips []byte
	var voiceoverPaths, titles []byte
	var createdAt, updatedAt sql.NullString
	var queuedAt sql.NullString

	err := row.Scan(&event.ID, &event.ExternalID, &event.Source, &event.Title, &event.Date, &event.Month, &event.Year,
		&event.Status, &stockFootage, &initialClips, &intermediateClips, &finalClips,
		&voiceoverPaths, &titles, &event.ScriptText, &event.Category, &event.JobID, &event.JobStatus, &queuedAt, &event.QueueError,
		&createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if len(stockFootage) > 0 {
		json.Unmarshal(stockFootage, &event.StockFootage)
	}
	if len(initialClips) > 0 {
		json.Unmarshal(initialClips, &event.InitialClips)
	}
	if len(intermediateClips) > 0 {
		json.Unmarshal(intermediateClips, &event.IntermediateClips)
	}
	if len(finalClips) > 0 {
		json.Unmarshal(finalClips, &event.FinalClips)
	}
	if len(voiceoverPaths) > 0 {
		json.Unmarshal(voiceoverPaths, &event.VoiceoverPaths)
	}
	if len(titles) > 0 {
		json.Unmarshal(titles, &event.Titles)
	}
	if createdAt.Valid {
		event.CreatedAt, _ = time.Parse(time.RFC3339, createdAt.String)
	}
	if updatedAt.Valid {
		event.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt.String)
	}
	if queuedAt.Valid {
		event.QueuedAt = queuedAt.String
	}

	return event, nil
}

// UpdateCalendarEvent updates an existing calendar event
func (s *SQLiteStore) UpdateCalendarEvent(ctx context.Context, event *CalendarEvent) error {
	event.UpdatedAt = time.Now()

	stockFootage, _ := json.Marshal(event.StockFootage)
	initialClips, _ := json.Marshal(event.InitialClips)
	intermediateClips, _ := json.Marshal(event.IntermediateClips)
	finalClips, _ := json.Marshal(event.FinalClips)
	voiceoverPaths, _ := json.Marshal(event.VoiceoverPaths)
	titles, _ := json.Marshal(event.Titles)

	result, err := s.db.ExecContext(ctx,
		`UPDATE calendar_events SET external_id=?, source=?, title=?, date=?, month=?, year=?, status=?, stock_footage=?, initial_clips=?, intermediate_clips=?, final_clips=?,
		 voiceover_paths_json=?, titles_json=?, script_text=?, category=?, job_id=?, job_status=?, queued_at=?, queue_error=?, updated_at=?
		 WHERE id = ?`,
		event.ExternalID, event.Source, event.Title, event.Date, event.Month, event.Year, event.Status,
		string(stockFootage), string(initialClips), string(intermediateClips), string(finalClips),
		string(voiceoverPaths), string(titles), event.ScriptText, event.Category, event.JobID, event.JobStatus, event.QueuedAt, event.QueueError,
		event.UpdatedAt.Format(time.RFC3339), event.ID,
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return fmt.Errorf("calendar event not found")
	}
	return nil
}

// DeleteCalendarEvent deletes a calendar event by ID
func (s *SQLiteStore) DeleteCalendarEvent(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM calendar_events WHERE id = ?`, id)
	return err
}

// ListCalendarEvents lists calendar events with optional filtering
func (s *SQLiteStore) ListCalendarEvents(ctx context.Context, filter CalendarEventFilter) ([]*CalendarEvent, error) {
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	query := `SELECT id, external_id, source, title, date, month, year, status, stock_footage, initial_clips, intermediate_clips, final_clips,
		                 voiceover_paths_json, titles_json, script_text, category, job_id, job_status, queued_at, queue_error, created_at, updated_at
	          FROM calendar_events WHERE 1=1`
	args := []interface{}{}

	if filter.Search != "" {
		query += " AND title LIKE ?"
		args = append(args, "%"+filter.Search+"%")
	}
	if filter.Month != nil {
		query += " AND month = ?"
		args = append(args, *filter.Month)
	}
	if filter.Year != nil {
		query += " AND year = ?"
		args = append(args, *filter.Year)
	}
	if filter.HasClips {
		query += " AND (json_array_length(stock_footage) > 0 OR json_array_length(initial_clips) > 0 OR json_array_length(intermediate_clips) > 0 OR json_array_length(final_clips) > 0)"
	}

	query += " ORDER BY year DESC, month DESC, date DESC LIMIT ? OFFSET ?"
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []*CalendarEvent{}
	for rows.Next() {
		event := &CalendarEvent{}
		var stockFootage, initialClips, intermediateClips, finalClips []byte
		var voiceoverPaths, titles []byte
		var createdAt, updatedAt, queuedAt sql.NullString

		if err := rows.Scan(&event.ID, &event.ExternalID, &event.Source, &event.Title, &event.Date, &event.Month, &event.Year, &event.Status,
			&stockFootage, &initialClips, &intermediateClips, &finalClips,
			&voiceoverPaths, &titles, &event.ScriptText, &event.Category, &event.JobID, &event.JobStatus, &queuedAt, &event.QueueError,
			&createdAt, &updatedAt); err != nil {
			continue
		}

		if len(stockFootage) > 0 {
			json.Unmarshal(stockFootage, &event.StockFootage)
		}
		if len(initialClips) > 0 {
			json.Unmarshal(initialClips, &event.InitialClips)
		}
		if len(intermediateClips) > 0 {
			json.Unmarshal(intermediateClips, &event.IntermediateClips)
		}
		if len(finalClips) > 0 {
			json.Unmarshal(finalClips, &event.FinalClips)
		}
		if len(voiceoverPaths) > 0 {
			json.Unmarshal(voiceoverPaths, &event.VoiceoverPaths)
		}
		if len(titles) > 0 {
			json.Unmarshal(titles, &event.Titles)
		}
		if createdAt.Valid {
			event.CreatedAt, _ = time.Parse(time.RFC3339, createdAt.String)
		}
		if updatedAt.Valid {
			event.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt.String)
		}
		if queuedAt.Valid {
			event.QueuedAt = queuedAt.String
		}
		events = append(events, event)
	}
	return events, nil
}

// GetCalendarEventsByDateRange retrieves events within a date range
func (s *SQLiteStore) GetCalendarEventsByDateRange(ctx context.Context, startMonth, startYear, endMonth, endYear int) ([]*CalendarEvent, error) {
	query := `SELECT id, external_id, source, title, date, month, year, status, stock_footage, initial_clips, intermediate_clips, final_clips,
		                 voiceover_paths_json, titles_json, script_text, category, job_id, job_status, queued_at, queue_error, created_at, updated_at 
			  FROM calendar_events 
			  WHERE (year > ? OR (year = ? AND month >= ?)) 
			    AND (year < ? OR (year = ? AND month <= ?))
			  ORDER BY year, month, date`

	rows, err := s.db.QueryContext(ctx, query, startYear, startYear, startMonth, endYear, endYear, endMonth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []*CalendarEvent{}
	for rows.Next() {
		event := &CalendarEvent{}
		var stockFootage, initialClips, intermediateClips, finalClips []byte
		var voiceoverPaths, titles []byte
		var createdAt, updatedAt, queuedAt sql.NullString

		if err := rows.Scan(&event.ID, &event.ExternalID, &event.Source, &event.Title, &event.Date, &event.Month, &event.Year, &event.Status,
			&stockFootage, &initialClips, &intermediateClips, &finalClips,
			&voiceoverPaths, &titles, &event.ScriptText, &event.Category, &event.JobID, &event.JobStatus, &queuedAt, &event.QueueError,
			&createdAt, &updatedAt); err != nil {
			continue
		}

		if len(stockFootage) > 0 {
			json.Unmarshal(stockFootage, &event.StockFootage)
		}
		if len(initialClips) > 0 {
			json.Unmarshal(initialClips, &event.InitialClips)
		}
		if len(intermediateClips) > 0 {
			json.Unmarshal(intermediateClips, &event.IntermediateClips)
		}
		if len(finalClips) > 0 {
			json.Unmarshal(finalClips, &event.FinalClips)
		}
		if len(voiceoverPaths) > 0 {
			json.Unmarshal(voiceoverPaths, &event.VoiceoverPaths)
		}
		if len(titles) > 0 {
			json.Unmarshal(titles, &event.Titles)
		}
		if createdAt.Valid {
			event.CreatedAt, _ = time.Parse(time.RFC3339, createdAt.String)
		}
		if updatedAt.Valid {
			event.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt.String)
		}
		if queuedAt.Valid {
			event.QueuedAt = queuedAt.String
		}
		events = append(events, event)
	}
	return events, nil
}

func (e *CalendarEvent) HasQueueAssets() bool {
	if e == nil {
		return false
	}
	if len(e.InitialClips)+len(e.IntermediateClips)+len(e.FinalClips)+len(e.StockFootage) == 0 {
		return false
	}
	for _, s := range e.VoiceoverPaths {
		if strings.TrimSpace(s) != "" {
			return true
		}
	}
	return strings.TrimSpace(e.ScriptText) != ""
}
