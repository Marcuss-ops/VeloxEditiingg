package calendar

import (
	"context"
	"strings"

	"velox-server/internal/store"
)

func mergeCalendarEvent(existing, incoming *store.CalendarEvent) *store.CalendarEvent {
	if existing == nil {
		if incoming == nil {
			return nil
		}
		clone := *incoming
		return &clone
	}
	merged := *existing
	if incoming == nil {
		return &merged
	}
	if strings.TrimSpace(incoming.ExternalID) != "" {
		merged.ExternalID = incoming.ExternalID
	}
	if strings.TrimSpace(incoming.Source) != "" {
		merged.Source = incoming.Source
	}
	if strings.TrimSpace(incoming.Title) != "" {
		merged.Title = incoming.Title
	}
	if incoming.Date != 0 {
		merged.Date = incoming.Date
	}
	if incoming.Month != 0 {
		merged.Month = incoming.Month
	}
	if incoming.Year != 0 {
		merged.Year = incoming.Year
	}
	if strings.TrimSpace(incoming.Status) != "" {
		merged.Status = incoming.Status
	}
	if strings.TrimSpace(incoming.YouTubeGroup) != "" {
		merged.YouTubeGroup = incoming.YouTubeGroup
	}
	if incoming.StockFootage != nil {
		merged.StockFootage = incoming.StockFootage
	}
	if incoming.InitialClips != nil {
		merged.InitialClips = incoming.InitialClips
	}
	if incoming.IntermediateClips != nil {
		merged.IntermediateClips = incoming.IntermediateClips
	}
	if incoming.FinalClips != nil {
		merged.FinalClips = incoming.FinalClips
	}
	if incoming.VoiceoverPaths != nil {
		merged.VoiceoverPaths = incoming.VoiceoverPaths
	}
	if incoming.Titles != nil {
		merged.Titles = incoming.Titles
	}
	if strings.TrimSpace(incoming.ScriptText) != "" {
		merged.ScriptText = incoming.ScriptText
	}
	if incoming.YouTubeLinks != nil {
		merged.YouTubeLinks = incoming.YouTubeLinks
	}
	if strings.TrimSpace(incoming.Category) != "" {
		merged.Category = incoming.Category
	}
	if strings.TrimSpace(incoming.JobID) != "" {
		merged.JobID = incoming.JobID
	}
	if strings.TrimSpace(incoming.JobStatus) != "" {
		merged.JobStatus = incoming.JobStatus
	}
	if strings.TrimSpace(incoming.QueuedAt) != "" {
		merged.QueuedAt = incoming.QueuedAt
	}
	if strings.TrimSpace(incoming.QueueError) != "" {
		merged.QueueError = incoming.QueueError
	}
	if strings.TrimSpace(incoming.PublishStatus) != "" {
		merged.PublishStatus = incoming.PublishStatus
	}
	if strings.TrimSpace(incoming.OutputVideoPath) != "" {
		merged.OutputVideoPath = incoming.OutputVideoPath
	}
	if strings.TrimSpace(incoming.OutputVideoURL) != "" {
		merged.OutputVideoURL = incoming.OutputVideoURL
	}
	return &merged
}

func (api *CalendarAPI) findExistingEvent(ctx context.Context, event *store.CalendarEvent) (*store.CalendarEvent, error) {
	if event == nil {
		return nil, nil
	}
	if strings.TrimSpace(event.ID) != "" {
		existing, err := api.store.GetCalendarEvent(ctx, event.ID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
	}
	if strings.TrimSpace(event.ExternalID) != "" {
		existing, err := api.store.GetCalendarEventByExternalID(ctx, event.ExternalID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
	}
	return nil, nil
}
