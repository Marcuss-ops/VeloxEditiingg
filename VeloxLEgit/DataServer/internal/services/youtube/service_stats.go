package youtube

import (
	"context"
	"fmt"
	"time"
)

// ManagerStatsResponse is the stats model.
type ManagerStatsResponse struct {
	OK                   bool                         `json:"ok"`
	Cached               bool                         `json:"cached"`
	GeneratedAt          time.Time                    `json:"generated_at"`
	ExpiresAt            time.Time                    `json:"expires_at"`
	CacheAgeSeconds      int                          `json:"cache_age_seconds"`
	TotalGroups          int                          `json:"total_groups"`
	TotalChannels        int                          `json:"total_channels"`
	ValidChannels        int                          `json:"valid_channels"`
	InvalidChannels      int                          `json:"invalid_channels"`
	QuotaSkippedChannels int                          `json:"quota_skipped_channels"`
	ServiceConfigured    bool                         `json:"service_configured"`
	Groups               map[string]ManagerGroupStats `json:"groups"`
	Error                string                       `json:"error,omitempty"`
}

type ManagerGroupStats struct {
	GroupName    string                `json:"group_name"`
	ChannelCount int                   `json:"channel_count"`
	ValidCount   int                   `json:"valid_count"`
	InvalidCount int                   `json:"invalid_count"`
	Channels     []ManagerChannelStats `json:"channels"`
}

type ManagerChannelStats struct {
	ChannelID         string `json:"channel_id"`
	Title             string `json:"title,omitempty"`
	HasTokenFile      bool   `json:"has_token_file"`
	Valid             bool   `json:"valid"`
	IsExpired         bool   `json:"is_expired"`
	HasRefreshToken   bool   `json:"has_refresh_token"`
	RefreshedThisCall bool   `json:"refreshed_this_call"`
	ErrorMessage      string `json:"error,omitempty"`
}

type statsCacheEntry struct {
	data        ManagerStatsResponse
	generatedAt time.Time
	expiresAt   time.Time
}

// GetCachedStats returns stats from cache if fresh, otherwise recomputes them.
func (s *Service) GetCachedStats(forceRefresh bool, ctx context.Context) (ManagerStatsResponse, bool, int, string, error) {
	if !forceRefresh {
		s.statsCacheMu.RLock()
		cached := s.statsCacheEntry
		s.statsCacheMu.RUnlock()

		if cached != nil && time.Now().Before(cached.expiresAt) {
			resp := cached.data
			age := int(time.Since(cached.generatedAt).Seconds())
			return resp, true, age, "HIT", nil
		}
	}

	resp, err := s.aggregateManagerStats(ctx)
	if err != nil {
		return ManagerStatsResponse{OK: false, Groups: map[string]ManagerGroupStats{}}, false, 0, "", err
	}

	resp.Cached = false
	resp.CacheAgeSeconds = 0
	resp.GeneratedAt = time.Now().UTC()
	resp.ExpiresAt = resp.GeneratedAt.Add(managerStatsCacheTTL)

	s.statsCacheMu.Lock()
	s.statsCacheEntry = &statsCacheEntry{
		data:        resp,
		generatedAt: resp.GeneratedAt,
		expiresAt:   resp.ExpiresAt,
	}
	s.statsCacheMu.Unlock()

	return resp, false, 0, "MISS", nil
}

// aggregateManagerStats walks every group and per-channel stat, hydrating
// from the canonical *youtube.Service. The migration to a single
// Repository dropped the legacy Storage; LoadData() returns the
// legacy `*StorageData` shape so this loop's iteration pattern over
// `group.Channels []Channel` continues to type-check.
func (s *Service) aggregateManagerStats(ctx context.Context) (ManagerStatsResponse, error) {
	if s.ytService == nil {
		return ManagerStatsResponse{Groups: map[string]ManagerGroupStats{}}, fmt.Errorf("youtube integration service not configured")
	}

	data := s.ytService.LoadData()
	groups := data.Groups

	resp := ManagerStatsResponse{
		OK:                true,
		Groups:            make(map[string]ManagerGroupStats, len(groups)),
		ServiceConfigured: s.ytService != nil,
	}

	for _, group := range groups {
		gs := ManagerGroupStats{
			GroupName: group.Name,
			Channels:  make([]ManagerChannelStats, 0, len(group.Channels)),
		}
		for _, ch := range group.Channels {
			stat := ManagerChannelStats{
				ChannelID:    ch.ID,
				Title:        ch.Title,
				HasTokenFile: s.ChannelHasTokenFile(ch.ID),
				Valid:        false,
			}

			if s.ytService != nil {
				result, _ := s.ytService.ValidateOAuthAccessToken(ctx, ch.ID)
				stat.Valid = asBool(result, "valid")
				stat.IsExpired = asBool(result, "is_expired")
				stat.HasRefreshToken = asBool(result, "has_refresh_token")
				stat.RefreshedThisCall = asBool(result, "refreshed")
				stat.ErrorMessage = asString(result, "error")
				if title, ok := result["channel_title"].(string); ok && title != "" && stat.Title == "" {
					stat.Title = title
				}
			} else {
				stat.ErrorMessage = "service not configured (degraded mode)"
				resp.QuotaSkippedChannels++
			}

			gs.Channels = append(gs.Channels, stat)
			gs.ChannelCount++
			if stat.Valid {
				gs.ValidCount++
			} else {
				gs.InvalidCount++
			}
		}
		resp.Groups[group.Name] = gs
		resp.TotalChannels += gs.ChannelCount
		resp.ValidChannels += gs.ValidCount
		resp.InvalidChannels += gs.InvalidCount
	}
	resp.TotalGroups = len(groups)

	return resp, nil
}
