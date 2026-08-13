// Package m2mkeys / m2mkeys_effective.go
//
// Per-key effective limits (scope presence + rate/quota resolution
// against operator defaults) and the shared CSV/time parsing helpers
// used by the m2mkeys.go scanner and m2mkeys_audit.go.
package m2mkeys

import (
	"fmt"
	"strings"
	"time"
)

// HasScope reports whether the key entry's scope list contains want.
// Useful at middleware boundaries where scope-check is
// scope-presence-only (no override ordering).
func (k *M2MAPIKey) HasScope(want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, s := range k.Scopes {
		if strings.EqualFold(strings.TrimSpace(s), want) {
			return true
		}
	}
	return false
}

// EffectiveRateLimitRPS resolves the per-key override or falls back
// to the operator-supplied default. The two-sided shape lets the
// middleware call one function in the hot path.
func (k *M2MAPIKey) EffectiveRateLimitRPS(defaultRPS int) int {
	if k == nil {
		return defaultRPS
	}
	if k.RateLimitRPS > 0 {
		return k.RateLimitRPS
	}
	return defaultRPS
}

// EffectiveBurst mirrors EffectiveRateLimitRPS for the burst cap.
func (k *M2MAPIKey) EffectiveBurst(defaultBurst int) int {
	if k == nil {
		return defaultBurst
	}
	if k.RateLimitBurst > 0 {
		return k.RateLimitBurst
	}
	return defaultBurst
}

// EffectiveMaxScenes mirrors the pattern for the per-request scene
// quota (caller passes cfg.M2M.MaxScenesPerRequest as the default).
func (k *M2MAPIKey) EffectiveMaxScenes(defaultMax int) int {
	if k == nil {
		return defaultMax
	}
	if k.Quotas.MaxScenes > 0 {
		return k.Quotas.MaxScenes
	}
	return defaultMax
}

// EffectiveMaxTotalDurationS mirrors MaxScenes for the per-request
// duration cap (cfg.M2M.MaxTotalDurationSecondsPerRequest).
func (k *M2MAPIKey) EffectiveMaxTotalDurationS(defaultMax float64) float64 {
	if k == nil {
		return defaultMax
	}
	if k.Quotas.MaxTotalDurationS > 0 {
		return k.Quotas.MaxTotalDurationS
	}
	return defaultMax
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseSQLiteTime parses datetime('now') outputs (UTC, no
// timezone). Returns zero time on error so the callers' signature
// stays simple.
func parseSQLiteTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// SQLite without timezone: try several common layouts.
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized sqlite timestamp: %q", s)
}
