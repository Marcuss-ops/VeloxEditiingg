package metrics

// supervisor_rollup.go: tryDailyRollup + gcSeenIDs + costAggregate —
// the calendar-driven rollup path of the metrics Supervisor. Split out
// of supervisor.go.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// tryDailyRollup checks if we've crossed midnight since the last
// rollup, and if so, computes the daily rollup for the day that just
// ended. Runs at most once per tick — the lastRollupDay watermark
// ensures idempotency across restarts.
func (s *Supervisor) tryDailyRollup(ctx context.Context, now time.Time) error {
	today := now.UTC().Format("2006-01-02")
	if today == s.lastRollupDay {
		return nil
	}

	// Determine the range of days to roll up: from (lastRollupDay+1) up to
	// (today-1). On first boot (lastRollupDay empty), roll up only yesterday
	// so we don't backfill the entire history on first start.
	//
	// Iterating the full range handles extended downtime: if the supervisor
	// was down for 3 days, all 3 missing days get rolled up on the first
	// tick after recovery.
	var days []string
	if s.lastRollupDay != "" {
		// Normal path: roll up all days from lastRollupDay+1 to today-1.
		start, err := dateAddDay(s.lastRollupDay, 1)
		if err != nil {
			return fmt.Errorf("daily rollup: bad lastRollupDay %q: %w", s.lastRollupDay, err)
		}
		end, err := dateAddDay(today, -1)
		if err != nil {
			return fmt.Errorf("daily rollup: bad today %q: %w", today, err)
		}
		// Walk from start up to end inclusive.
		for d := start; d <= end; {
			days = append(days, d)
			next, err := dateAddDay(d, 1)
			if err != nil {
				return fmt.Errorf("daily rollup: date arithmetic failed at %q: %w", d, err)
			}
			d = next
		}
	} else {
		// First boot: roll up yesterday only.
		yesterday := now.Add(-24 * time.Hour).UTC().Format("2006-01-02")
		days = append(days, yesterday)
	}

	var errs []error
	for _, day := range days {
		log.Printf("[METRICS-SUPERVISOR] daily rollup for %s started", day)
		if err := s.attempts.ComputeDailyRollups(ctx, day); err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup for %s FAILED: %v", day, err)
			errs = append(errs, fmt.Errorf("day %s daily metrics: %w", day, err))
			continue
		}
		if err := s.attempts.ComputeRenderPerformanceDailyRollup(ctx, day); err != nil {
			log.Printf("[METRICS-SUPERVISOR] render performance rollup for %s FAILED: %v", day, err)
			errs = append(errs, fmt.Errorf("day %s render performance: %w", day, err))
			continue
		}
		log.Printf("[METRICS-SUPERVISOR] daily rollups for %s completed", day)
	}
	if len(errs) == 0 {
		s.lastRollupDay = today
		return nil
	}
	log.Printf("[METRICS-SUPERVISOR] daily rollup: %d/%d days failed; watermark remains %q for retry", len(errs), len(days), s.lastRollupDay)
	return errors.Join(errs...)
}

// gcSeenIDs clears the seenIDs map when it exceeds seenCap. The
// intent is time-bounding (the thinker's critique) but a pure
// time-based sweep would risk re-processing attempts whose
// updated_at lands on the boundary. A size cap is the pragmatic
// compromise — see supervisor.go header for the worst-case
// double-count window analysis.
func (s *Supervisor) gcSeenIDs(now time.Time) {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if len(s.seenIDs) > s.seenCap {
		s.seenIDs = make(map[string]time.Time, len(s.seenIDs)/2)
	}
}

// costAggregate is the per-class rolling accumulator held in
// supervisor.tickOnce for a single tick.
type costAggregate struct {
	cpuSecs, networkGB, storageGB, outputMin float64
}
