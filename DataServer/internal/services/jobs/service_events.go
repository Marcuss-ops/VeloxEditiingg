package jobs

import (
	"context"
	"sort"
	"strings"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/workers"
)

func (s *Service) GetJobEvents(ctx context.Context, jobID string, limit int) ([]map[string]interface{}, error) {
	events := make([]map[string]interface{}, 0, limit)
	if s.logger != nil {
		loggedEvents, err := s.logger.GetRecentEvents(jobID, limit)
		if err != nil {
			return nil, err
		}
		events = append(events, loggedEvents...)
	}

	if jobID != "" && s.fileQ != nil {
		jobMap, err := s.fileQ.GetJobAsMap(ctx, jobID)
		if err == nil && jobMap != nil {
			events = append(events, buildJobLogEvents(jobID, jobMap["logs"])...)
			events = append(events, BuildWorkerRecentLogEvents(ctx, s.reg, jobMap, jobID)...)
		}
	}

	if len(events) > 1 {
		events = DedupeAndSortEvents(events)
	}
	if len(events) > limit {
		events = events[:limit]
	}
	return events, nil
}

func (s *Service) enrichJobWithProcessingLogs(ctx context.Context, job map[string]interface{}, jobID string) {
	if s == nil || s.reg == nil || len(job) == 0 || strings.TrimSpace(jobID) == "" {
		return
	}
	recent := BuildWorkerRecentLogEvents(ctx, s.reg, job, jobID)
	if len(recent) == 0 {
		return
	}

	existingAny, _ := job["logs"].([]interface{})
	seen := make(map[string]struct{}, len(existingAny)+len(recent))
	merged := make([]interface{}, 0, len(existingAny)+len(recent))

	for _, row := range existingAny {
		if m, ok := row.(map[string]interface{}); ok {
			msg := strings.TrimSpace(asJobString(m["message"]))
			ts := strings.TrimSpace(asJobString(m["timestamp"]))
			if msg != "" {
				seen[ts+"|"+msg] = struct{}{}
			}
		}
		merged = append(merged, row)
	}

	for _, ev := range recent {
		msg := strings.TrimSpace(asJobString(ev["message"]))
		ts := strings.TrimSpace(asJobString(ev["timestamp"]))
		if msg == "" {
			continue
		}
		key := ts + "|" + msg
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, map[string]interface{}{
			"timestamp": ts,
			"message":   msg,
			"worker_id": asJobString(ev["worker_id"]),
			"level":     "info",
		})
	}

	if len(merged) > 0 {
		job["logs"] = merged
	}
}

func BuildWorkerRecentLogEvents(ctx context.Context, reg *workers.Registry, job map[string]interface{}, jobID string) []map[string]interface{} {
	if reg == nil || job == nil {
		return nil
	}
	workerID := strings.TrimSpace(asJobString(job["assigned_to"]))
	if workerID == "" {
		workerID = strings.TrimSpace(asJobString(job["claimed_by"]))
	}
	if workerID == "" {
		return nil
	}
	info := reg.GetWorker(ctx, workerID)
	if info == nil || len(info.RecentLogs) == 0 {
		return nil
	}
	status := strings.ToUpper(strings.TrimSpace(asJobString(job["status"])))
	includeAll := status == "PROCESSING"

	out := make([]map[string]interface{}, 0, len(info.RecentLogs))
	for _, line := range info.RecentLogs {
		raw := strings.TrimSpace(line)
		if raw == "" {
			continue
		}
		if !includeAll && !strings.Contains(raw, jobID) {
			continue
		}
		ts := time.Now().UTC().Format(time.RFC3339)
		msg := raw
		if split := strings.SplitN(raw, " [", 2); len(split) == 2 {
			if parsed, err := time.ParseInLocation("2006/01/02 15:04:05", split[0], time.UTC); err == nil {
				ts = parsed.UTC().Format(time.RFC3339)
				msg = "[" + split[1]
			}
		}
		out = append(out, map[string]interface{}{
			"timestamp":  ts,
			"job_id":     jobID,
			"event":      "worker_log",
			"event_type": "worker_log",
			"message":    msg,
			"worker_id":  workerID,
			"source":     "worker_recent_logs",
		})
	}
	return out
}

func DedupeAndSortEvents(events []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]struct{}, len(events))
	out := make([]map[string]interface{}, 0, len(events))
	for _, event := range events {
		key := strings.TrimSpace(asJobString(event["timestamp"])) + "|" +
			strings.TrimSpace(asJobString(event["event"])) + "|" +
			strings.TrimSpace(asJobString(event["message"]))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, event)
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti := strings.TrimSpace(asJobString(out[i]["timestamp"]))
		tj := strings.TrimSpace(asJobString(out[j]["timestamp"]))
		return ti > tj
	})
	return out
}

func buildJobLogEvents(jobID string, rawLogs interface{}) []map[string]interface{} {
	switch logs := rawLogs.(type) {
	case []queue.JobLogEntry:
		out := make([]map[string]interface{}, 0, len(logs))
		for _, row := range logs {
			msg := strings.TrimSpace(row.Message)
			if msg == "" {
				continue
			}
			ts := strings.TrimSpace(row.Timestamp)
			if ts == "" {
				ts = strings.TrimSpace(row.Time)
			}
			if ts == "" {
				ts = time.Now().UTC().Format(time.RFC3339)
			}
			eventType := "worker_log"
			if strings.TrimSpace(row.Level) != "" {
				eventType = strings.ToLower(strings.TrimSpace(row.Level))
			}
			if row.IsError {
				eventType = "error"
			}
			out = append(out, map[string]interface{}{
				"timestamp":  ts,
				"job_id":     jobID,
				"event":      eventType,
				"event_type": eventType,
				"message":    msg,
				"worker_id":  row.WorkerID,
				"source":     "job_logs",
			})
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(logs))
		for _, row := range logs {
			m, ok := row.(map[string]interface{})
			if !ok {
				continue
			}
			msg := strings.TrimSpace(asJobString(m["message"]))
			if msg == "" {
				continue
			}
			ts := strings.TrimSpace(asJobString(m["timestamp"]))
			if ts == "" {
				ts = strings.TrimSpace(asJobString(m["time"]))
			}
			if ts == "" {
				ts = time.Now().UTC().Format(time.RFC3339)
			}
			workerID := strings.TrimSpace(asJobString(m["worker_id"]))
			eventType := "worker_log"
			if strings.TrimSpace(asJobString(m["level"])) != "" {
				eventType = strings.ToLower(strings.TrimSpace(asJobString(m["level"])))
			}
			if v, ok := m["is_error"].(bool); ok && v {
				eventType = "error"
			}
			out = append(out, map[string]interface{}{
				"timestamp":  ts,
				"job_id":     jobID,
				"event":      eventType,
				"event_type": eventType,
				"message":    msg,
				"worker_id":  workerID,
				"source":     "job_logs",
			})
		}
		return out
	default:
		return nil
	}
}

func ExtractWorkerLogEntries(output map[string]interface{}, workerID string) []queue.JobLogEntry {
	if len(output) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	var lines []string

	candidates := []string{"logs", "progress_logs", "processing_logs", "events", "validation_details"}
	for _, key := range candidates {
		v, ok := output[key]
		if !ok || v == nil {
			continue
		}
		switch vv := v.(type) {
		case []interface{}:
			for _, it := range vv {
				if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
					lines = append(lines, strings.TrimSpace(s))
				}
			}
		case []string:
			for _, s := range vv {
				if strings.TrimSpace(s) != "" {
					lines = append(lines, strings.TrimSpace(s))
				}
			}
		}
	}
	for _, key := range []string{"status_log"} {
		v, ok := output[key]
		if !ok || v == nil {
			continue
		}
		if s, ok := v.(string); ok {
			for _, part := range strings.Split(s, "\n") {
				part = strings.TrimSpace(part)
				if part != "" {
					lines = append(lines, part)
				}
			}
		}
	}

	if len(lines) == 0 {
		return nil
	}

	entries := make([]queue.JobLogEntry, 0, len(lines))
	for _, line := range lines {
		entries = append(entries, queue.JobLogEntry{
			Timestamp: now,
			Time:      now,
			Message:   line,
			WorkerID:  workerID,
		})
	}
	return entries
}

func ExtractOutputVideoPath(output map[string]interface{}) string {
	if len(output) == 0 {
		return ""
	}
	for _, key := range []string{"master_video_path", "output_path", "result_path", "video_path"} {
		if s, ok := output[key].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if nested, ok := output["result"].(map[string]interface{}); ok {
		return ExtractOutputVideoPath(nested)
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func asJobString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
