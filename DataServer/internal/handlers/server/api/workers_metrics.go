package api

// WorkerMetrics is the typed projection of the worker heartbeat metrics
// blob. The registry still stores the raw map (so the heartbeat wire
// format and the SQLite raw_json contract stay unchanged), but the
// operator-facing mapper works against this typed struct instead of
// repeatedly probing a map[string]interface{}.
type WorkerMetrics struct {
	ActiveTasks         int32
	TaskSlots           int32
	CPUUtilizationRatio float64
	MemoryUsedBytes     int64
	DiskFreeBytes       int64
	Load1               float64
	JobsCompleted       int64
	JobsFailed          int64
	ActiveJobs          []ActiveTaskMetrics
}

// ActiveTaskMetrics is the typed counterpart of the per-active-job
// sub-document carried inside the metrics blob.
type ActiveTaskMetrics struct {
	JobID       string `json:"job_id"`
	TaskID      string `json:"task_id,omitempty"`
	AttemptID   string `json:"attempt_id,omitempty"`
	Executor    string `json:"executor,omitempty"`
	Stage       string `json:"stage,omitempty"`
	Percent     int64  `json:"percent"`
	Scene       int64  `json:"scene"`
	TotalScenes int64  `json:"total_scenes"`
	LeaseID     string `json:"lease_id,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`

	// Operational lifecycle phase (prefetching/rendering/publishing/...).
	// Populated when the worker sends operational_phase in its heartbeat.
	OperationalPhase string `json:"operational_phase,omitempty"`

	// Detailed render progress fields from the C++ engine.
	Segment          int64   `json:"segment,omitempty"`
	TotalSegments    int64   `json:"total_segments,omitempty"`
	FramesDecoded    int64   `json:"frames_decoded,omitempty"`
	FramesComposited int64   `json:"frames_composited,omitempty"`
	FramesEncoded    int64   `json:"frames_encoded,omitempty"`
	SpeedX           float64 `json:"speed_x,omitempty"`
	ElapsedMS        int64   `json:"elapsed_ms,omitempty"`
	LastProgressAt   string  `json:"last_progress_at,omitempty"`
}

// ParseWorkerMetrics converts the raw JSON-decoded metrics map into a
// typed WorkerMetrics. Missing keys produce zero values; numeric
// fields tolerate float64, int, int64, int32 and json.Number so we
// remain compatible with whatever json.Unmarshal produced.
func ParseWorkerMetrics(raw map[string]interface{}) WorkerMetrics {
	var m WorkerMetrics
	if raw == nil {
		return m
	}

	if v, ok := toInt64(raw["active_tasks"]); ok {
		m.ActiveTasks = int32(v)
	}
	if v, ok := toInt64(raw["task_slots"]); ok {
		m.TaskSlots = int32(v)
	}
	if v, ok := toFloat64(raw["cpu_utilization_ratio"]); ok {
		m.CPUUtilizationRatio = v
	}
	if v, ok := toInt64(raw["memory_used_bytes"]); ok {
		m.MemoryUsedBytes = v
	}
	if v, ok := toInt64(raw["disk_free_bytes"]); ok {
		m.DiskFreeBytes = v
	}
	if v, ok := toFloat64(raw["load1"]); ok {
		m.Load1 = v
	} else if v, ok := toFloat64(raw["load_1"]); ok {
		m.Load1 = v
	}
	if v, ok := toInt64(raw["jobs_completed"]); ok {
		m.JobsCompleted = v
	}
	if v, ok := toInt64(raw["jobs_failed"]); ok {
		m.JobsFailed = v
	}

	switch jobs := raw["active_jobs"].(type) {
	case []interface{}:
		m.ActiveJobs = parseActiveJobs(jobs)
	case []map[string]interface{}:
		m.ActiveJobs = make([]ActiveTaskMetrics, 0, len(jobs))
		for _, job := range jobs {
			m.ActiveJobs = append(m.ActiveJobs, activeTaskFromMap(job))
		}
	}

	return m
}

// parseActiveJobs converts a JSON-decoded []interface{} of active job
// maps into typed ActiveTaskMetrics entries.
func parseActiveJobs(raw []interface{}) []ActiveTaskMetrics {
	out := make([]ActiveTaskMetrics, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		out = append(out, activeTaskFromMap(m))
	}
	return out
}

func activeTaskFromMap(m map[string]interface{}) ActiveTaskMetrics {
	return ActiveTaskMetrics{
		JobID:            toString(m["job_id"]),
		TaskID:           toString(m["task_id"]),
		AttemptID:        toString(m["attempt_id"]),
		Executor:         toString(m["job_type"]),
		Stage:            toString(m["progress_stage"]),
		Percent:          toInt64Zero(m["progress_percent"]),
		Scene:            toInt64Zero(m["progress_scene"]),
		TotalScenes:      toInt64Zero(m["progress_total"]),
		LeaseID:          toString(m["lease_id"]),
		StartedAt:        toString(m["started_at"]),
		OperationalPhase: toString(m["operational_phase"]),
		Segment:          toInt64Zero(m["progress_segment"]),
		TotalSegments:    toInt64Zero(m["progress_total_segments"]),
		FramesDecoded:    toInt64Zero(m["frames_decoded"]),
		FramesComposited: toInt64Zero(m["frames_composited"]),
		FramesEncoded:    toInt64Zero(m["frames_encoded"]),
		SpeedX:           toFloat64Zero(m["ffmpeg_speed_x"]),
		ElapsedMS:        toInt64Zero(m["elapsed_ms"]),
		LastProgressAt:   toString(m["last_progress_at"]),
	}
}
