package store

// ProgressSnapshot represents a job progress record from the job_progress table.
type ProgressSnapshot struct {
	JobID         string  `json:"job_id"`
	AttemptNumber int     `json:"attempt_number"`
	Percent       float64 `json:"percent"`
	Stage         string  `json:"stage"`
	CurrentItem   int     `json:"current_item"`
	TotalItems    int     `json:"total_items"`
	Message       string  `json:"message"`
}
