package ansible

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *AnsibleHandlers) AnsibleComputersSummaryHandler(c *gin.Context) {
	if h.computers != nil {
		total := h.computers.Count()
		enabled := h.computers.CountEnabled()
		c.JSON(http.StatusOK, gin.H{
			"total":     total,
			"available": enabled,
			"busy":      total - enabled,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total":     0,
		"available": 0,
		"busy":      0,
	})
}

func (h *AnsibleHandlers) AnsibleComputersListHandler(c *gin.Context) {
	if h.computers != nil {
		computers := h.computers.ListComputers()
		c.JSON(http.StatusOK, gin.H{
			"computers": computers,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"computers": []interface{}{},
	})
}

func (h *AnsibleHandlers) GetCapabilitiesHandler(c *gin.Context) {
	c.JSON(http.StatusOK, h.capabilitiesPayload())
}

func (h *AnsibleHandlers) GetRunsHandler(c *gin.Context) {
	if h.manager == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	type runPayload struct {
		RunID       string   `json:"run_id"`
		Action      string   `json:"action"`
		ComputerIDs []string `json:"computer_ids"`
		Status      string   `json:"status"`
		StartedAt   string   `json:"started_at"`
		CompletedAt string   `json:"completed_at,omitempty"`
		ReturnCode  int      `json:"return_code,omitempty"`
		Output      string   `json:"output,omitempty"`
		Preamble    string   `json:"preamble,omitempty"`
	}

	runs := h.manager.ListRuns()
	out := make([]runPayload, 0, len(runs))
	for _, run := range runs {
		status := run.Status
		switch status {
		case "ok":
			status = "completed"
		case "running":
			status = "running"
		case "failed":
			status = "failed"
		default:
			if status == "" {
				status = "pending"
			}
		}

		payload := runPayload{
			RunID:       run.ID,
			Action:      run.Action,
			ComputerIDs: run.Hosts,
			Status:      status,
			StartedAt:   time.Unix(run.StartedAt, 0).UTC().Format(time.RFC3339),
			ReturnCode:  run.ReturnCode,
			Output:      run.Output,
			Preamble:    run.Preamble,
		}
		if run.EndedAt > 0 {
			payload.CompletedAt = time.Unix(run.EndedAt, 0).UTC().Format(time.RFC3339)
		}
		out = append(out, payload)
	}

	c.JSON(http.StatusOK, out)
}

func (h *AnsibleHandlers) RunActionHandler(c *gin.Context) {
	var body struct {
		ComputerIDs []string `json:"computer_ids"`
		WorkerIDs   []string `json:"worker_ids"`
		Workers     []string `json:"workers"`
		Hosts       []string `json:"hosts"`
		Action      string   `json:"action"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	targets := body.ComputerIDs
	if len(targets) == 0 {
		targets = body.WorkerIDs
	}
	if len(targets) == 0 {
		targets = body.Workers
	}
	if len(targets) == 0 {
		targets = body.Hosts
	}
	targets = h.resolveComputerIDs(targets)
	if len(targets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "computer_ids required"})
		return
	}
	if body.Action == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "action required"})
		return
	}
	if body.Action != "update_workers" && body.Action != "install_workers" && body.Action != "preflight_workers" && body.Action != "test_ssh" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported action"})
		return
	}

	runID, err := h.runActionForTargets(body.Action, targets)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"run_id":  runID,
		"status":  "queued",
		"action":  body.Action,
		"targets": targets,
	})
}

func (h *AnsibleHandlers) RunShellHandler(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "Not implemented yet",
		"hint":  "Use /ansible/computers/run_action for update/install/preflight",
	})
}

func (h *AnsibleHandlers) TestSSHHandler(c *gin.Context) {
	var body struct {
		ComputerID  string   `json:"computer_id"`
		ComputerIDs []string `json:"computer_ids"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	targets := body.ComputerIDs
	if len(targets) == 0 && body.ComputerID != "" {
		targets = []string{body.ComputerID}
	}
	targets = h.resolveComputerIDs(targets)
	if len(targets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "computer_id required"})
		return
	}

	runID, err := h.runActionForTargets("test_ssh", targets)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"run_id":  runID,
		"status":  "queued",
		"action":  "test_ssh",
		"targets": targets,
	})
}

func (h *AnsibleHandlers) AnsibleComputersSaveHandler(c *gin.Context) {
	if h.computers == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "computer manager unavailable"})
		return
	}

	var body AnsibleComputer
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	host := strings.TrimSpace(body.Host)
	if host == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "host required"})
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if body.CreatedAt == "" {
		body.CreatedAt = now
	}
	body.UpdatedAt = now
	if strings.TrimSpace(body.AnsibleUser) == "" {
		body.AnsibleUser = "pierone"
	}

	if err := h.computers.SaveComputer(body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	hostKey := host
	c.JSON(http.StatusOK, gin.H{"id": hostKey, "computer": body})
}

func (h *AnsibleHandlers) AnsibleComputersDeleteHandler(c *gin.Context) {
	if h.computers == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "computer manager unavailable"})
		return
	}

	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}

	if _, ok := h.computers.GetComputer(id); !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "computer not found", "id": id})
		return
	}

	if err := h.computers.DeleteComputer(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": id})
}

func (h *AnsibleHandlers) AnsibleComputersLogsHandler(c *gin.Context) {
	if h.manager == nil {
		c.JSON(http.StatusOK, []AnsibleRunRecord{})
		return
	}

	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}

	limit := 200
	if v := strings.TrimSpace(c.Query("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}

	all := h.manager.ListRuns()
	type logEntry struct {
		RunID       string   `json:"run_id"`
		Action      string   `json:"action"`
		Playbook    string   `json:"playbook,omitempty"`
		Status      string   `json:"status"`
		StartedAt   string   `json:"started_at"`
		CompletedAt string   `json:"completed_at,omitempty"`
		Output      string   `json:"output,omitempty"`
		Hosts       []string `json:"hosts"`
		ReturnCode  int      `json:"return_code,omitempty"`
	}

	matched := make([]logEntry, 0, len(all))
	for _, run := range all {
		hit := false
		for _, host := range run.Hosts {
			if host == id {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		entry := logEntry{
			RunID:      run.ID,
			Action:     run.Action,
			Playbook:   run.Playbook,
			Status:     run.Status,
			Hosts:      run.Hosts,
			ReturnCode: run.ReturnCode,
			Output:     run.Output,
		}
		if run.StartedAt > 0 {
			entry.StartedAt = time.Unix(run.StartedAt, 0).UTC().Format(time.RFC3339)
		}
		if run.EndedAt > 0 {
			entry.CompletedAt = time.Unix(run.EndedAt, 0).UTC().Format(time.RFC3339)
		}
		matched = append(matched, entry)
	}

	if len(matched) > limit {
		matched = matched[:limit]
	}
	c.JSON(http.StatusOK, matched)
}
