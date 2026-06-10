package ansible

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// MasterResolution holds the result of resolving a master URL.
type MasterResolution struct {
	URL    string
	Source string
}

// ResolveMasterURL resolves the master URL from configuration and request context.
//
// Resolution order (first non-empty wins):
//  1. cfg.MasterURL        — env-derived (MASTER_PUBLIC_URL > VELOX_MASTER_URL > MASTER_URL)
//  2. X-Forwarded-* headers — used when behind a reverse proxy
//  3. request Host header   — used as last resort for same-origin callers
//  4. fallback              — used when no signal is available
//
// The optional gin.Context is used to read the original Host/Proto when the
// request is forwarded. If cfg is nil, the cfg-derived source is skipped.
func ResolveMasterURL(cfg interface{}, c interface{}, fallback string) MasterResolution {
	if cfg != nil {
		if v, ok := extractMasterURLFromConfig(cfg); ok && v != "" {
			return MasterResolution{URL: v, Source: "config"}
		}
	}
	if c != nil {
		if v, ok := extractMasterURLFromContext(c); ok && v != "" {
			return MasterResolution{URL: v, Source: "request"}
		}
	}
	return MasterResolution{URL: fallback, Source: "fallback"}
}

// extractMasterURLFromConfig reads the MasterURL field via reflection so callers
// can pass either *config.Config or a wrapper.
func extractMasterURLFromConfig(cfg interface{}) (string, bool) {
	v := reflect.ValueOf(cfg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "", false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", false
	}
	f := v.FieldByName("MasterURL")
	if !f.IsValid() || f.Kind() != reflect.String {
		return "", false
	}
	return strings.TrimSpace(f.String()), true
}

// extractMasterURLFromContext inspects X-Forwarded-* / Host headers when the
// caller provided a *gin.Context. Returns false when the context type is
// unknown or no usable header is present.
func extractMasterURLFromContext(c interface{}) (string, bool) {
	gc, ok := c.(*gin.Context)
	if !ok || gc == nil || gc.Request == nil {
		return "", false
	}
	r := gc.Request
	host := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(r.Host)
	}
	if host == "" {
		return "", false
	}
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else {
			proto = "http"
		}
	}
	return fmt.Sprintf("%s://%s", proto, host), true
}

// IsLocalhostURL checks if a URL points to localhost.
func IsLocalhostURL(url string) bool {
	return strings.Contains(url, "localhost") || strings.Contains(url, "127.0.0.1") || strings.Contains(url, "0.0.0.0")
}

// AnsibleHandlers provides HTTP handlers for Ansible operations.
type AnsibleHandlers struct {
	manager   *AnsibleRunManager
	computers *AnsibleComputerManager
	dataDir   string
	masterURL string
}

// NewAnsibleHandlers creates a new Ansible handlers instance.
func NewAnsibleHandlers(manager *AnsibleRunManager) *AnsibleHandlers {
	return &AnsibleHandlers{manager: manager}
}

// SetComputerManager sets the computer manager for loading real data.
func (h *AnsibleHandlers) SetComputerManager(computers *AnsibleComputerManager, dataDir string) {
	h.computers = computers
	h.dataDir = dataDir
}

// SetMasterURL sets the master URL used when invoking playbooks.
func (h *AnsibleHandlers) SetMasterURL(masterURL string) {
	h.masterURL = strings.TrimSpace(masterURL)
}

func (h *AnsibleHandlers) isReady() bool {
	return h != nil && h.manager != nil && h.manager.Ready()
}

func (h *AnsibleHandlers) capabilitiesPayload() gin.H {
	playbooksDir := ""
	if h.manager != nil {
		playbooksDir = h.manager.PlaybookDir()
	}

	actions := []gin.H{
		{
			"name":      "preflight_workers",
			"playbook":  "preflight_workers.yml",
			"available": h.playbookExists("preflight_workers.yml"),
			"reason":    "SSH ping, disk, Python/ffmpeg, worker status",
		},
		{
			"name":      "update_workers",
			"playbook":  "update_workers.yml",
			"available": h.playbookExists("update_workers.yml"),
			"reason":    "Aggiorna codice sui computer selezionati",
		},
		{
			"name":      "install_workers",
			"playbook":  "install_workers.yml",
			"available": h.playbookExists("install_workers.yml"),
			"reason":    "Reinstallazione completa worker sui computer selezionati",
		},
		{
			"name":      "restart_workers",
			"playbook":  "restart_workers.yml",
			"available": h.playbookExists("restart_workers.yml"),
			"reason":    "Riavvio worker",
		},
	}

	return gin.H{
		"ansible_ready": h.isReady(),
		"playbooks_dir": playbooksDir,
		"version":       os.Getenv("VELOX_VERSION_NUMBER"),
		"actions":       actions,
	}
}

func (h *AnsibleHandlers) playbookExists(name string) bool {
	if h.manager == nil || h.manager.PlaybookDir() == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(h.manager.PlaybookDir(), name))
	return err == nil
}

func (h *AnsibleHandlers) resolveComputerIDs(ids []string) []string {
	if len(ids) == 0 || h.computers == nil {
		return ids
	}

	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if c, ok := h.computers.GetComputer(id); ok {
			out = append(out, c.Host)
			continue
		}
		out = append(out, id)
	}
	return out
}

func (h *AnsibleHandlers) collectTargets(c *gin.Context) []string {
	var body struct {
		ComputerIDs []string `json:"computer_ids"`
		WorkerIDs   []string `json:"worker_ids"`
		Workers     []string `json:"workers"`
		Hosts       []string `json:"hosts"`
	}
	_ = c.ShouldBindJSON(&body)

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
	return h.resolveComputerIDs(targets)
}

func (h *AnsibleHandlers) runActionForTargets(action string, targets []string) (string, error) {
	if h.manager == nil {
		return "", context.Canceled
	}

	playbookByAction := map[string]string{
		"update_workers":    "update_workers.yml",
		"install_workers":   "install_workers.yml",
		"preflight_workers": "preflight_workers.yml",
		"test_ssh":          "preflight_workers.yml",
	}

	playbook, ok := playbookByAction[action]
	if !ok {
		return "", fmt.Errorf("unsupported action: %s", action)
	}

	vars := map[string]interface{}{
		"master_url": firstNonEmpty(h.masterURL, os.Getenv("VELOX_MASTER_URL"), os.Getenv("VELOX_MASTER_SERVER_URL"), detectLocalMasterURL()),
	}
	return h.manager.RunPlaybook(context.Background(), strings.Join(targets, ","), playbook, vars)
}

// AnsibleComputersSummaryHandler returns a summary of Ansible computers.
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

// AnsibleComputersListHandler returns a list of Ansible computers.
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

// GetCapabilitiesHandler returns current capability metadata.
func (h *AnsibleHandlers) GetCapabilitiesHandler(c *gin.Context) {
	c.JSON(http.StatusOK, h.capabilitiesPayload())
}

// GetRunsHandler returns the current Ansible run list.
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

// RunActionHandler executes one of the supported update playbooks.
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

// RunShellHandler keeps the endpoint available, but the current implementation
// uses the update playbooks instead of arbitrary shell commands.
func (h *AnsibleHandlers) RunShellHandler(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error": "Not implemented yet",
		"hint":  "Use /ansible/computers/run_action for update/install/preflight",
	})
}

// TestSSHHandler is mapped to the preflight flow for now.
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

// AnsibleComputersSaveHandler creates or updates a computer record.
// POST /api/v1/admin/ansible/computers
//
// Body may include an `id` (preserved on update). When `id` is missing the
// computer host is used as the key — matching the existing list/save contract
// used by the SPA.
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

// AnsibleComputersDeleteHandler removes a computer record by id.
// DELETE /api/v1/admin/ansible/computers/:id
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

// AnsibleComputersLogsHandler returns recent ansible run history for a computer.
// GET /api/v1/admin/ansible/computers/logs/:id?limit=200
//
// The id may be either the computer host or its linked worker id; the handler
// matches against the ansible run history and returns the most recent N entries
// produced by playbooks targeting that host.
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
		RunID       string `json:"run_id"`
		Action      string `json:"action"`
		Playbook    string `json:"playbook,omitempty"`
		Status      string `json:"status"`
		StartedAt   string `json:"started_at"`
		CompletedAt string `json:"completed_at,omitempty"`
		Output      string `json:"output,omitempty"`
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

func detectLocalMasterURL() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "http://127.0.0.1:8000"
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}
		ip := ipNet.IP
		if ip.IsLoopback() || ip.To4() == nil {
			continue
		}
		return fmt.Sprintf("http://%s:8000", ip.String())
	}
	return "http://127.0.0.1:8000"
}
