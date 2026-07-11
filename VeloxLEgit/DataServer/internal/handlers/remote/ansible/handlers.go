package ansible

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

type AnsibleHandlers struct {
	manager   *AnsibleRunManager
	computers *AnsibleComputerManager
	dataDir   string
	masterURL string
}

func NewAnsibleHandlers(manager *AnsibleRunManager) *AnsibleHandlers {
	return &AnsibleHandlers{manager: manager}
}

func (h *AnsibleHandlers) SetComputerManager(computers *AnsibleComputerManager, dataDir string) {
	h.computers = computers
	h.dataDir = dataDir
}

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
			"name":      "deploy_workers",
			"playbook":  "update_workers.yml",
			"available": h.playbookExists("update_workers.yml"),
			"reason":    "Deploy progressivo con canary e batch",
		},
		{
			"name":      "update_workers",
			"playbook":  "update_workers.yml",
			"available": h.playbookExists("update_workers.yml"),
			"reason":    "Aggiorna codice sui computer selezionati",
		},
		{
			"name":      "rollout_update",
			"playbook":  "update_workers.yml",
			"available": h.playbookExists("update_workers.yml"),
			"reason":    "Rollout progressivo con canary e batch",
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

// runDeployWorkers is the canary/batch deploy entry-point. It generates
// inventory from the DB, builds the ansible-playbook command, and returns
// a run_id that the caller can poll for completion.
func (h *AnsibleHandlers) runDeployWorkers(targets []string, batchSize int, canaryPercent float64) (string, error) {
	if h.manager == nil {
		return "", context.Canceled
	}
	return h.manager.RunPlaybook("update_workers.yml", targets, "deploy_workers", h.masterURL, batchSize, canaryPercent)
}

func (h *AnsibleHandlers) runActionForTargets(action string, targets []string) (string, error) {
	if h.manager == nil {
		return "", context.Canceled
	}

	playbookByAction := map[string]string{
		"deploy_workers":    "update_workers.yml",
		"update_workers":    "update_workers.yml",
		"rollout_update":    "update_workers.yml",
		"install_workers":   "install_workers.yml",
		"preflight_workers": "preflight_workers.yml",
		"test_ssh":          "preflight_workers.yml",
	}

	playbook, ok := playbookByAction[action]
	if !ok {
		return "", fmt.Errorf("unsupported action: %s", action)
	}

	return h.manager.RunPlaybook(playbook, targets, action, h.masterURL, 0, 0)
}
