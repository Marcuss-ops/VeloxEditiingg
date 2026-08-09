package ansible

import (
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
	version   string
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

func (h *AnsibleHandlers) SetVersion(version string) {
	h.version = strings.TrimSpace(version)
}

func (h *AnsibleHandlers) isReady() bool {
	return h != nil && h.manager != nil && h.manager.Ready()
}

func (h *AnsibleHandlers) capabilitiesPayload() gin.H {
	playbooksDir := ""
	if h.manager != nil {
		playbooksDir = h.manager.PlaybookDir()
	}

	// Deployment, install, restart, preflight and SSH actions no longer
	// belong to this compatibility module. The authoritative surfaces are
	// fleetctl/FleetController (mutations) and WorkerNodeRegistry (connectivity).
	actions := []gin.H{}

	return gin.H{
		"ansible_ready": h.isReady(),
		"playbooks_dir": playbooksDir,
		"version":       h.version,
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

// runDeployWorkers is the retired canary/batch deploy seam. It fails closed;
// FleetController owns production rollout and its ledger.
func (h *AnsibleHandlers) runDeployWorkers(targets []string, batchSize int, canaryPercent float64) (string, error) {
	return "", ErrExecutorRemoved
}

func (h *AnsibleHandlers) runActionForTargets(action string, targets []string) (string, error) {
	return "", ErrExecutorRemoved
}
