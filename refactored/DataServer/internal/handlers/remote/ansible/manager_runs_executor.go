package ansible

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (m *AnsibleRunManager) buildCommand(playbookPath, inventoryPath string, limitAliases []string, extraVars []string) []string {
	args := []string{
		"-i", inventoryPath,
		"--forks", "50",
		playbookPath,
	}
	if len(limitAliases) > 0 {
		args = append(args, "--limit", strings.Join(limitAliases, ","))
	}
	if len(extraVars) > 0 {
		args = append(args, "-e", strings.Join(extraVars, " "))
	}
	return args
}

func (m *AnsibleRunManager) runAsync(runID string, inventoryPath string, command []string, commandDisplay string, preamble string) {
	go func() {
		started := time.Now().Unix()
		_ = m.updateRun(runID, func(run *AnsibleRunRecord) {
			run.Status = "running"
			if run.StartedAt == 0 {
				run.StartedAt = started
			}
			run.Preamble = preamble
			run.Commands = []string{commandDisplay}
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cmd := exec.CommandContext(ctx, "ansible-playbook", command...)
		cmd.Env = append(os.Environ(),
			"ANSIBLE_HOST_KEY_CHECKING=False",
			"ANSIBLE_STDOUT_CALLBACK=default",
		)

		output, err := cmd.CombinedOutput()
		returnCode := 0
		if err != nil {
			returnCode = 1
			if exitErr, ok := err.(*exec.ExitError); ok {
				returnCode = exitErr.ExitCode()
			}
		}

		status := "ok"
		if returnCode != 0 {
			status = "failed"
		}

		_ = m.updateRun(runID, func(run *AnsibleRunRecord) {
			run.Status = status
			run.EndedAt = time.Now().Unix()
			run.ReturnCode = returnCode
			run.Output = string(output)
		})

		_ = os.Remove(inventoryPath)
	}()
}

// RunPlaybook executes an Ansible playbook on one or more target hosts.
func (m *AnsibleRunManager) RunPlaybook(ctx context.Context, host, playbook string, vars map[string]interface{}) (string, error) {
	if m == nil {
		return "", errors.New("ansible run manager unavailable")
	}
	if _, err := exec.LookPath("ansible-playbook"); err != nil {
		return "", err
	}

	hosts := splitRequestedHosts(host)
	if len(hosts) == 0 {
		return "", errors.New("host required")
	}

	playbookPath := playbook
	if !filepath.IsAbs(playbookPath) {
		playbookPath = filepath.Join(m.playbookDir, playbook)
	}
	if _, err := os.Stat(playbookPath); err != nil {
		return "", err
	}

	inventoryPath, aliasByTarget, err := m.writeInventoryFile(hosts)
	if err != nil {
		return "", err
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	if len(runID) == 0 {
		runID = fmt.Sprintf("%08x", rand.Uint32())
	}

	limitAliases := make([]string, 0, len(hosts))
	for _, hostName := range hosts {
		if alias := aliasByTarget[hostName]; alias != "" {
			limitAliases = append(limitAliases, alias)
		}
	}

	extraVars := buildExtraVars(vars)
	command := m.buildCommand(playbookPath, inventoryPath, limitAliases, extraVars)
	commandDisplay := fmt.Sprintf("ansible-playbook %s", strings.Join(command, " "))

	record := AnsibleRunRecord{
		ID:        runID,
		Action:    filepath.Base(playbook),
		Playbook:  filepath.Base(playbook),
		Hosts:     hosts,
		Commands:  []string{commandDisplay},
		Status:    "running",
		StartedAt: time.Now().Unix(),
		Preamble: fmt.Sprintf("ansibleDir=%s\nplaybook_path=%s\ncomando=%s\nlimit=%s\nhosts=%s\n",
			m.playbookDir,
			playbookPath,
			commandDisplay,
			strings.Join(limitAliases, ","),
			strings.Join(hosts, ","),
		),
	}
	if v, ok := vars["master_url"].(string); ok && strings.TrimSpace(v) != "" {
		record.MasterURL = v
		record.MasterURLSource = "body"
	}

	if err := m.saveRun(record); err != nil {
		return "", err
	}

	m.runAsync(runID, inventoryPath, command, commandDisplay, record.Preamble)
	return runID, nil
}
