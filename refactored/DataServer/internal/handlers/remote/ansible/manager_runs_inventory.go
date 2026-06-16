package ansible

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"velox-shared/payload"
)

func splitRequestedHosts(hosts string) []string {
	parts := strings.FieldsFunc(hosts, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func sanitizeInventoryAlias(v string) string {
	trimmed := strings.TrimSpace(v)
	for strings.HasPrefix(trimmed, "host_") {
		trimmed = strings.TrimPrefix(trimmed, "host_")
	}
	replacer := strings.NewReplacer(".", "_", "-", "_", ":", "_", " ", "_", "/", "_")
	return "host_" + replacer.Replace(trimmed)
}

func buildExtraVars(vars map[string]interface{}) []string {
	if len(vars) == 0 {
		return nil
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		if k == "inventory_path" || k == "inventory_file" || k == "inventory" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		switch v := vars[key].(type) {
		case string:
			out = append(out, fmt.Sprintf("%s=%s", key, v))
		case bool:
			out = append(out, fmt.Sprintf("%s=%t", key, v))
		default:
			raw, _ := json.Marshal(v)
			out = append(out, fmt.Sprintf("%s=%s", key, string(raw)))
		}
	}
	return out
}

func (m *AnsibleRunManager) loadComputerInventory(hosts []string) (map[string]AnsibleComputer, map[string]string, error) {
	if m.computerMgr == nil {
		return nil, nil, fmt.Errorf("computer manager not configured")
	}
	if err := m.computerMgr.LoadComputers(); err != nil {
		return nil, nil, err
	}

	allComputers := m.computerMgr.ListComputers()
	aliasByTarget := make(map[string]string, len(hosts))
	selected := make(map[string]AnsibleComputer, len(hosts))

	for _, host := range hosts {
		if host == "" {
			continue
		}

		if computer, ok := allComputers[host]; ok {
			aliasByTarget[host] = sanitizeInventoryAlias(host)
			selected[host] = computer
			continue
		}

		found := false
		for id, computer := range allComputers {
			if strings.EqualFold(id, host) || strings.EqualFold(computer.Host, host) {
				aliasByTarget[host] = sanitizeInventoryAlias(id)
				selected[host] = computer
				found = true
				break
			}
		}
		if found {
			continue
		}

		aliasByTarget[host] = sanitizeInventoryAlias(host)
		selected[host] = AnsibleComputer{
			Host:         host,
			AnsibleUser:  "pierone",
			Enabled:      true,
			Availability: "UNKNOWN",
		}
	}

	return selected, aliasByTarget, nil
}

func (m *AnsibleRunManager) writeInventoryFile(hosts []string) (string, map[string]string, error) {
	selected, aliasByTarget, err := m.loadComputerInventory(hosts)
	if err != nil {
		return "", nil, err
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("inventory_%d_*.yml", time.Now().UnixNano()))
	if err != nil {
		return "", nil, err
	}

	lines := []string{
		"all:",
		"  children:",
		"    workers:",
		"      hosts:",
	}
	for _, host := range hosts {
		c, ok := selected[host]
		if !ok {
			continue
		}

		alias := aliasByTarget[host]
		if alias == "" {
			alias = sanitizeInventoryAlias(host)
		}

		lines = append(lines, fmt.Sprintf("        %s:", alias))
		lines = append(lines, fmt.Sprintf("          ansible_host: %s", c.Host))
		lines = append(lines, fmt.Sprintf("          ansible_user: %s", payload.FirstNonEmpty(c.AnsibleUser, "pierone")))
		lines = append(lines, "          ansible_connection: ssh")
		lines = append(lines, "          ansible_ssh_common_args: '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'")
		lines = append(lines, "          ansible_python_interpreter: auto")
		if secretRef := m.computerMgr.GetSecretRef(c.Host); secretRef != "" {
			if secret, err := m.computerMgr.ResolveSecret(secretRef); err == nil && secret != "" {
				lines = append(lines, fmt.Sprintf("          ansible_password: %s", secret))
				lines = append(lines, fmt.Sprintf("          ansible_ssh_pass: %s", secret))
			}
		}
		if c.SSHKeyPath != "" {
			lines = append(lines, fmt.Sprintf("          ansible_ssh_private_key_file: %s", c.SSHKeyPath))
		}
		if c.WorkerID != "" {
			lines = append(lines, fmt.Sprintf("          worker_id: %s", sanitizeInventoryAlias(c.WorkerID)))
		} else {
			lines = append(lines, fmt.Sprintf("          worker_id: %s", alias))
		}
		if c.Enabled {
			lines = append(lines, "          ansible_become: true", "          ansible_become_method: sudo")
			if secretRef := m.computerMgr.GetSecretRef(c.Host); secretRef != "" {
				if secret, err := m.computerMgr.ResolveSecret(secretRef); err == nil && secret != "" {
					lines = append(lines, fmt.Sprintf("          ansible_become_password: %s", secret))
				}
			}
		}
	}

	if _, err := tmpFile.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		_ = tmpFile.Close()
		return "", nil, err
	}

	if err := tmpFile.Close(); err != nil {
		return "", nil, err
	}

	return tmpFile.Name(), aliasByTarget, nil
}
