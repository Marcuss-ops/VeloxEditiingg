package config

import (
	"os"
	"path/filepath"
)

func loadAnsibleConfig(dataDir string) AnsibleConfig {
	c := AnsibleConfig{
		PlaybookDir: os.Getenv("VELOX_ANSIBLE_PLAYBOOK_DIR"),
	}
	if c.PlaybookDir == "" {
		c.PlaybookDir = filepath.Join(dataDir, "ansible", "playbooks")
	}
	return c
}
