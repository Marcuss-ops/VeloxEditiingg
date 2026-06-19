package config

import (
	"os"
	"strings"
)

func loadWorkersConfig() WorkersConfig {
	c := WorkersConfig{
		MaxJobAttempts:   3,
		HeartbeatTimeout: 900,
		VersionNumber:    "v1.0.6",
	}
	c.AllowedWorkers = os.Getenv("VELOX_ALLOWED_WORKERS")
	c.ForceSingleWorker = os.Getenv("VELOX_FORCE_SINGLE_WORKER")
	c.MaxJobAttempts = intFromEnv("VELOX_MAX_JOB_ATTEMPTS", 3, 1)
	c.AllowlistRegistered = boolFromEnv("VELOX_ALLOWLIST_ALLOW_REGISTERED", false)
	c.BundleDir = os.Getenv("VELOX_WORKER_BUNDLE_DIR")
	c.CodeVersion = os.Getenv("VELOX_CODE_VERSION")
	c.VersionNumber = os.Getenv("VELOX_VERSION_NUMBER")
	if c.VersionNumber == "" {
		if v, err := os.ReadFile("../../VERSION.txt"); err == nil {
			c.VersionNumber = strings.TrimSpace(string(v))
		}
	}
	if c.VersionNumber == "" {
		c.VersionNumber = "v1.0.6"
	}
	if c.CodeVersion == "" {
		c.CodeVersion = c.VersionNumber
	}
	c.HeartbeatTimeout = intFromEnv("VELOX_WORKER_HEARTBEAT_TIMEOUT", 900, 1)
	c.ScriptDir = os.Getenv("VELOX_SCRIPT_DIR")
	c.MasterURL = GetMasterURL()
	c.MasterServerURL = GetMasterServerURL()
	if ips := os.Getenv("VELOX_ALLOWED_WORKER_IPS"); ips != "" {
		c.AllowedIPs = parseCommaList(ips)
	}
	return c
}
