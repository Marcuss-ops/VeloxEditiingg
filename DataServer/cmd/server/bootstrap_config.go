package main

import (
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/platform/database"
)

func databaseConfigFromConfig(dcfg config.DatabaseConfig) database.Config {
	return database.ConfigFromApplication(
		strings.ToLower(strings.TrimSpace(dcfg.Driver)),
		dcfg.URL,
		dcfg.DBPath,
		dcfg.MaxOpenConns,
		dcfg.MaxIdleConns,
		dcfg.ConnMaxLifetime,
	)
}

func schemaModeLabel(migrateOnStart bool) string {
	if migrateOnStart {
		return "master-owned (forward, migrations+post-adjustments run on boot)"
	}
	return "forward-only (external tool owns schema; master skips migrations+post-adjustments)"
}
