package main

import (
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/platform/database"
)

func databaseConfigFromConfig(dcfg config.DatabaseConfig) database.Config {
	return database.Config{
		Driver:          database.Driver(strings.ToLower(strings.TrimSpace(dcfg.Driver))),
		SQLitePath:      dcfg.DBPath,
		URL:             dcfg.URL,
		MaxOpenConns:    dcfg.MaxOpenConns,
		MaxIdleConns:    dcfg.MaxIdleConns,
		ConnMaxLifetime: dcfg.ConnMaxLifetime,
	}
}

func schemaModeLabel(migrateOnStart bool) string {
	if migrateOnStart {
		return "master-owned (forward, migrations+post-adjustments run on boot)"
	}
	return "forward-only (external tool owns schema; master skips migrations+post-adjustments)"
}
