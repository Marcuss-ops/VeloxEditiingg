package config

import (
	"os"
	"path/filepath"
)

func loadDatabaseConfig(dataDir, runtimeDir string) DatabaseConfig {
	c := DatabaseConfig{
		Driver:          os.Getenv("VELOX_DB_DRIVER"),
		MaxOpenConns:    50,
		MaxIdleConns:    10,
		ConnMaxLifetime: 1800,
		ConnMaxIdleTime: 300,
	}
	if c.Driver == "" {
		c.Driver = "sqlite3"
	}
	c.DSN = os.Getenv("VELOX_DB_DSN")
	if c.DSN == "" && dataDir != "" {
		c.DSN = dataDir + "/velox.db"
	} else if c.DSN == "" {
		c.DSN = filepath.Join(runtimeDir, "data", "velox.db")
	}
	c.MaxOpenConns = intFromEnv("VELOX_DB_MAX_OPEN_CONNS", 50, 1)
	c.MaxIdleConns = intFromEnv("VELOX_DB_MAX_IDLE_CONNS", 10, 1)
	c.ConnMaxLifetime = intFromEnv("VELOX_DB_CONN_MAX_LIFETIME", 1800, 1)
	c.ConnMaxIdleTime = intFromEnv("VELOX_DB_CONN_MAX_IDLE_TIME", 300, 1)
	return c
}
