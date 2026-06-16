package config

import (
	"os"
	"path/filepath"
	"strconv"
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
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_MAX_OPEN_CONNS")); n > 0 {
		c.MaxOpenConns = n
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_MAX_IDLE_CONNS")); n > 0 {
		c.MaxIdleConns = n
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_CONN_MAX_LIFETIME")); n > 0 {
		c.ConnMaxLifetime = n
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_CONN_MAX_IDLE_TIME")); n > 0 {
		c.ConnMaxIdleTime = n
	}
	return c
}
