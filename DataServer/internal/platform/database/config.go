// Package database contains backend-neutral connection configuration and the
// database/sql connection factory. Environment binding belongs to the
// application config package; this package consumes typed values only.
package database

import "time"

// Driver identifies which SQL backend the Handle speaks.
type Driver string

const (
	DriverSQLite  Driver = "sqlite"
	DriverUnknown Driver = ""
)

// Config contains connection-pool and driver selection settings.
type Config struct {
	Driver          Driver
	SQLitePath      string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// ConfigFromApplication maps validated application settings to the database
// package's narrow connection configuration. No process environment is read.
func ConfigFromApplication(driver, sqlitePath string, maxOpen, maxIdle int, lifetime time.Duration) Config {
	return Config{
		Driver:          Driver(driver),
		SQLitePath:      sqlitePath,
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    maxIdle,
		ConnMaxLifetime: lifetime,
	}
}
