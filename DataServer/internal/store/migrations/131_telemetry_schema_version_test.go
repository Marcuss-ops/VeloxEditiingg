package migrations

import (
	_ "embed"
	"testing"
)

//go:embed sqlite/110_task_execution_events.sql
var sqliteSQL110ForTelemetrySchemaVersion string

//go:embed sqlite/131_telemetry_schema_version.sql
var sqliteSQL131TelemetrySchemaVersion string

func TestMigration131_TaskExecutionEventsTelemetrySchemaVersion(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL110ForTelemetrySchemaVersion)
	applyMigrationSQL(t, db, sqliteSQL131TelemetrySchemaVersion)

	if !columnExists(t, db, "task_execution_events", "telemetry_schema_version") {
		t.Fatal("task_execution_events.telemetry_schema_version is missing")
	}
}
