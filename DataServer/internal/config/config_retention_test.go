package config

import "testing"

func TestRetentionConfigQuarantineDaysUsesNewNameAndLegacyFallback(t *testing.T) {
	if got := loadRetentionConfig(NewRawConfig(nil)); got.ArtifactQuarantineDays != 7 || got.JobEventsDays != 30 {
		t.Fatalf("retention defaults = %+v, want quarantine=7d and job events=30d", got)
	}
	legacy := loadRetentionConfig(NewRawConfig(map[string]string{"VELOX_RETENTION_ARTIFACT_QUARANTINE_DAYS": "11"}))
	if legacy.ArtifactQuarantineDays != 11 {
		t.Fatalf("legacy quarantine setting = %d, want 11", legacy.ArtifactQuarantineDays)
	}
	current := loadRetentionConfig(NewRawConfig(map[string]string{
		"VELOX_RETENTION_ARTIFACT_QUARANTINE_DAYS": "11",
		"VELOX_QUARANTINE_GC_DAYS":                 "5",
	}))
	if current.ArtifactQuarantineDays != 5 {
		t.Fatalf("new quarantine setting = %d, want 5", current.ArtifactQuarantineDays)
	}
	invalid := loadRetentionConfig(NewRawConfig(map[string]string{"VELOX_RETENTION_JOB_EVENTS_DAYS": "0"}))
	if invalid.JobEventsDays != 30 {
		t.Fatalf("zero job-event retention = %d, want safe default 30", invalid.JobEventsDays)
	}
}
