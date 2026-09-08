package native

import (
	"reflect"
	"testing"
)

func TestSetEnvValueReplacesExistingEntries(t *testing.T) {
	got := setEnvValue([]string{"PATH=/bin", "VELOX_FFMPEG_DECODE_TELEMETRY=0", "X=1", "VELOX_FFMPEG_DECODE_TELEMETRY=old"}, "VELOX_FFMPEG_DECODE_TELEMETRY", "1")
	want := []string{"PATH=/bin", "X=1", "VELOX_FFMPEG_DECODE_TELEMETRY=1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("setEnvValue() = %#v, want %#v", got, want)
	}
}

func TestSetEnvIfAbsentPreservesExplicitOverride(t *testing.T) {
	explicit := []string{"VELOX_NATIVE_ENCODER_THREADS=9"}
	if got := setEnvIfAbsent(explicit, "VELOX_NATIVE_ENCODER_THREADS", 2); !reflect.DeepEqual(got, explicit) {
		t.Fatalf("setEnvIfAbsent changed explicit override: %#v", got)
	}

	got := setEnvIfAbsent(nil, "VELOX_NATIVE_ENCODER_THREADS", 2)
	want := []string{"VELOX_NATIVE_ENCODER_THREADS=2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("setEnvIfAbsent() = %#v, want %#v", got, want)
	}
}
