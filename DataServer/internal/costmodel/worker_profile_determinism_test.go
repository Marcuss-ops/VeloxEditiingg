package costmodel

import "testing"

func TestBuildWorkerProfile_DeterministicAllExecutors(t *testing.T) {
	caps := map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{
				"resource_class": "cpu",
				"temporal_mode":  "frame_local",
				"deterministic":  true,
			},
			map[string]interface{}{
				"resource_class": "gpu",
				"temporal_mode":  "global",
				"deterministic":  true,
			},
		},
	}

	w := BuildWorkerProfile("w-deterministic", true, false, "online", 0, 0, caps)
	if !w.Deterministic {
		t.Fatalf("Deterministic=false, want true when every executor reports deterministic=true: %+v", w)
	}
}

func TestBuildWorkerProfile_DeterministicMixedExecutors(t *testing.T) {
	caps := map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{
				"resource_class": "cpu",
				"temporal_mode":  "frame_local",
				"deterministic":  true,
			},
			map[string]interface{}{
				"resource_class": "gpu",
				"temporal_mode":  "global",
				"deterministic":  false,
			},
		},
	}

	w := BuildWorkerProfile("w-mixed", true, false, "online", 0, 0, caps)
	if w.Deterministic {
		t.Fatalf("Deterministic=true, want false when any executor reports deterministic=false: %+v", w)
	}
}

func TestBuildWorkerProfile_DeterministicMissingSignalStaysConservative(t *testing.T) {
	caps := map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{
				"resource_class": "cpu",
				"temporal_mode":  "frame_local",
			},
		},
	}

	w := BuildWorkerProfile("w-legacy-executor", true, false, "online", 0, 0, caps)
	if w.Deterministic {
		t.Fatalf("Deterministic=true, want conservative false when executors omit the deterministic signal: %+v", w)
	}
}
