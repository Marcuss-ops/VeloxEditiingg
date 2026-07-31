package enqueue

import "testing"

func TestSceneVoiceoverDurationSeconds_Precedence(t *testing.T) {
	tests := []struct {
		name  string
		scene map[string]interface{}
		want  float64
	}{
		{
			name: "top-level canonical value wins",
			scene: map[string]interface{}{
				"voiceover_duration_seconds": 12.5,
				"voiceover":                  map[string]interface{}{"duration_ms": 7000},
			},
			want: 12.5,
		},
		{
			name: "nested seconds fallback",
			scene: map[string]interface{}{
				"voiceover": map[string]interface{}{"duration_seconds": 7.25},
			},
			want: 7.25,
		},
		{
			name: "nested milliseconds fallback",
			scene: map[string]interface{}{
				"voiceover": map[string]interface{}{"duration_ms": 5250},
			},
			want: 5.25,
		},
		{name: "nil scene returns zero", scene: nil, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sceneVoiceoverDurationSeconds(tt.scene); got != tt.want {
				t.Fatalf("sceneVoiceoverDurationSeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSceneClipDurationSeconds_Precedence(t *testing.T) {
	tests := []struct {
		name  string
		scene map[string]interface{}
		want  float64
	}{
		{
			name: "canonical value wins over legacy alias",
			scene: map[string]interface{}{
				"final_clip_duration_seconds": 3.5,
				"clip_duration_seconds":       10.0,
			},
			want: 3.5,
		},
		{
			name:  "legacy alias fallback",
			scene: map[string]interface{}{"clip_duration_seconds": 8.25},
			want:  8.25,
		},
		{
			name: "nested seconds fallback",
			scene: map[string]interface{}{
				"clip": map[string]interface{}{"duration_seconds": 4.5},
			},
			want: 4.5,
		},
		{
			name: "nested milliseconds fallback",
			scene: map[string]interface{}{
				"clip": map[string]interface{}{"duration_ms": 6250},
			},
			want: 6.25,
		},
		{name: "missing duration returns zero", scene: map[string]interface{}{}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sceneClipDurationSeconds(tt.scene); got != tt.want {
				t.Fatalf("sceneClipDurationSeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}
