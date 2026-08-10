package instaedit

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidateCreateJobCommand(t *testing.T) {
	tests := []struct {
		name          string
		cmd           CreateJobCmd
		wantErr       error
		wantScenes    bool
		wantVideoName string
	}{
		{
			name: "project id required",
			cmd: CreateJobCmd{
				Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
			},
			wantErr: ErrInvalidPayload,
		},
		{
			name: "destinations required for normal render",
			cmd: CreateJobCmd{
				ProjectID: "project-1",
			},
			wantErr: ErrInvalidPayload,
		},
		{
			name: "whitespace project id rejected",
			cmd: CreateJobCmd{
				ProjectID:    "   ",
				Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
			},
			wantErr: ErrInvalidPayload,
		},
		{
			name: "malformed render spec",
			cmd: CreateJobCmd{
				ProjectID:    "project-1",
				Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
				RenderSpec:   json.RawMessage(`not-json`),
			},
			wantErr: ErrBadRequest,
		},
		{
			name: "strict contract rejects legacy alias",
			cmd: CreateJobCmd{
				ProjectID:    "project-1",
				Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
				RenderSpec:   json.RawMessage(`{"video_name":"x","voiceover_path":"/legacy.mp3"}`),
			},
			wantErr: ErrInvalidPayload,
		},
		{
			name: "render only does not require destinations",
			cmd: CreateJobCmd{
				ProjectID:  "project-1",
				RenderOnly: true,
				RenderSpec: json.RawMessage(`{"scenes":[]}`),
			},
			wantScenes: true,
		},
		{
			name: "valid command returns parsed spec",
			cmd: CreateJobCmd{
				ProjectID:    "project-1",
				Destinations: []CreateDestinationCmd{{ExternalDestinationID: "ext-1"}},
				RenderSpec:   json.RawMessage(`{"video_name":"Video","scenes":[]}`),
			},
			wantScenes:    true,
			wantVideoName: "Video",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateCreateJobCommand(tt.cmd)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				if got != nil {
					t.Fatalf("expected no render spec on error, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("expected parsed render spec")
			}
			if tt.wantScenes {
				scenes, ok := got["scenes"].([]any)
				if !ok {
					t.Fatalf("expected parsed scenes array, got %#v", got["scenes"])
				}
				if len(scenes) != 0 {
					t.Fatalf("expected empty scenes array, got %#v", scenes)
				}
			}
			if tt.wantVideoName != "" {
				if videoName, ok := got["video_name"].(string); !ok || videoName != tt.wantVideoName {
					t.Fatalf("expected video_name %q, got %#v", tt.wantVideoName, got["video_name"])
				}
			}
			for _, field := range []string{"delivery_plan", "scenes_json", "render_only"} {
				if _, present := got[field]; present {
					t.Fatalf("validation helper unexpectedly added payload field %q: %#v", field, got[field])
				}
			}
		})
	}
}
