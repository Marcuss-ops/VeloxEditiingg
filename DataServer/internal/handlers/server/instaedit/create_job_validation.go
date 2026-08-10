package instaedit

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
)

// validateCreateJobCommand validates the request fields and returns the
// strictly validated render specification for payload construction.
func validateCreateJobCommand(cmd CreateJobCmd) (map[string]any, error) {
	if strings.TrimSpace(cmd.ProjectID) == "" {
		return nil, fmt.Errorf("%w: project_id is required", ErrInvalidPayload)
	}
	if !cmd.RenderOnly && len(cmd.Destinations) == 0 {
		return nil, fmt.Errorf("%w: delivery_plan.destinations is required unless render_only=true", ErrInvalidPayload)
	}

	var renderSpec map[string]any
	if len(cmd.RenderSpec) > 0 {
		if err := json.Unmarshal(cmd.RenderSpec, &renderSpec); err != nil {
			return nil, fmt.Errorf("%w: invalid render_spec JSON: %v", ErrBadRequest, err)
		}
	} else {
		renderSpec = map[string]any{}
	}

	if err := contract.StrictValidatePayload(renderSpec); err != nil {
		return nil, fmt.Errorf("%w: invalid render_spec: %v", ErrInvalidPayload, err)
	}
	return renderSpec, nil
}
