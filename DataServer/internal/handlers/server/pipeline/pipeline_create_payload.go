package pipeline

// pipeline_create_payload.go owns the typed API contract and the
// remote payload builder for POST /api/v1/pipeline-runs. The handler
// itself lives in pipeline_create.go.

// CreatePipelineRunRequest is the typed, versioned API contract for
// POST /api/v1/pipeline-runs. It is the canonical entry point for a
// client-initiated generation pipeline.
//
// The `generation` block drives the remote script-generation service;
// `output` describes the desired video format; `video_metadata` carries
// the social-platform metadata; `delivery_plan` is the list of
// destinations the finished artifact should be delivered to.
type CreatePipelineRunRequest struct {
	IdempotencyKey string             `json:"idempotency_key" binding:"required"`
	UserID         string             `json:"user_id"`
	CampaignID     string             `json:"campaign_id"`
	CampaignItemID string             `json:"campaign_item_id"`
	Generation     *GenerationSpec    `json:"generation"`
	Output         *OutputSpec        `json:"output"`
	VideoMetadata  *VideoMetadataSpec `json:"video_metadata"`
	DeliveryPlan   []DeliveryPlanItem `json:"delivery_plan"`
}

// GenerationSpec describes the script-generation parameters sent to the
// remote engine.
type GenerationSpec struct {
	Topic      string `json:"topic"`
	Language   string `json:"language"`
	Style      string `json:"style"`
	SceneCount int    `json:"scene_count"`
	SourceText string `json:"source_text"`
}

// OutputSpec describes the desired video output format.
type OutputSpec struct {
	Format string `json:"format"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	FPS    int    `json:"fps"`
}

// VideoMetadataSpec carries the social-platform metadata for the
// finished video.
type VideoMetadataSpec struct {
	Title         string   `json:"title"`
	Description   string   `json:"description"`
	Tags          []string `json:"tags"`
	PrivacyStatus string   `json:"privacy_status"`
}

// DeliveryPlanItem is a single destination in the delivery plan.
type DeliveryPlanItem struct {
	Provider    string `json:"provider"`
	ChannelID   string `json:"channel_id"`
	PublishAt   string `json:"publish_at"`
	Destination string `json:"destination"`
}

// buildRemotePayload converts the typed CreatePipelineRunRequest into
// the map[string]interface{} shape the remote engine's
// /api/script/generate-with-images endpoint expects. The remote engine
// still consumes the legacy flat shape; this mapping isolates the
// versioned API contract from the remote engine's wire format.
func buildRemotePayload(req *CreatePipelineRunRequest) map[string]interface{} {
	payload := map[string]interface{}{}

	if req.Generation != nil {
		if req.Generation.Topic != "" {
			payload["topic"] = req.Generation.Topic
		}
		if req.Generation.SourceText != "" {
			payload["source_text"] = req.Generation.SourceText
		}
		if req.Generation.Language != "" {
			payload["language"] = req.Generation.Language
		}
		if req.Generation.Style != "" {
			payload["style"] = req.Generation.Style
		}
		if req.Generation.SceneCount > 0 {
			payload["scene_count"] = req.Generation.SceneCount
		}
	}

	if req.Output != nil {
		if req.Output.Format != "" {
			payload["format"] = req.Output.Format
		}
		if req.Output.Width > 0 {
			payload["width"] = req.Output.Width
		}
		if req.Output.Height > 0 {
			payload["height"] = req.Output.Height
		}
		if req.Output.FPS > 0 {
			payload["fps"] = req.Output.FPS
		}
	}

	if req.VideoMetadata != nil {
		meta := map[string]interface{}{}
		if req.VideoMetadata.Title != "" {
			meta["title"] = req.VideoMetadata.Title
		}
		if req.VideoMetadata.Description != "" {
			meta["description"] = req.VideoMetadata.Description
		}
		if len(req.VideoMetadata.Tags) > 0 {
			meta["tags"] = req.VideoMetadata.Tags
		}
		if req.VideoMetadata.PrivacyStatus != "" {
			meta["privacy_status"] = req.VideoMetadata.PrivacyStatus
		}
		if len(meta) > 0 {
			payload["video_metadata"] = meta
		}
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			item := map[string]interface{}{}
			if d.Provider != "" {
				item["provider"] = d.Provider
			}
			if d.ChannelID != "" {
				item["channel_id"] = d.ChannelID
			}
			if d.PublishAt != "" {
				item["publish_at"] = d.PublishAt
			}
			if d.Destination != "" {
				item["destination_id"] = d.Destination
			}
			plan = append(plan, item)
		}
		payload["delivery_plan"] = plan
	}

	return payload
}
