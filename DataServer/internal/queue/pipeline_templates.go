package queue

func PipelineAudioVideo(jobID string, audioPayload, videoPayload map[string]interface{}) []*JobStep {
	return []*JobStep{
		{
			StepID:    jobID + "_audio",
			JobID:     jobID,
			StepName:  "generate_audio",
			StepOrder: 0,
			Status:    StepStatusPending,
			Payload:   audioPayload,
		},
		{
			StepID:       jobID + "_video",
			JobID:        jobID,
			StepName:     "generate_video",
			StepOrder:    1,
			Status:       StepStatusPending,
			Dependencies: []string{jobID + "_audio"},
			Payload:      videoPayload,
		},
	}
}

func PipelineScriptAudioVideo(jobID string, scriptPayload, audioPayload, videoPayload map[string]interface{}) []*JobStep {
	return []*JobStep{
		{
			StepID:    jobID + "_script",
			JobID:     jobID,
			StepName:  "generate_script",
			StepOrder: 0,
			Status:    StepStatusPending,
			Payload:   scriptPayload,
		},
		{
			StepID:       jobID + "_audio",
			JobID:        jobID,
			StepName:     "generate_audio",
			StepOrder:    1,
			Status:       StepStatusPending,
			Dependencies: []string{jobID + "_script"},
			Payload:      audioPayload,
		},
		{
			StepID:       jobID + "_video",
			JobID:        jobID,
			StepName:     "generate_video",
			StepOrder:    2,
			Status:       StepStatusPending,
			Dependencies: []string{jobID + "_audio"},
			Payload:      videoPayload,
		},
	}
}
