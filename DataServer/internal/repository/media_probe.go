package repository

import (
	"context"
	"time"
)

type MediaProbeEnqueueParams struct {
	ArtifactID           string
	SHA256               string
	StorageKey           string
	ExpectedAudioStreams int
	DestinationID        string
	MaxAttempts          int
	Now                  time.Time
}

type MediaProbeEnqueuer interface {
	EnqueueMediaProbe(context.Context, MediaProbeEnqueueParams) error
}
