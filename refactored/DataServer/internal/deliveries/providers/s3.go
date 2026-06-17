// Package deliveries/providers: S3 skeleton.
//
// S3Provider is the contract-complete stub for an S3-backed provider.
// Until the deployment is wired with AWS credentials + bucket name,
// Deliver refuses to act and returns ErrProviderNotConfigured. The
// runner marks the delivery FAILED with no retry.
package providers

import (
	"context"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

// S3Provider is the production S3 adapter (skeleton).
type S3Provider struct{}

// NewS3Provider constructs a S3Provider stub.
func NewS3Provider() *S3Provider { return &S3Provider{} }

// Name returns "s3".
func (s *S3Provider) Name() string { return "s3" }

// Deliver always returns ErrProviderNotConfigured until the S3 client +
// bucket credentials are propagated via internal/config.
func (s *S3Provider) Deliver(_ context.Context, _ *store.Artifact, _ *deliveries.Destination) (*deliveries.Result, error) {
	return nil, deliveries.ErrProviderNotConfigured
}
