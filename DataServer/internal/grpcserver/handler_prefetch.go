package grpcserver

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	sharedfutureasset "velox-shared/futureasset"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// SendFutureAssetPlan delivers a complete worker-scoped plan through the
// canonical control stream. The caller owns placement and reservation policy;
// this method only validates the snapshot and queues the typed envelope.
// Workers without prefetch.plan.v1 are deliberately rejected so an older
// runtime never receives an unknown optimization message.
func (h *Handler) SendFutureAssetPlan(ctx context.Context, plan sharedfutureasset.Plan) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	sess := h.getSession(plan.WorkerID)
	if sess == nil {
		return fmt.Errorf("prefetch: worker %q has no active session", plan.WorkerID)
	}
	sess.capabilitiesMu.RLock()
	supported := sess.capabilities.Has(controltransport.CapabilityFutureAssetPrefetchV1)
	sess.capabilitiesMu.RUnlock()
	if !supported {
		return fmt.Errorf("prefetch: worker %q does not advertise %s", plan.WorkerID, controltransport.CapabilityFutureAssetPrefetchV1)
	}
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("future-asset-plan-%s-%d", plan.WorkerID, time.Now().UnixNano()),
		WorkerId:        plan.WorkerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg:             &pb.MasterToWorkerEnvelope_FutureAssetPlan{FutureAssetPlan: plan.ToProto()},
	}
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		return fmt.Errorf("prefetch: worker %q control stream is full or closed", plan.WorkerID)
	}
	return nil
}

// SendCancelPrefetch is the low-latency hint path. The subsequent full plan
// remains the reconciliation source of truth if this message is lost.
func (h *Handler) SendCancelPrefetch(ctx context.Context, workerID string, cancel *pb.CancelPrefetch) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if cancel == nil || cancel.GetJobId() == "" || cancel.GetReservationId() == "" {
		return fmt.Errorf("prefetch: cancel requires job_id and reservation_id")
	}
	sess := h.getSession(workerID)
	if sess == nil {
		return fmt.Errorf("prefetch: worker %q has no active session", workerID)
	}
	sess.capabilitiesMu.RLock()
	supported := sess.capabilities.Has(controltransport.CapabilityFutureAssetPrefetchV1)
	sess.capabilitiesMu.RUnlock()
	if !supported {
		return fmt.Errorf("prefetch: worker %q does not advertise %s", workerID, controltransport.CapabilityFutureAssetPrefetchV1)
	}
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("cancel-prefetch-%s-%s-%d", workerID, cancel.GetJobId(), time.Now().UnixNano()),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg:             &pb.MasterToWorkerEnvelope_CancelPrefetch{CancelPrefetch: cancel},
	}
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		return fmt.Errorf("prefetch: worker %q control stream is full or closed", workerID)
	}
	return nil
}
