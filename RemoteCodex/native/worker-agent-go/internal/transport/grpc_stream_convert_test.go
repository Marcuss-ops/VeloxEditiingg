package transport

import (
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
)

func TestProgressiveUploadMessagesConvertToTypedEnvelopes(t *testing.T) {
	transport := NewGRPCStreamTransport("", "worker-test")
	intent := &pb.ArtifactUploadIntent{
		TaskId:         "task-1",
		AttemptId:      "attempt-1",
		LeaseId:        "lease-1",
		WorkerSpoolKey: "task-1:output:0",
		OutputKind:     "final_video",
	}
	workerEnvelope := transport.messageToEnvelope(controltransport.ControlMessage{
		MessageID:       "message-1",
		WorkerID:        "worker-test",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		SentAt:          time.Now().UTC(),
		TypedPayload:    intent,
	})
	workerIntent, ok := workerEnvelope.Msg.(*pb.WorkerToMasterEnvelope_ArtifactUploadIntent)
	if !ok || workerIntent.ArtifactUploadIntent != intent {
		t.Fatalf("worker intent conversion = %T/%v, want typed intent", workerEnvelope.Msg, workerEnvelope.GetArtifactUploadIntent())
	}

	plan := &pb.ArtifactEarlyUploadPlan{
		TaskId:     "task-1",
		AttemptId:  "attempt-1",
		ArtifactId: "artifact-1",
		UploadId:   "upload-1",
	}
	masterEnvelope := &pb.MasterToWorkerEnvelope{
		Msg: &pb.MasterToWorkerEnvelope_ArtifactEarlyUploadPlan{ArtifactEarlyUploadPlan: plan},
	}
	message := transport.envelopeToMessage(masterEnvelope)
	gotPlan, ok := message.TypedPayload.(*pb.ArtifactEarlyUploadPlan)
	if !ok || gotPlan != plan || message.Type != controltransport.MsgArtifactEarlyUploadPlan {
		t.Fatalf("master early plan conversion = %s/%T/%v, want typed early plan", message.Type, message.TypedPayload, gotPlan)
	}
}
