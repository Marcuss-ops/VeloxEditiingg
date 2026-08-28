package transport

import (
	"encoding/json"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// grpc_stream_convert.go owns the typed proto ↔ ControlMessage
// conversions used by GRPCStreamTransport: helloToEnvelope (Hello
// handshake), messageToEnvelope (worker→master) and envelopeToMessage
// (master→worker). The transport lifecycle lives in grpc_stream.go.

// ---- Typed Proto ↔ ControlMessage conversion ----

// helloToEnvelope builds a typed WorkerToMasterEnvelope with a Hello message.
func (t *GRPCStreamTransport) helloToEnvelope(hello controltransport.WorkerHello) *pb.WorkerToMasterEnvelope {
	var caps *structpb.Struct
	if hello.Capabilities != nil {
		// Capability reports are assembled from Go-native values (including
		// ints). structpb.Struct accepts JSON value types, not every Go
		// numeric type; normalise through JSON before conversion so a single
		// unsupported value cannot silently drop the entire capability map.
		if raw, err := json.Marshal(hello.Capabilities); err == nil {
			var normalized map[string]interface{}
			if err := json.Unmarshal(raw, &normalized); err == nil {
				caps, _ = structpb.NewStruct(normalized)
			}
		}
	}

	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	t.mu.Unlock()

	return &pb.WorkerToMasterEnvelope{
		MessageId:       fmt.Sprintf("grpc-%s-%d", t.workerID, time.Now().UnixNano()),
		WorkerId:        hello.WorkerID,
		SequenceNumber:  seq,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: hello.ProtocolVersion,
		Msg: &pb.WorkerToMasterEnvelope_Hello{
			Hello: &pb.Hello{
				WorkerName:     hello.WorkerName,
				Hostname:       hello.Hostname,
				Version:        hello.Version,
				BundleVersion:  hello.BundleVersion,
				BundleHash:     hello.BundleHash,
				EngineVersion:  hello.EngineVersion,
				CredentialHash: hello.CredentialHash,
				Capabilities:   caps,
				WorkerClass:    hello.WorkerClass,
				RolloutGroup:   hello.RolloutGroup,
			},
		},
	}
}

// messageToEnvelope converts a ControlMessage to a typed WorkerToMasterEnvelope.
func (t *GRPCStreamTransport) messageToEnvelope(msg controltransport.ControlMessage) *pb.WorkerToMasterEnvelope {
	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	sid := t.sessionID
	t.mu.Unlock()

	env := &pb.WorkerToMasterEnvelope{
		MessageId:       msg.MessageID,
		WorkerId:        msg.WorkerID,
		SessionId:       sid,
		SequenceNumber:  seq,
		SentAt:          timestamppb.New(msg.SentAt),
		ProtocolVersion: msg.ProtocolVersion,
	}

	switch tp := msg.TypedPayload.(type) {
	case *pb.Heartbeat:
		env.Msg = &pb.WorkerToMasterEnvelope_Heartbeat{Heartbeat: tp}

	case *pb.TaskLeaseRenewal:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskLeaseRenewal{TaskLeaseRenewal: tp}

	case *pb.TaskAccepted:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskAccepted{TaskAccepted: tp}

	case *pb.TaskRejected:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskRejected{TaskRejected: tp}

	case *pb.TaskResult:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskResult{TaskResult: tp}

	case *pb.CommandAck:
		env.Msg = &pb.WorkerToMasterEnvelope_CommandAck{CommandAck: tp}

	case *pb.ArtifactUploaded:
		env.Msg = &pb.WorkerToMasterEnvelope_ArtifactUploaded{ArtifactUploaded: tp}

	case *pb.TaskOutputDeclared:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskOutputDeclared{TaskOutputDeclared: tp}

	case *pb.ArtifactUploadCompleted:
		env.Msg = &pb.WorkerToMasterEnvelope_ArtifactUploadCompleted{ArtifactUploadCompleted: tp}

	case *pb.AssetDownloadProgress:
		env.Msg = &pb.WorkerToMasterEnvelope_AssetDownloadProgress{AssetDownloadProgress: tp}

	case *pb.PrefetchLifecycleEvent:
		env.Msg = &pb.WorkerToMasterEnvelope_PrefetchLifecycleEvent{PrefetchLifecycleEvent: tp}
	}

	return env
}

// envelopeToMessage converts a typed MasterToWorkerEnvelope to a ControlMessage.
func (t *GRPCStreamTransport) envelopeToMessage(env *pb.MasterToWorkerEnvelope) controltransport.ControlMessage {
	sentAt := time.Now().UTC()
	if env.SentAt != nil {
		sentAt = env.SentAt.AsTime()
	}

	msg := controltransport.ControlMessage{
		MessageID:       env.MessageId,
		WorkerID:        env.WorkerId,
		SessionID:       env.SessionId,
		SequenceNumber:  env.SequenceNumber,
		SentAt:          sentAt,
		ProtocolVersion: env.ProtocolVersion,
	}

	switch m := env.Msg.(type) {
	case *pb.MasterToWorkerEnvelope_HelloAck:
		msg.Type = controltransport.MsgHelloAck

	case *pb.MasterToWorkerEnvelope_TaskOffer:
		msg.Type = controltransport.MsgTaskOffer
		msg.TypedPayload = m.TaskOffer

	case *pb.MasterToWorkerEnvelope_TaskLeaseGranted:
		msg.Type = controltransport.MsgTaskLeaseGranted
		msg.TypedPayload = m.TaskLeaseGranted

	case *pb.MasterToWorkerEnvelope_Command:
		msg.Type = controltransport.MsgCommand
		msg.TypedPayload = m.Command

	case *pb.MasterToWorkerEnvelope_CancelJob:
		msg.Type = controltransport.MsgCancelJob
		msg.TypedPayload = m.CancelJob

	case *pb.MasterToWorkerEnvelope_Drain:
		msg.Type = controltransport.MsgDrain
		msg.TypedPayload = m.Drain

	case *pb.MasterToWorkerEnvelope_ConfigurationUpdate:
		msg.Type = controltransport.MsgConfigurationUpdate
		msg.TypedPayload = m.ConfigurationUpdate

	case *pb.MasterToWorkerEnvelope_LeaseRevoked:
		msg.Type = controltransport.MsgLeaseRevoked
		msg.TypedPayload = m.LeaseRevoked

	case *pb.MasterToWorkerEnvelope_Ping:
		msg.Type = controltransport.MsgPing

	case *pb.MasterToWorkerEnvelope_TaskResultAck:
		msg.Type = controltransport.MsgTaskResultAck
		msg.TypedPayload = m.TaskResultAck

	case *pb.MasterToWorkerEnvelope_ArtifactUploadPlan:
		msg.Type = controltransport.MsgArtifactUploadPlan
		msg.TypedPayload = m.ArtifactUploadPlan

	case *pb.MasterToWorkerEnvelope_TaskCommitAck:
		msg.Type = controltransport.MsgTaskCommitAck
		msg.TypedPayload = m.TaskCommitAck

	case *pb.MasterToWorkerEnvelope_FutureAssetPlan:
		msg.Type = controltransport.MsgFutureAssetPlan
		msg.TypedPayload = m.FutureAssetPlan

	case *pb.MasterToWorkerEnvelope_CancelPrefetch:
		msg.Type = controltransport.MsgCancelPrefetch
		msg.TypedPayload = m.CancelPrefetch
	}

	return msg
}
