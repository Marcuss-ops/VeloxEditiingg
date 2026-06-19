# 07 — gRPC Proto Definitions

## Stato attuale

Non esiste alcun file `.proto` nel progetto. Non ci sono dipendenze gRPC in `go.mod` né lato master
né lato worker. La comunicazione è interamente HTTP/JSON.

## Stato target

File `.proto` che definisce il servizio `WorkerControl` con stream bidirezionale:

```proto
syntax = "proto3";

package velox.control.v1;
option go_package = "velox-shared/proto/control/v1;controlv1";

service WorkerControl {
  rpc Connect(stream WorkerToMaster) returns (stream MasterToWorker);
}

// --- Worker → Master ---
message WorkerToMaster {
  string message_id = 1;
  string worker_id = 2;
  string session_id = 3;
  int64 sequence_number = 4;
  string sent_at = 5;
  string protocol_version = 6;

  oneof payload {
    Hello hello = 10;
    Heartbeat heartbeat = 11;
    LeaseRenewal lease_renewal = 12;
    JobAccepted job_accepted = 13;
    JobRejected job_rejected = 14;
    JobProgress job_progress = 15;
    CommandAck command_ack = 16;
    JobResult job_result = 17;
    Goodbye goodbye = 18;
  }
}

// --- Master → Worker ---
message MasterToWorker {
  string message_id = 1;
  string worker_id = 2;
  string session_id = 3;
  int64 sequence_number = 4;
  string sent_at = 5;
  string protocol_version = 6;

  oneof payload {
    HelloAck hello_ack = 10;
    JobOffer job_offer = 11;
    Command command = 12;
    CancelJob cancel_job = 13;
    Drain drain = 14;
    ConfigurationUpdate configuration_update = 15;
    LeaseRevoked lease_revoked = 16;
    Ping ping = 17;
  }
}
```

Con tutti i sottotipi di messaggio:

```
Hello        { worker_id, worker_name, hostname, version, protocol_version,
               engine_version, bundle_hash, cpu, capacity, supported_job_types,
               credential_hash }
HelloAck     { session_id, token, expires_at, master_version }
Heartbeat    { status, current_jobs[], capabilities, versions,
               jobs_completed, jobs_failed, recent_logs, recent_errors }
LeaseRenewal { job_id, lease_id, attempt, new_expires_at }
JobOffer     { job_id, job_run_id, job_type, priority, parameters,
               timeout_secs, lease_id, lease_expiry, attempt }
JobAccepted  { job_id, lease_id }
JobRejected  { job_id, reason }
JobProgress  { job_id, percent, scene, total_scenes, stage }
JobResult    { job_id, job_run_id, status, output_json, error_message,
               artifact_id, output_sha256 }
Command      { command_id, type, params_json, expires_at, attempt }
CommandAck   { command_id, success, error_message }
CancelJob    { job_id, reason }
Drain        { reason }
Goodbye      { reason }
```

## File coinvolti

| File | Azione |
|---|---|
| `shared/proto/control/v1/control.proto` | Nuovo: definizione completa del servizio e messaggi |
| `shared/proto/control/v1/generate.go` | Nuovo: direttiva `go generate` |
| `shared/go.mod` | Modificare: aggiungere dipendenze protobuf e gRPC |

## Definition of Done

- [ ] `control.proto` definisce `service WorkerControl` con `rpc Connect(stream WorkerToMaster) returns (stream MasterToWorker)`
- [ ] Tutti i messaggi Worker→Master definiti: `Hello`, `Heartbeat`, `LeaseRenewal`, `JobAccepted`,
  `JobRejected`, `JobProgress`, `CommandAck`, `JobResult`, `Goodbye`
- [ ] Tutti i messaggi Master→Worker definiti: `HelloAck`, `JobOffer`, `Command`, `CancelJob`,
  `Drain`, `ConfigurationUpdate`, `LeaseRevoked`, `Ping`
- [ ] Ogni messaggio ha `message_id`, `worker_id`, `session_id`, `sequence_number`,
  `sent_at`, `protocol_version`
- [ ] Ogni comando ha `command_id`, `delivery_attempt`, `expires_at`, `idempotency_key`
- [ ] `buf.gen.yaml` o `generate.go` per la generazione Go
- [ ] Codice generato compila senza errori
- [ ] Nessuna dipendenza ciclica con `shared`

## Criteri di test

```bash
# Generazione
cd refactored/shared && buf generate

# Compilazione
cd refactored/shared && go build ./proto/...
```

## Dipendenze

- Nessuna per la definizione.
- Serve per 08 (GRPCStreamTransport).
