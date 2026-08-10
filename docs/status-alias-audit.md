# Status alias audit

The repository uses several status domains. Equal-looking wire values must not
be interpreted as interchangeable across those domains.

| Value | Domain | Meaning | Boundary policy |
|---|---|---|---|
| `completed` | `contract.InputAssemblyStatus` | Producer has assembled a complete handoff payload | Accepted by canonical producer/forwarding gates |
| `completed_with_warnings` | `contract.InputAssemblyStatus` | Producer handoff is complete with non-fatal warnings | Defined in the shared contract; not accepted by the current remote-engine/CreatorPush completion gate |
| `SUCCEEDED` | `jobs.JobStatus` | Velox job reached terminal success | Never treated as input-assembly completion |
| `SUCCEEDED` | `deliveries.DeliveryStatus` | One delivery reached terminal success | Separate from job and publication state |
| `PUBLISHED` | `publicationstate.PublicationStatus` | Remote publication passed its verification phase | Separate from delivery/job success |
| `PUBLISHED` | `pipelineruns.Status` | Aggregated client-facing projection of a successful delivery | Derived projection, not an internal transition |
| `COMPLETED` | `pipelineruns.Status` | Aggregated projection when all deliveries succeed and are not scheduled | Derived projection, not input assembly |
| `completed` | `remoteengine` wire status | Remote engine reports its own completed generation state | Validated only by the remote-engine contract |

## Compatibility boundaries retained

- `instaedit` accepts legacy delivery aliases `PUBLISHED` and `COMPLETED` only
  while projecting old delivery responses into the client-facing view.
- Delivery reconciliation maps provider responses such as `published` and
  `completed` to the internal delivery result only after provider reconciliation.
- Remote-engine `completed` is validated at the remote-engine adapter; `done`
  and `succeeded` are not remote-engine statuses.

## Ambiguity corrected

`enqueue.ShouldForwardPipelineResult` previously accepted `succeeded` and
`done` as if they meant that a producer-side input payload was complete. The
gate now accepts only `InputAssemblyCompleted` (plus an absent status for legacy
payloads).
A Velox job status of `SUCCEEDED` can therefore no longer be mistaken for a
completed input handoff.

## Status-domain boundaries: wire → runtime

The canonical payload still carries an established overloaded wire key named
`status`. That wire shape is preserved for compatibility. At the runtime
boundary, however, the value must be interpreted through the domain expected
by the caller; a raw string must never be cast or copied between domains merely
because the spelling looks familiar.

### 1. Input handoff — `InputAssemblyStatus`

This is the producer-side state of assembling and handing off a request:

| Runtime value | Wire spelling | Meaning |
|---|---|---|
| `InputAssemblyPending` | `PENDING` | Handoff is not complete |
| `InputAssemblyCompleted` | `completed` | Request envelope is completely assembled |
| `InputAssemblyCompletedWithWarnings` | `completed_with_warnings` | Handoff is complete with non-fatal warnings |
| `InputAssemblyFailed` | `failed` | Handoff assembly failed |

`completed` ends input assembly only. It does **not** mean that rendering,
artifact verification, delivery, or remote publication succeeded. Use
`contract.ParseInputAssemblyStatus` / `JobPayloadV2.InputAssemblyStatus()` at
this boundary. Do not map it to `jobs.StatusSucceeded` or
`publicationstate.Published`.

### 2. Job lifecycle — `jobs.JobStatus`

This is the lifecycle of the Velox job aggregate. Its terminal states are
`SUCCEEDED`, `FAILED`, and `CANCELLED`; intermediate states such as `RUNNING`,
`AWAITING_ARTIFACT`, `DELIVERING`, and `RETRY_WAIT` remain non-terminal.

Job success requires lifecycle evidence, not merely a completed handoff. The
registered runtime authorities are:

- `ActorArtifactFinalizer` for the verified-finalization paths
  `RUNNING → SUCCEEDED` and `AWAITING_ARTIFACT → SUCCEEDED`;
- `ActorDeliveryRunner` for `DELIVERING → SUCCEEDED` when delivery invariants,
  including the remote delivery identifier, hold.

These are distinct runtime transitions. A producer emitting wire
`status=completed` is not a job lifecycle writer.

### 3. Delivery attempt — `deliveries.DeliveryStatus`

This is the lifecycle of one delivery attempt, independent of the job and the
artifact it delivers. `DeliveryStatus=SUCCEEDED` means that the delivery
attempt obtained the provider-side success evidence required by its delivery
contract. It does not, by itself, mean that the parent job is `SUCCEEDED` or
that a publication is `PUBLISHED`.

Delivery terminal values are `SUCCEEDED`, `FAILED`, `BLOCKED_AUTH`, and
`CANCELLED`. Delivery reconciliation must complete before a provider response
is treated as final success; an accepted/submitted remote operation is not
implicitly a completed publication.

### 4. Remote publication — `publicationstate.PublicationStatus`

This is the durable state machine for one remote publication. `PUBLISHED` is a
publication result, not a synonym for job or delivery success. The normal
successful path reaches `VERIFYING` and then takes `VERIFYING → PUBLISHED`;
the state machine also explicitly supports `Partial → PUBLISHED` for a
previously partial publication. Retry and partial paths retain their
phase-specific checkpoint.

The publication package currently protects this boundary through
`publicationstate.ValidateTransition` and durable snapshots rather than the
actor registry used for job and delivery transitions. Documentation must not
claim a publication actor allowlist that the runtime does not expose.

## Boundary rules

1. **Parse by domain.** At a wire boundary, use the parser for the intended
   domain (`ParseInputAssembly`, `ParseJob`, `ParseDelivery`, or
   `ParsePublication`). Invalid cross-domain values are rejected.
2. **Keep handoff and lifecycle separate.** `completed` is an input-assembly
   fact; `SUCCEEDED` and `PUBLISHED` require their own lifecycle evidence.
3. **Do not infer terminal state from submission.** A provider accepting a
   request or returning a remote operation ID is not proof of publication.
   Reconciliation is the authority for asynchronous remote completion.
4. **Preserve wire compatibility, not semantic ambiguity.** Existing JSON
   keys and spellings remain stable while typed runtime adapters make the
   intended domain explicit.
5. **Treat terminal writers as explicit.** New writers should validate and
   register their intended lifecycle transition and invariants where the
   applicable catalogue exists; they must not assign a terminal string
   directly in a generic payload mapper. The registry is a documented
   authority catalogue, not a claim that every writer is automatically blocked
   by one global runtime gate.

Authoritative implementation references:

- `shared/contract/input_assembly_status.go`
- `DataServer/internal/statusboundary/statusboundary.go`
- `DataServer/internal/jobs/status.go`
- `DataServer/internal/deliveries/status.go`
- `DataServer/internal/publicationstate/state.go`
- `DataServer/internal/statemachine/registry.go`
- `DataServer/internal/statemachine/registry_test.go`
- `DataServer/internal/publicationstate/state_test.go`

This document describes the contract and boundary policy; it does not change
any persisted or JSON wire value.
