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
