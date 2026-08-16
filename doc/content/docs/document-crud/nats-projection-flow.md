---
title: NATS Projection Flow
description: Worker processing, redelivery, dedupe, and crash recovery for document projections.
---

# NATS Projection Flow

NATS JetStream distributes document commands, ingestion work, projection work,
and UI status events. Postgres records the document read model and projection
outcomes, but NATS owns workflow delivery and redelivery.

The normal worker rule is:

```text
consume NATS event
-> read JetStream delivery metadata
-> if delivery count is low, perform idempotent side effect optimistically
   before a gate pre-check
-> if delivery count is elevated, check the NATS KV gate before heavy work
-> if KV already advanced past this stage, republish the deterministic next
   wakeup and ACK
-> if delivery count exceeds the stage retry budget, CAS the gate to a
   terminal/compensating state before writing terminal PG state
-> otherwise perform or repeat the idempotent side effect
-> update PG projection/status state with CAS/version guards
-> advance NATS KV workflow gate with CAS; if it already advanced, republish
   the deterministic next wakeup; if it is blocked, do not resurrect
-> publish any next NATS wakeup with deterministic Nats-Msg-Id
-> ACK NATS message
```

NATS is at-least-once, not exactly-once. Duplicate or overlapping deliveries
must be safe.

## Streams And Subjects

Use separate streams for commands and work. Live UI progress uses NATS KV.
Add a shared progress stream only if durable progress history is required:

```text
DOC_COMMANDS
  subjects:
    documents.commands.v1.>

DOC_SNAPSHOTS
  subjects:
    documents.snapshots.v1.>

DOC_EVENTS
  subjects:
    documents.work.v1.>
    documents.projections.v1.>

DOC_PROGRESS optional
  subjects:
    documents.progress.v1.>
```

Example consumers:

```text
doc-pg-projector       documents.snapshots.v1.>
doc-ingest-orchestrator documents.commands.v1.upload.completed
doc-ingest-validate    documents.work.v1.ingest.validate
doc-ingest-extract     documents.work.v1.ingest.extract
doc-ingest-chunk       documents.work.v1.ingest.chunk
doc-vector-qdrant      documents.work.v1.vector.qdrant.>
doc-graph-nebula       documents.work.v1.graph.nebula.>
doc-realtime-ui        watches NATS KV document_jobs/jobs.>
```

Configure consumers with explicit ACK, bounded `AckWait`, duplicate detection,
and a `MaxDeliver` that is higher than the stage retry budget. NATS owns
transport delivery and retry counts. NATS KV owns short-lived workflow stage
checkpoints. PG rows record current document/projection results and repair
state, not per-message attempts.

## Worker Algorithm

Every worker follows the same high-level algorithm.

```text
1. Receive NATS message and read JetStream metadata.
2. Decode envelope and validate event_version/event_type.
3. Define the NATS KV workflow gate for this tenant/document/job.
   The authoritative field is `stage`: the next uncompleted stage. If
   `stage = extract`, extract has not passed yet. A successful worker
   advances it from `extract` to `chunk` with CAS.
4. If delivery count is low, the worker may skip the pre-read optimization for
   cheap work and run idempotent work optimistically. It still must read/CAS
   the KV gate before publishing success.
5. If delivery count is elevated and the work is heavy, check the gate before
   running work. If KV has advanced past this event's stage, republish the
   deterministic next wakeup from KV/event data and ACK. Do not silently ACK,
   because the earlier delivery may have crashed after advancing state but
   before publishing the next event.
6. If JetStream delivery count exceeds the stage retry budget, enter abort
   mode. CAS the KV gate from this stage to `failed` or `compensating`.
   If the CAS fails because KV already advanced, the stage succeeded first:
   republish the deterministic next wakeup and ACK. If the CAS succeeds, late
   workers are blocked from passing the gate.
7. In abort mode after the gate is blocked, force the terminal PG
   document/projection state, publish a deterministic failure or compensation
   event, publish UI failure status when needed, and ACK.
8. For normal retryable work, rely on NATS redelivery instead of a PG attempt
   row.
9. Run or rerun the idempotent side effect. For cheap stages, run
   optimistically and let CAS/version-guarded writes decide whether anything
   changed.
10. Commit PG projection/status state with deterministic keys and version
   guards. Terminal PG states must reject late success writes.
11. Advance the NATS KV workflow gate with CAS from this stage to the next.
    If CAS succeeds, continue to publish the deterministic next wakeup. If CAS fails
    because the gate already advanced, republish the deterministic next wakeup
    and ACK. If CAS fails because the gate is blocked or terminal, do not
    publish success or resurrect PG state; ACK after observing the terminal
    gate.
12. Publish any next NATS wakeup with deterministic Nats-Msg-Id.
13. ACK the current NATS message.
```

The KV gate has one authoritative field for progression:

```json
{
  "stage": "extract"
}
```

`stage` is the gate.

There is no `document_projection_attempt` table. Delivery count from
JetStream is the single retry counter. NATS KV is the workflow checkpoint and
must be updated with compare-and-set operations. If the worker cannot complete
a retryable step, it `NAK`s with delay or leaves the message unacked for
`AckWait` redelivery. When the delivery count exceeds the stage retry budget,
the worker converts the exhausted work message into explicit failed state only
if KV still shows that stage as current.

The failure event uses a deterministic id so a crash after publishing failure
but before ACKing the original message remains idempotent:

```text
failure_event_id = hash(original_event_id + projection_name + stage + "failed")
Nats-Msg-Id = failure_event_id
causation_event_id = original_event_id
correlation_id = original.correlation_id
```

## Idempotent Side Effects

PG document projection:

- upsert by `tenant_id + document_id`;
- apply only if the incoming target document version is newer than the stored
  document version;
- same version is a no-op;
- older version is skipped.

PG/S3 ingestion:

- validate reads S3 `HEAD` and bounded object ranges;
- extract writes current `document_content` by deterministic document key;
- chunk writes `document_chunk` by deterministic chunk ordinal;
- stages use the event and NATS KV source object key/hash before writing;
- events for older object key/hash become skipped no-ops.

Qdrant projection:

- point id is deterministic:
  `tenant_id:document_id:chunk_ordinal:model`;
- payload includes `projection_version = target_document_version`;
- upsert only if stored projection version is missing or older.

NebulaGraph projection:

- vertex/edge ids are deterministic;
- nGQL writes use `UPDATE` or `UPSERT ... WHEN` against `projection_version`;
- repeated upsert for the same aggregate version converges.

## Commit And Next Wakeup

After side effects, the worker updates projection/status state, advances the
NATS KV workflow checkpoint, and publishes the next NATS wakeup.

Example validate success:

```sql
BEGIN;

INSERT INTO document_validation (...)
VALUES (...)
ON CONFLICT (tenant_id, document_id)
DO UPDATE SET
  source_object_key = EXCLUDED.source_object_key,
  content_sha256 = EXCLUDED.content_sha256,
  object_size_bytes = EXCLUDED.object_size_bytes,
  content_type = EXCLUDED.content_type,
  source_document_version = EXCLUDED.source_document_version,
  updated_at = now();

UPDATE document
SET ingestion_state = 'extracting',
    document_version = GREATEST(document_version, $target_document_version),
    updated_at = now()
WHERE tenant_id = $tenant_id
  AND document_id = $document_id
  AND object_key = $object_key
  AND content_sha256 = $content_sha256;

COMMIT;

-- after commit, CAS the NATS KV workflow checkpoint from validate to extract
-- after commit, publish documents.work.v1.ingest.extract
-- with Nats-Msg-Id = deterministic next event id
```

If the worker crashes after the PG commit but before the KV CAS, redelivery
repeats the idempotent PG write and advances KV. If the worker crashes after
the KV CAS but before publishing the next wakeup, redelivery sees KV already
advanced, republishes the deterministic next wakeup, and ACKs. NATS duplicate
detection handles duplicate publishes.

## Retryable And Terminal Failure

On retryable failure, the worker does not write an attempt record. It either
`NAK`s with delay or leaves the message unacked so JetStream redelivers it
according to the consumer configuration.

When JetStream delivery count exceeds the stage retry budget, the worker
treats the message as terminally failed:

```text
receive terminal delivery
-> read NATS KV workflow gate
-> if KV advanced past this stage, republish deterministic next wakeup and ACK
-> otherwise skip normal side effect
-> CAS NATS KV gate from this stage to failed/compensating
-> if CAS fails because KV advanced, republish deterministic next wakeup and ACK
-> if CAS succeeds, force current PG document/projection state to failed or
   tombstoned according to workflow policy
-> publish document.ingest.failed, document.projection.failed, or next
   compensation event with deterministic Nats-Msg-Id
-> update the NATS KV progress snapshot to failed when user-visible status changes
-> ACK original message
```

For now there is no compensation path. A later repair or reindex command may
publish new work.

## Crash Matrix

| Worker crash point | Durable state | Redelivery behavior |
| --- | --- | --- |
| Before decode | NATS message unacked only. | Redelivery runs normally. |
| After decode before KV read | NATS message unacked only. | Redelivery runs normally. |
| KV already advanced before next publish | KV shows a later stage; next event may be missing. | Redelivery republishes deterministic next wakeup and ACKs. |
| On terminal retry delivery before gate block | NATS message unacked only. | Redelivery reaches terminal branch and re-checks KV. |
| After KV failed/compensating CAS before terminal PG write | KV blocks late success; PG may not yet show terminal state. | Redelivery observes blocked gate, force-writes terminal PG state, republishes failure/compensation event, and ACKs. |
| After terminal PG write before failure publish | KV is blocked and PG is terminal; failure event may be missing. | Redelivery republishes deterministic failure/compensation event and ACKs. |
| After failure publish before ACK | Failure event is durable; original message unacked. | Redelivery republishes with same `Nats-Msg-Id`; dedupe/no-op applies. |
| During S3/PG/Qdrant/NebulaGraph side effect | Partial deterministic output may exist. | Redelivery repeats idempotent upsert/delete. |
| After side effect before PG status update | Side effect may exist, PG status not applied. | Redelivery repeats side effect and records current state. |
| During PG commit | PG transaction committed or rolled back. | Redelivery repeats the deterministic write; CAS/version guards converge. |
| After PG commit before KV advance | Projection state is durable; KV still on this stage. | Redelivery repeats idempotent work and advances KV. |
| After KV advance before next wakeup publish | KV is advanced; next message may be missing. | Redelivery republishes deterministic next wakeup. |
| After next wakeup PubAck before ACK | Next message is durable. | Redelivery republishes with same `Nats-Msg-Id`, then ACKs. |
| Duplicate delivery overlaps original work | Same event may run twice. | Deterministic keys and CAS guards make one path a no-op. |

## Direct Publish Rule

Worker-published messages are normal NATS work events. They must:

1. use deterministic `Nats-Msg-Id`;
2. wait for `PubAck`;
3. publish before ACKing the current event;
4. be reconstructable from the event and NATS KV workflow state.

## Document Job State

Upload, ingestion, and projection stages use one NATS KV document job as the
short-lived workflow gate and live progress snapshot. The key is scoped to the
tenant, document, and job id, and the value expires after the workflow reaches
a terminal state:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

NATS KV tracks the minimal document job state:

```json
{
  "schema_version": 1,
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid",
  "operation": "create",
  "stage": "awaiting_upload",
  "upload_id": "uuid",
  "object_key": "documents/<tenant_id>/<document_id>/original"
}
```

`upload_id` and `object_key` are present only for upload-backed operations
such as `create` and `replace_content`. Workers update the value with CAS. It
is not the document read model and is not a permanent event store.

Upload expiry uses a separate time-bucket index because KV cannot query inside
values by `expires_at`:

```text
bucket: document_job_timeouts
key: upload_timeouts.<epoch_minute>.<tenant_id>.<document_id>.<job_id>
```

The timeout index value is only a pointer:

```json
{
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid"
}
```

The timeout scheduler lists due bucket keys, publishes deterministic
`document.upload_timeout_requested` events, and deletes timeout index keys
after `PubAck`. The document job KV gate remains the source of truth.

## Realtime Service

The realtime service watches NATS KV job state keys and fans out authorized
snapshots to browser WebSockets. It does not consume one NATS subject or stream
per job. Job keys are scoped by tenant, document, and job id:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

```d2
shape: sequence_diagram

KV: NATS KV document job state
RT: realtime-service
API: api-service
PG: Postgres projection
Browser: Browser

Browser -> API: authorize document list/detail
Browser -> RT: open WebSocket with auth token
RT -> API: verify tenant/user/document access
KV -> RT: watch job state changes
RT -> KV: load live job snapshot on reconnect
RT -> PG: load completed status snapshot when KV expired
RT -> Browser: send status snapshot/event
Browser -> API: reload status on reconnect or resync
```

Realtime owns no committed state. It can drop and reconnect safely because
live jobs have KV snapshots and completed jobs have PG status.

If a durable progress timeline is required later, publish progress snapshots to
a shared JetStream stream with wildcard subjects:

```text
documents.progress.v1.tenant.<tenant_id>.document.<document_id>.job.<job_id>
```

Do not create one stream per job. Configure shared-stream retention with
`MaxAge`, size/count limits, or per-subject limits according to the product
history requirement.

## Operational Metrics

Track at least:

- NATS command publish latency and PubAck failures;
- NATS consumer pending, redelivered, duplicate, and parked counts;
- NATS stream retention pressure;
- NATS KV saga count, age, and expiration count;
- NATS KV progress watch lag and active watched key count;
- terminal delivery failures by projection/stage/status;
- projection terminal failures by tenant/document/projection;
- PG projection lag from event `occurred_at` to row update;
- Qdrant/NebulaGraph projection lag;
- S3 orphan upload sessions and cleanup actions;
- UI snapshot delivery lag from KV watch receive to WebSocket send.
